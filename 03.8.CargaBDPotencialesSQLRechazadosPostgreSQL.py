import pandas as pd
import numpy as np
import re
import psycopg2
from psycopg2 import extras
import os
from datetime import datetime
import warnings
import unicodedata

# Suprimir warnings específicos
warnings.filterwarnings('ignore', category=UserWarning, message='.*dayfirst.*')

# ======================
# CONFIGURACIÓN GLOBAL - RECHAZADOS POSTGRESQL
# ======================

# Archivo por defecto para RECHAZADOS
ARCHIVO_TXT_DEFAULT = r"D:\FNB\Reportes\04 Reporte Clientes Potenciales\2025\12. Diciembre\BD01122025_Rechazado.txt"

# Conexión PostgreSQL
PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "bd_calidda_fnb",
    "user": "postgres",
    "password": "ibr2025"
}

# Mapeo de tipos de datos PostgreSQL - RECHAZADOS
COLUMN_TYPES_PG = {
    "fecha_eval": "TIMESTAMP",
    "tipo_docum": "VARCHAR(500)",
    "n_i_f_1": "VARCHAR(20)",
    "int_cial": "BIGINT",
    "nombre": "VARCHAR(500)",
    "mensaje": "VARCHAR(500)"
}

# Mapeo de nombres originales a nombres PostgreSQL - RECHAZADOS
MAPEO_NOMBRES_COLUMNAS = {
    "Fecha Eval": "fecha_eval",
    "Tipo Docum": "tipo_docum", 
    "N.I.F.1": "n_i_f_1",
    "Int.cial.": "int_cial",
    "Soc.cial.": "int_cial",
    "Nombre": "nombre",
    "Mensaje": "mensaje"
}

def crear_tabla_bd_potenciales_rechazados(cursor, table_name):
    """Crea la tabla bd_potenciales_rechazado si no existe"""
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        fecha_eval TIMESTAMP,
        tipo_docum VARCHAR(500),
        n_i_f_1 VARCHAR(20),
        int_cial BIGINT,
        nombre VARCHAR(500),
        mensaje VARCHAR(500)
    )
    """
    cursor.execute(create_table_sql)
    print(f"✅ Tabla {table_name} verificada/creada")

# ======================
# FUNCIONES DE LIMPIEZA DE ARCHIVOS TXT
# ======================

def detectar_codificacion(archivo):
    """Detecta la codificación del archivo"""
    codificaciones_comunes = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1', 'utf-16']
    
    for encoding in codificaciones_comunes:
        try:
            with open(archivo, 'r', encoding=encoding) as f:
                f.read()
            return encoding
        except:
            continue
    
    return 'latin-1'

def detectar_separador(linea_cabecera):
    """Detecta el separador utilizado en el archivo"""
    separadores = ['\t', ';', ',', '|']
    for sep in separadores:
        if sep in linea_cabecera and linea_cabecera.count(sep) >= 3:
            return sep
    return '\t'

def _normalizar_texto(s: str) -> str:
    """Normaliza texto: minúsculas, sin acentos, sin puntuación extra, colapsa espacios."""
    if s is None:
        return ""
    s = str(s).strip()
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[\.:;\\/\\-]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def detectar_fila_cabecera(lineas, max_busqueda=50):
    """Detecta dinámicamente la fila de cabecera"""
    nombres_esperados_norm = set(_normalizar_texto(k) for k in MAPEO_NOMBRES_COLUMNAS.keys())
    mejor = {'idx': None, 'sep': None, 'cols': None, 'score': -1, 'total_cols': 0}
    limite = min(len(lineas), max_busqueda)
    
    for i in range(limite):
        linea = lineas[i].strip()
        if not linea:
            continue
        sep = detectar_separador(linea)
        partes = [p.strip() for p in linea.split(sep)]
        if len(partes) < 3:
            continue
        
        norm_partes = [_normalizar_texto(p) for p in partes]
        coincidencias = sum(1 for np in norm_partes if np in nombres_esperados_norm)
        
        if coincidencias > mejor['score']:
            mejor = {'idx': i, 'sep': sep, 'cols': partes, 'score': coincidencias, 'total_cols': len(partes)}
    
    if mejor['idx'] is not None:
        return mejor['idx'], mejor['sep'], mejor['cols']
    return None, None, None

def analizar_archivo_txt(archivo_entrada):
    """Analiza la estructura del archivo antes de la limpieza"""
    print("=== ANÁLISIS DEL ARCHIVO TXT RECHAZADOS ===")
    
    codificacion = detectar_codificacion(archivo_entrada)
    
    with open(archivo_entrada, 'r', encoding=codificacion) as file:
        lineas = file.readlines()
    
    print(f"Total de líneas: {len(lineas)}")
    idx_cab, separador, cabecera = detectar_fila_cabecera(lineas)
    if idx_cab is not None:
        print(f"Cabecera detectada en línea: {idx_cab + 1}")
        print(f"Separador detectado: {repr(separador)}")
        print(f"Número de columnas: {len(cabecera)}")
        print(f"Columnas: {', '.join(cabecera)}")
    else:
        print("⚠ No se detectó cabecera automáticamente")
    
    print("=" * 50)

def limpiar_archivo_txt(archivo_entrada):
    """Limpia un archivo TXT de rechazados y retorna DataFrame"""
    print("=== INICIANDO LIMPIEZA DE ARCHIVO TXT RECHAZADOS ===")
    
    codificacion = detectar_codificacion(archivo_entrada)
    
    with open(archivo_entrada, 'r', encoding=codificacion) as file:
        lineas = file.readlines()
    
    print(f"Total de líneas leídas: {len(lineas)}")
    
    if len(lineas) < 2:
        raise ValueError("El archivo tiene menos de 2 líneas")
    
    idx_cab, separador, cabecera_raw = detectar_fila_cabecera(lineas)
    if idx_cab is None:
        print("⚠ No se detectó cabecera, usando línea 0")
        idx_cab = 0
        separador = detectar_separador(lineas[0])
        cabecera_raw = lineas[0].strip().split(separador)
    else:
        print(f"✓ Cabecera detectada en línea {idx_cab + 1}")
    
    # Limpiar cabecera
    cabecera = []
    for i, col in enumerate(cabecera_raw):
        col_limpia = col.strip()
        if not col_limpia:
            col_limpia = f"Columna_{i+1}"
        cabecera.append(col_limpia)
    
    num_columnas_esperadas = len(cabecera)
    print(f"Cabecera extraída: {num_columnas_esperadas} columnas")
    print(f"Columnas detectadas: {', '.join(cabecera)}")

    # Identificar y remover columnas en blanco
    idx_cabeceras_vacias = [i for i, col in enumerate(cabecera) if col.startswith('Columna_') or col.strip() == '']
    if idx_cabeceras_vacias:
        print(f"⚠ Se encontraron {len(idx_cabeceras_vacias)} columnas vacías")
    cabecera_filtrada = [col for i, col in enumerate(cabecera) if i not in idx_cabeceras_vacias]
    num_cols_esperadas_final = len(cabecera_filtrada)
    
    # Procesar datos
    datos_raw = lineas[idx_cab + 1:]
    datos_limpios = []
    filas_omitidas = 0
    
    for i, linea in enumerate(datos_raw, start=idx_cab + 2):
        linea = linea.strip()
        if not linea:
            continue
        
        partes = linea.split(separador)
        partes_filtradas = [partes[j] if j < len(partes) else "" for j in range(len(cabecera)) if j not in idx_cabeceras_vacias]
        
        if len(partes_filtradas) == num_cols_esperadas_final:
            datos_limpios.append(partes_filtradas)
        elif len(partes_filtradas) < num_cols_esperadas_final:
            while len(partes_filtradas) < num_cols_esperadas_final:
                partes_filtradas.append("")
            datos_limpios.append(partes_filtradas)
        else:
            partes_filtradas = partes_filtradas[:num_cols_esperadas_final]
            datos_limpios.append(partes_filtradas)
    
    if filas_omitidas > 0:
        print(f"⚠ Se omitieron {filas_omitidas} filas con problemas")
    print(f"Total de filas procesadas: {len(datos_limpios)}")
    
    if len(datos_limpios) == 0:
        raise ValueError("No se procesó ninguna fila de datos")
    
    # Crear DataFrame con cabecera filtrada
    df = pd.DataFrame(datos_limpios, columns=cabecera_filtrada)
    
    # Eliminar columnas vacías
    columnas_a_eliminar = [col for col in df.columns if 'Columna' in col]
    if columnas_a_eliminar:
        df = df.drop(columns=columnas_a_eliminar)
    
    # FILTRAR SOLO COLUMNAS MAPEADAS
    grupos_por_destino = {}
    for original, destino in MAPEO_NOMBRES_COLUMNAS.items():
        grupos_por_destino.setdefault(destino, []).append(original)

    seleccionadas = []
    faltantes = []
    for destino, originales in grupos_por_destino.items():
        encontrada = next((o for o in originales if o in df.columns), None)
        if encontrada:
            seleccionadas.append(encontrada)
        else:
            faltantes.append(destino)

    if seleccionadas:
        df = df[seleccionadas]
        print(f"✓ Columnas seleccionadas (mapeadas): {len(seleccionadas)} columnas")
    else:
        raise ValueError("No se encontró ninguna columna mapeada")

    if faltantes:
        print(f"⚠ Columnas faltantes: {', '.join(faltantes)}")
    
    # Limpieza general
    df = aplicar_limpieza_general(df)
    
    print(f"✅ DataFrame creado: {len(df)} filas x {len(df.columns)} columnas")
    
    return df

def aplicar_limpieza_general(df):
    """Aplica limpieza general a los datos del DataFrame"""
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].str.strip()
            df[col] = df[col].replace(['', 'nan', 'NaN', 'None', 'null'], None)
    
    df = convertir_tipos_datos_basicos(df)
    
    return df

def convertir_tipos_datos_basicos(df):
    """Convierte tipos de datos básicos"""
    for col in df.columns:
        if df[col].dtype == 'object':
            sample = df[col].dropna().head(10).astype(str)
            
            if any(c in s for s in sample for c in ['/', '-', ':']):
                df[col] = _parse_datetime_series(df[col])
            elif sample.str.match(r'^\d+$').any():
                df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

def _parse_datetime_series(series: pd.Series) -> pd.Series:
    """Parsea fechas intentando formatos comunes"""
    s = series.astype(str).str.strip()
    sample = s.dropna().head(50)
    fmt = None
    patterns = [
        (r'^\d{1,2}/\d{1,2}/\d{4}$', '%d/%m/%Y'),
        (r'^\d{1,2}-\d{1,2}-\d{4}$', '%d-%m-%Y'),
        (r'^\d{1,2}/\d{1,2}/\d{2}$', '%d/%m/%y'),
        (r'^\d{1,2}-\d{1,2}-\d{2}$', '%d-%m-%y'),
    ]
    for pat, f in patterns:
        if sample.str.match(pat).any():
            fmt = f
            break
    if fmt:
        return pd.to_datetime(s, format=fmt, errors='coerce')
    
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return pd.to_datetime(s, errors='coerce')

# ======================
# FUNCIONES OPTIMIZADAS PARA POSTGRESQL
# ======================

def generar_nombre_tabla(archivo_path):
    """Genera nombre de tabla basado en el archivo - RECHAZADOS"""
    nombre_archivo = os.path.basename(archivo_path)
    if "BD" in nombre_archivo and any(c.isdigit() for c in nombre_archivo):
        match = re.search(r'BD(\d{8})', nombre_archivo)
        if match:
            fecha_str = match.group(1)
            try:
                fecha = datetime.strptime(fecha_str, "%d%m%Y")
                return f"bd_potenciales_rechazado_{fecha.strftime('%Y%m%d')}"
            except:
                pass
    
    timestamp = datetime.now().strftime("%Y%m%d")
    return f"bd_potenciales_rechazado_{timestamp}"

def limpiar_nombres_columnas_postgresql(df):
    """Limpia nombres de columnas para PostgreSQL"""
    print("\n=== Limpiando nombres de columnas para PostgreSQL ===")
    
    nuevos_nombres = []
    cambios = []
    
    for col in df.columns:
        nombre_original = col
        nuevo_nombre = MAPEO_NOMBRES_COLUMNAS.get(col, col)
        nuevo_nombre = nuevo_nombre.lower()
        
        if nuevo_nombre != nombre_original:
            cambios.append(f"  {nombre_original} → {nuevo_nombre}")
        
        nuevos_nombres.append(nuevo_nombre)
    
    if cambios:
        print(f"Se renombraron {len(cambios)} columnas:")
        for cambio in cambios:
            print(cambio)
    else:
        print("No se requirieron cambios en nombres de columnas")
    
    df.columns = nuevos_nombres
    return df

def convertir_tipos_datos_postgresql(df):
    """Convierte tipos de datos según mapeo PostgreSQL"""
    print("\n=== Conversión de tipos para PostgreSQL ===")
    
    for col in df.columns:
        if col in COLUMN_TYPES_PG:
            tipo_esperado = COLUMN_TYPES_PG[col]
            
            if "TIMESTAMP" in tipo_esperado:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            elif tipo_esperado in ("BIGINT", "INT"):
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            elif "NUMERIC" in tipo_esperado:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif "VARCHAR" in tipo_esperado:
                df[col] = df[col].astype(str)
                df[col] = df[col].replace(['nan', 'None', 'NaN'], None)
    
    return df

def limpiar_datos_postgresql(df):
    """Limpia datos para PostgreSQL"""
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].replace(['', 'nan', 'NaN', 'None'], None)
    
    return df

def verificar_tabla_existente(table_name):
    """Verifica si la tabla existe y consulta acción"""
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_name = %s
            )
        """, (table_name,))
        
        existe = cursor.fetchone()[0]
        
        if existe:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            num_registros = cursor.fetchone()[0]
            
            print(f"\n⚠ La tabla '{table_name}' ya existe con {num_registros:,} registros")
            respuesta = input("¿Desea (1) Reemplazar, (2) Agregar datos, o (3) Cancelar? [1/2/3]: ").strip()
            
            cursor.close()
            conn.close()
            
            if respuesta == "1":
                return "replace"
            elif respuesta == "2":
                return "append"
            else:
                return "cancel"
        else:
            cursor.close()
            conn.close()
            return "new"
            
    except Exception as e:
        print(f"Error al verificar tabla: {e}")
        return "error"

def preparar_dataframe_para_postgresql(df):
    """Prepara el DataFrame optimizado para PostgreSQL"""
    df_prep = df.copy()
    
    for col in df_prep.columns:
        if df_prep[col].dtype == 'object':
            df_prep[col] = df_prep[col].replace({pd.NaT: None, np.nan: None, 'nan': None, '': None})
    
    return df_prep

def cargar_dataframe_a_postgresql_optimizado(df, table_name):
    """Carga optimizada a PostgreSQL usando execute_values"""
    print(f"\n🚀 Carga OPTIMIZADA a PostgreSQL...")
    print(f"   Tabla: {table_name}")
    print(f"   Registros: {len(df):,}")
    
    try:
        df_prep = preparar_dataframe_para_postgresql(df)
        
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        
        # Crear tabla si no existe
        crear_tabla_bd_potenciales_rechazados(cursor, table_name)
        conn.commit()
        
        accion = verificar_tabla_existente(table_name)
        
        if accion == "cancel":
            print("❌ Operación cancelada por el usuario")
            cursor.close()
            conn.close()
            return False
        elif accion == "replace":
            print(f"🗑 Truncando tabla {table_name}...")
            cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
            conn.commit()
        
        columnas = df_prep.columns.tolist()
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columnas)}) VALUES %s"
        
        batch_size = 5000
        total = len(df_prep)
        
        for i in range(0, total, batch_size):
            batch_df = df_prep.iloc[i:i+batch_size].copy()
            batch = [tuple(row) for row in batch_df.values]
            
            extras.execute_values(cursor, insert_sql, batch, page_size=batch_size)
            conn.commit()
            
            porcentaje = ((i + len(batch)) / total) * 100
            print(f"  Progreso: {i + len(batch):,}/{total:,} ({porcentaje:.1f}%)")
        
        cursor.close()
        conn.close()
        
        print(f"✅ Carga completada: {total:,} registros")
        return True
        
    except Exception as e:
        print(f"❌ Error durante la carga: {e}")
        return False

def mostrar_resumen_proceso(df, archivo_original, csv_generado=None):
    """Muestra resumen completo del proceso"""
    if df is None:
        return
    
    print("\n" + "=" * 60)
    print("        RESUMEN DEL PROCESO - RECHAZADOS")
    print("=" * 60)
    print(f"📁 Archivo original: {os.path.basename(archivo_original)}")
    if csv_generado:
        print(f"📄 CSV generado: {os.path.basename(csv_generado)}")
    print(f"📊 Registros procesados: {len(df):,}")
    print(f"📋 Columnas: {len(df.columns)}")
    print(f"   {', '.join(df.columns.tolist())}")

# ======================
# FUNCIÓN PRINCIPAL
# ======================

def procesar_archivo_completo(archivo_txt, generar_csv=True, cargar_postgresql=True, csv_path=None):
    """Procesa el archivo completo: limpieza, CSV opcional, y carga a PostgreSQL"""
    
    inicio_total = datetime.now()
    
    try:
        print("\n" + "=" * 60)
        print("PASO 1: ANÁLISIS DEL ARCHIVO")
        print("=" * 60)
        analizar_archivo_txt(archivo_txt)
        
        print("\n" + "=" * 60)
        print("PASO 2: LIMPIEZA Y PROCESAMIENTO")
        print("=" * 60)
        df = limpiar_archivo_txt(archivo_txt)
        
        if df is None or df.empty:
            print("❌ No se pudo crear el DataFrame")
            return None, None
        
        df = limpiar_nombres_columnas_postgresql(df)
        df = convertir_tipos_datos_postgresql(df)
        df = limpiar_datos_postgresql(df)
        
        csv_generado = None
        if generar_csv:
            print("\n" + "=" * 60)
            print("PASO 3: GENERACIÓN DE CSV")
            print("=" * 60)
            
            if csv_path is None:
                base_name = os.path.splitext(archivo_txt)[0]
                csv_generado = f"{base_name}_limpio.csv"
            else:
                csv_generado = csv_path
            
            df.to_csv(csv_generado, index=False, encoding='utf-8-sig')
            print(f"✅ CSV guardado: {csv_generado}")
        
        if cargar_postgresql:
            print("\n" + "=" * 60)
            print("PASO 4: CARGA A POSTGRESQL")
            print("=" * 60)
            
            table_name = generar_nombre_tabla(archivo_txt)
            print(f"📋 Tabla destino: {table_name}")
            
            exito = cargar_dataframe_a_postgresql_optimizado(df, table_name)
            
            if not exito:
                print("⚠ La carga a PostgreSQL fue cancelada o falló")
        
        mostrar_resumen_proceso(df, archivo_txt, csv_generado)
        
        fin_total = datetime.now()
        print(f"\n⏱ Tiempo total: {fin_total - inicio_total}")
        
        return df, csv_generado
        
    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO: {e}")
        import traceback
        traceback.print_exc()
        return None, None

def main():
    """Función principal interactiva"""
    print("=" * 60)
    print("  PROCESADOR DE ARCHIVOS TXT PARA POSTGRESQL")
    print("  BD Clientes Potenciales - RECHAZADOS")
    print("=" * 60)
    
    archivo_txt = input(f"\nRuta del archivo TXT [{ARCHIVO_TXT_DEFAULT}]: ").strip()
    if not archivo_txt:
        archivo_txt = ARCHIVO_TXT_DEFAULT
    
    if not os.path.exists(archivo_txt):
        print(f"❌ El archivo no existe: {archivo_txt}")
        return
    
    generar_csv = input("¿Generar CSV limpio? (s/n) [s]: ").strip().lower() != 'n'
    cargar_postgresql = input("¿Cargar a PostgreSQL? (s/n) [s]: ").strip().lower() != 'n'
    
    procesar_archivo_completo(archivo_txt, generar_csv, cargar_postgresql)

# ======================
# PUNTO DE ENTRADA
# ======================

if __name__ == "__main__":
    main()

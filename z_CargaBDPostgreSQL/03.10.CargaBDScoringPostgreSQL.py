import pandas as pd
import numpy as np
import re
import psycopg2
from psycopg2 import extras
import os
from datetime import datetime
import warnings

# Suprimir warnings específicos
warnings.filterwarnings('ignore', category=UserWarning, message='.*dayfirst.*')

# ======================
# CONFIGURACIÓN GLOBAL - SCORING RIESGOS POSTGRESQL
# ======================

# Archivo por defecto para SCORING RIESGOS
ARCHIVO_TXT_DEFAULT = r"D:\FNB\Reportes\26. Scoring Riesgos\JV_SCORING_RIESGOS_HISTORICO_01122025.txt"

# Conexión PostgreSQL
PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "bd_calidda_fnb",
    "user": "postgres",
    "password": "ibr2025"
}

# Mapeo de tipos de datos PostgreSQL - SCORING RIESGOS
COLUMN_TYPES_PG = {
    "periodo": "TIMESTAMP",
    "cta_contr": "BIGINT",
    "dni": "VARCHAR(20)",
    "segmento_riesgo": "VARCHAR(500)",
    "interlocutor": "BIGINT",
    "flag_segmento_corregido": "VARCHAR(500)"
}

# Mapeo de nombres originales a nombres PostgreSQL - SCORING RIESGOS
MAPEO_NOMBRES_COLUMNAS = {
    "PERIODO": "periodo",
    "CTA_CONTR": "cta_contr",
    "DNI": "dni",
    "SEGMENTO_RIESGO": "segmento_riesgo",
    "INTERLOCUTOR": "interlocutor",
    "FLAG_SEGMENTO_CORREGIDO": "flag_segmento_corregido"
}

def crear_tabla_bd_scoring(cursor, table_name):
    """Crea la tabla bd_scoring_historico si no existe"""
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        periodo TIMESTAMP,
        cta_contr BIGINT,
        dni VARCHAR(20),
        segmento_riesgo VARCHAR(500),
        interlocutor BIGINT,
        flag_segmento_corregido VARCHAR(500)
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

def convertir_periodo_a_fecha(periodo_str):
    """
    Convierte PERIODO en formato YYYYMM a fecha TIMESTAMP
    Ejemplo: '202510' -> datetime(2025, 10, 1)
    """
    try:
        if pd.isna(periodo_str) or periodo_str is None:
            return None
        
        # Convertir a string y limpiar
        periodo = str(periodo_str).strip()
        
        # Si ya es una fecha, retornarla
        if '/' in periodo or '-' in periodo:
            return pd.to_datetime(periodo, errors='coerce')
        
        # Si es formato YYYYMM
        if len(periodo) == 6 and periodo.isdigit():
            anio = int(periodo[:4])
            mes = int(periodo[4:6])
            return datetime(anio, mes, 1)
        
        return None
    except:
        return None

def analizar_archivo_txt(archivo_entrada):
    """Analiza la estructura del archivo antes de la limpieza"""
    print("=== ANÁLISIS DEL ARCHIVO TXT SCORING RIESGOS ===")
    
    codificacion = detectar_codificacion(archivo_entrada)
    
    with open(archivo_entrada, 'r', encoding=codificacion) as file:
        lineas = file.readlines()
    
    print(f"Total de líneas: {len(lineas)}")
    
    if len(lineas) > 0:
        separador = detectar_separador(lineas[0])
        cabecera = lineas[0].strip().split(separador)
        print(f"Cabecera detectada: {len(cabecera)} columnas")
        print(f"Separador detectado: {repr(separador)}")
        print(f"Columnas: {', '.join(cabecera)}")
        
        if len(lineas) > 1:
            print(f"\nMuestra de datos (primera fila):")
            datos_muestra = lineas[1].strip().split(separador)
            for i, (col, val) in enumerate(zip(cabecera, datos_muestra)):
                print(f"  {col}: {val}")
    
    print("=" * 50)

def limpiar_archivo_txt(archivo_entrada):
    """Limpia un archivo TXT de SCORING RIESGOS y retorna DataFrame"""
    print("=== INICIANDO LIMPIEZA DE ARCHIVO TXT SCORING RIESGOS ===")
    
    codificacion = detectar_codificacion(archivo_entrada)
    
    with open(archivo_entrada, 'r', encoding=codificacion) as file:
        lineas = file.readlines()
    
    print(f"Total de líneas leídas: {len(lineas)}")
    
    if len(lineas) <= 1:
        raise ValueError("El archivo tiene 1 o menos líneas")
    
    # Primera línea es la cabecera
    separador = detectar_separador(lineas[0])
    cabecera_raw = lineas[0].strip().split(separador)
    
    # Limpiar cabecera
    cabecera = [col.strip() for col in cabecera_raw]
    
    num_columnas_esperadas = len(cabecera)
    print(f"Cabecera extraída: {num_columnas_esperadas} columnas")
    print(f"Columnas detectadas: {', '.join(cabecera)}")
    
    # Procesar datos desde la línea 2 (índice 1)
    datos_raw = lineas[1:]
    datos_limpios = []
    filas_omitidas = 0
    
    for i, linea in enumerate(datos_raw, start=2):
        linea = linea.strip()
        if not linea:
            continue
        
        partes = linea.split(separador)
        
        if len(partes) == num_columnas_esperadas:
            datos_limpios.append(partes)
        else:
            filas_omitidas += 1
    
    if filas_omitidas > 0:
        print(f"⚠ Se omitieron {filas_omitidas} filas con problemas de columnas")
    print(f"Total de filas procesadas: {len(datos_limpios)}")
    
    if len(datos_limpios) == 0:
        raise ValueError("No se procesó ninguna fila de datos")
    
    # Crear DataFrame
    df = pd.DataFrame(datos_limpios, columns=cabecera)
    
    # FILTRAR SOLO COLUMNAS MAPEADAS
    columnas_originales_mapeadas = list(MAPEO_NOMBRES_COLUMNAS.keys())
    columnas_existentes = [col for col in df.columns if col in columnas_originales_mapeadas]
    
    if len(columnas_existentes) < len(columnas_originales_mapeadas):
        faltantes = set(columnas_originales_mapeadas) - set(columnas_existentes)
        print(f"⚠ Columnas faltantes: {', '.join(faltantes)}")
    
    if len(columnas_existentes) == 0:
        raise ValueError("No se encontraron columnas mapeadas en el archivo")
    
    # Mantener solo las columnas mapeadas
    df = df[columnas_existentes]
    print(f"✅ Columnas seleccionadas (mapeadas): {len(columnas_existentes)} columnas")
    
    # CONVERSIÓN ESPECIAL DE PERIODO (YYYYMM -> TIMESTAMP)
    if 'PERIODO' in df.columns:
        print("\n🔄 Convirtiendo columna PERIODO (YYYYMM) a TIMESTAMP...")
        
        # Mostrar muestra antes de conversión
        muestra_antes = df['PERIODO'].head(3).tolist()
        print(f"  Muestra antes: {muestra_antes}")
        
        df['PERIODO'] = df['PERIODO'].apply(convertir_periodo_a_fecha)
        
        # Mostrar muestra después de conversión
        muestra_despues = df['PERIODO'].head(3).tolist()
        print(f"  Muestra después: {muestra_despues}")
        
        # Verificar conversión exitosa
        valores_nulos = df['PERIODO'].isna().sum()
        if valores_nulos > 0:
            print(f"  ⚠ {valores_nulos} valores no pudieron ser convertidos (NULL)")
    
    # Limpieza general
    df = aplicar_limpieza_general(df)
    
    print(f"✅ DataFrame creado: {len(df)} filas x {len(df.columns)} columnas")
    
    return df

def aplicar_limpieza_general(df):
    """Aplica limpieza general a los datos del DataFrame"""
    for col in df.columns:
        if df[col].dtype == 'object' and col != 'PERIODO':
            df[col] = df[col].str.strip()
            df[col] = df[col].replace(['', 'nan', 'NaN', 'None', 'null'], None)
    
    df = convertir_tipos_datos_basicos(df)
    
    return df

def convertir_tipos_datos_basicos(df):
    """Convierte tipos de datos básicos"""
    for col in df.columns:
        if col == 'PERIODO':
            continue
        
        if df[col].dtype == 'object':
            sample = df[col].dropna().head(10).astype(str)
            
            if sample.str.match(r'^\d+$').any():
                df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

# ======================
# FUNCIONES OPTIMIZADAS PARA POSTGRESQL
# ======================

def generar_nombre_tabla(archivo_path):
    """Genera nombre de tabla basado en el archivo - SCORING RIESGOS"""
    nombre_archivo = os.path.basename(archivo_path)
    
    # Buscar patrón de fecha en el nombre del archivo (DDMMYYYY)
    fecha_match = re.search(r'(\d{8})', nombre_archivo)
    if fecha_match:
        fecha_str = fecha_match.group(1)
        try:
            fecha = datetime.strptime(fecha_str, "%d%m%Y")
            return f"bd_scoring_historico_{fecha.strftime('%Y%m%d')}"
        except:
            pass
    
    # Fallback: usar fecha actual
    timestamp = datetime.now().strftime("%Y%m%d")
    return f"bd_scoring_historico_{timestamp}"

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
    """Convierte tipos de datos según mapeo PostgreSQL - SCORING RIESGOS"""
    print("\n=== Conversión de tipos para PostgreSQL ===")
    
    for col in df.columns:
        if col in COLUMN_TYPES_PG:
            tipo_esperado = COLUMN_TYPES_PG[col]
            
            if "TIMESTAMP" in tipo_esperado:
                # Ya debería estar convertido en limpiar_archivo_txt
                if df[col].dtype != 'datetime64[ns]':
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
        crear_tabla_bd_scoring(cursor, table_name)
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
    print("        RESUMEN DEL PROCESO - SCORING RIESGOS")
    print("=" * 60)
    print(f"📁 Archivo original: {os.path.basename(archivo_original)}")
    if csv_generado:
        print(f"📄 CSV generado: {os.path.basename(csv_generado)}")
    print(f"📊 Registros procesados: {len(df):,}")
    print(f"📋 Columnas: {len(df.columns)}")
    print(f"   {', '.join(df.columns.tolist())}")
    
    # Mostrar rango de periodos si existe la columna
    if 'periodo' in df.columns:
        periodo_min = df['periodo'].min()
        periodo_max = df['periodo'].max()
        if pd.notnull(periodo_min) and pd.notnull(periodo_max):
            print(f"📅 Rango de periodos: {periodo_min.strftime('%Y-%m')} a {periodo_max.strftime('%Y-%m')}")

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
    print("  BD Scoring Riesgos Histórico")
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

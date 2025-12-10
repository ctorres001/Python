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
# CONFIGURACIÓN GLOBAL - SEGMENTACIÓN POSTGRESQL
# ======================

# Archivo por defecto para SEGMENTACIÓN
ARCHIVO_TXT_DEFAULT = r"D:\FNB\Reportes\25. Segmentación\SEGMENTACION_FNB_ACUMULADA_01112025.txt"

# Conexión PostgreSQL
PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "bd_calidda_fnb",
    "user": "postgres",
    "password": "ibr2025"
}

# Mapeo de tipos de datos PostgreSQL - SEGMENTACIÓN
COLUMN_TYPES_PG = {
    "interlocutor": "BIGINT",
    "flag_segmentacion": "VARCHAR(500)",
    "fecha_corte": "TIMESTAMP",
    "cta_contr": "BIGINT"
}

# Mapeo de nombres originales a nombres PostgreSQL - SEGMENTACIÓN
MAPEO_NOMBRES_COLUMNAS = {
    "INTERLOCUTOR": "interlocutor",
    "FLAG_SEGMENTACION": "flag_segmentacion",
    "FECHA_CORTE": "fecha_corte",
    "CTA_CONTR": "cta_contr"
}

def crear_tabla_bd_segmentacion(cursor, table_name):
    """Crea la tabla bd_segmentacion_historica si no existe"""
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        interlocutor BIGINT,
        flag_segmentacion VARCHAR(500),
        fecha_corte TIMESTAMP,
        cta_contr BIGINT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            with open(archivo, 'r', encoding=encoding) as file:
                sample = file.read(5000)
                if len(sample) > 0 and not any(ord(char) > 65535 for char in sample[:100]):
                    return encoding
        except (UnicodeDecodeError, UnicodeError):
            continue
    
    return 'latin-1'

def detectar_separador(linea_cabecera):
    """Detecta el separador utilizado en el archivo"""
    separadores = ['\t', ';', ',', '|']
    for sep in separadores:
        if linea_cabecera.count(sep) > 2:  # Al menos 3 columnas
            return sep
    return '\t'

def analizar_archivo_txt(archivo_entrada):
    """Analiza la estructura del archivo antes de la limpieza"""
    print("=== ANÁLISIS DEL ARCHIVO TXT SEGMENTACIÓN ===")
    
    codificacion = detectar_codificacion(archivo_entrada)
    
    with open(archivo_entrada, 'r', encoding=codificacion) as file:
        lineas = file.readlines()
    
    print(f"Total de líneas: {len(lineas)}")
    
    if len(lineas) > 0:
        # Asumir que la primera línea es la cabecera
        separador = detectar_separador(lineas[0])
        cabecera = lineas[0].strip().split(separador)
        print(f"Cabecera detectada: {len(cabecera)} columnas")
        print(f"Columnas: {', '.join(cabecera)}")
        
        # Verificar algunas filas de datos
        problemas = 0
        for i, linea in enumerate(lineas[1:min(100, len(lineas))], start=2):
            if linea.strip() == '':
                continue
            campos = linea.strip().split(separador)
            if len(campos) != len(cabecera):
                problemas += 1
        
        if problemas > 0:
            print(f"⚠️  Filas con estructura diferente (muestra): {problemas}")
        else:
            print("✅ Estructura consistente detectada")
    
    print("=" * 50)

def limpiar_archivo_txt(archivo_entrada):
    """Limpia un archivo TXT de SEGMENTACIÓN y retorna DataFrame"""
    print("=== INICIANDO LIMPIEZA DE ARCHIVO TXT SEGMENTACIÓN ===")
    
    codificacion = detectar_codificacion(archivo_entrada)
    
    with open(archivo_entrada, 'r', encoding=codificacion) as file:
        lineas = file.readlines()
    
    print(f"Total de líneas leídas: {len(lineas)}")
    
    if len(lineas) <= 1:
        print("❌ Error: El archivo no tiene suficientes líneas para procesar")
        return None
    
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
        if linea.strip() == '':
            continue
            
        campos = linea.strip().split(separador)
        
        if len(campos) == num_columnas_esperadas:
            datos_limpios.append(campos)
        else:
            filas_omitidas += 1
    
    if filas_omitidas > 0:
        print(f"⚠️  Filas omitidas por estructura incorrecta: {filas_omitidas}")
    print(f"Total de filas procesadas: {len(datos_limpios)}")
    
    if len(datos_limpios) == 0:
        print("❌ No se procesaron datos válidos")
        return None
    
    # Crear DataFrame
    df = pd.DataFrame(datos_limpios, columns=cabecera)
    
    # FILTRAR SOLO COLUMNAS MAPEADAS
    columnas_originales_mapeadas = list(MAPEO_NOMBRES_COLUMNAS.keys())
    columnas_existentes = [col for col in df.columns if col in columnas_originales_mapeadas]
    
    if len(columnas_existentes) < len(columnas_originales_mapeadas):
        columnas_faltantes = set(columnas_originales_mapeadas) - set(columnas_existentes)
        print(f"⚠️  Columnas esperadas pero no encontradas: {', '.join(columnas_faltantes)}")
    
    if len(columnas_existentes) == 0:
        print("❌ No se encontraron columnas mapeadas en el archivo")
        return None
    
    # Mantener solo las columnas mapeadas
    df = df[columnas_existentes]
    print(f"✅ Columnas seleccionadas (mapeadas): {len(columnas_existentes)} columnas")
    
    # Limpieza general
    df = aplicar_limpieza_general(df)
    
    print(f"✅ DataFrame creado: {len(df)} filas x {len(df.columns)} columnas")
    
    return df

def aplicar_limpieza_general(df):
    """Aplica limpieza general a los datos del DataFrame"""
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).str.replace('\n', ' ', regex=False)
            df[col] = df[col].str.replace('\r', ' ', regex=False)
            df[col] = df[col].str.replace('\t', ' ', regex=False)
            df[col] = df[col].str.strip()
            df[col] = df[col].replace(['nan', 'NaN', 'NULL', ''], np.nan)
    
    df = convertir_tipos_datos_basicos(df)
    
    return df

def convertir_tipos_datos_basicos(df):
    """Convierte tipos de datos básicos"""
    for col in df.columns:
        if df[col].dtype == 'object':
            # Verificar si parece fecha
            muestra = df[col].dropna().head(10)
            if any(re.match(r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}', str(val)) for val in muestra):
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
                    continue
                except:
                    pass
            
            # Verificar si parece numérico
            try:
                col_limpia = df[col].astype(str).str.replace(',', '').str.replace(' ', '')
                numeric_df = pd.to_numeric(col_limpia, errors='coerce')
                
                if numeric_df.notna().sum() / len(df) > 0.8:
                    df[col] = numeric_df
            except:
                pass
    
    return df

# ======================
# FUNCIONES OPTIMIZADAS PARA POSTGRESQL
# ======================

def generar_nombre_tabla(archivo_path):
    """Genera nombre de tabla basado en el archivo - SEGMENTACIÓN"""
    nombre_archivo = os.path.basename(archivo_path)
    
    # Buscar patrón de fecha en el nombre del archivo (DDMMYYYY)
    fecha_match = re.search(r'(\d{8})', nombre_archivo)
    if fecha_match:
        fecha = fecha_match.group(1)
        # Convertir de DDMMYYYY a YYYYMMDD
        dia = fecha[0:2]
        mes = fecha[2:4]
        anio = fecha[4:8]
        fecha_postgresql = f"{anio}{mes}{dia}"
        return f"bd_segmentacion_historica_{fecha_postgresql}"
    
    # Fallback: usar fecha actual
    timestamp = datetime.now().strftime("%Y%m%d")
    return f"bd_segmentacion_historica_{timestamp}"

def limpiar_nombres_columnas_postgresql(df):
    """Limpia nombres de columnas para PostgreSQL"""
    print("\n=== Limpiando nombres de columnas para PostgreSQL ===")
    
    nuevos_nombres = []
    cambios = []
    
    for col in df.columns:
        # PRIMERO: Verificar mapeo directo
        col_limpio = col.strip()
        
        if col_limpio in MAPEO_NOMBRES_COLUMNAS:
            nuevo_nombre = MAPEO_NOMBRES_COLUMNAS[col_limpio]
        else:
            # SEGUNDO: Limpieza automática si no está en el mapeo
            nuevo_nombre = col_limpio
            nuevo_nombre = re.sub(r'[^\w\s]', '_', nuevo_nombre)
            nuevo_nombre = re.sub(r'\s+', '_', nuevo_nombre)
            nuevo_nombre = re.sub(r'_+', '_', nuevo_nombre)
            nuevo_nombre = nuevo_nombre.strip('_').lower()
        
        if col != nuevo_nombre:
            cambios.append((col, nuevo_nombre))
        
        nuevos_nombres.append(nuevo_nombre)
    
    if cambios:
        print(f"Columnas renombradas: {len(cambios)}")
        for orig, nuevo in cambios:
            print(f"  {orig} → {nuevo}")
    else:
        print("✅ No se requirieron cambios en nombres de columnas")
    
    df.columns = nuevos_nombres
    return df

def convertir_tipos_datos_postgresql(df):
    """Convierte tipos de datos según mapeo PostgreSQL - SEGMENTACIÓN"""
    print("\n=== Conversión de tipos para PostgreSQL ===")
    
    for col in df.columns:
        # IGNORAR COLUMNAS NO MAPEADAS
        if col not in COLUMN_TYPES_PG:
            continue
            
        pg_type = COLUMN_TYPES_PG[col]
        
        try:
            if pg_type.startswith("VARCHAR"):
                df[col] = df[col].astype(str)
                # Aplicar trim (strip) para eliminar espacios
                df[col] = df[col].str.strip()
                # Limpiar valores nulos
                df[col] = df[col].replace(['nan', 'NaN', 'None', 'null'], pd.NA)
                
            elif pg_type in ("INT", "BIGINT"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
                df[col] = df[col].round(0).astype("Int64")
                
            elif "NUMERIC" in pg_type:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                df[col] = df[col].round(2)
                
            elif pg_type == "TIMESTAMP":
                df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
            
            print(f"✓ {col}: {pg_type}")
            
        except Exception as e:
            print(f"❌ Error convirtiendo {col}: {str(e)}")
    
    return df

def limpiar_datos_postgresql(df):
    """Limpia datos para PostgreSQL"""
    for col in df.select_dtypes(include=["object"]).columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str)
            df[col] = df[col].str.replace("¬", "-", regex=False)
            df[col] = df[col].str.replace("\n", " ", regex=False)
            df[col] = df[col].str.replace("\r", " ", regex=False)
            df[col] = df[col].str.replace("\t", " ", regex=False)
            df[col] = df[col].str.replace('"', '""', regex=False)
            df[col] = df[col].replace(['nan', 'NaN', 'None'], None)
    
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
        
        tabla_existe = cursor.fetchone()[0]
        
        if tabla_existe:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            registros_existentes = cursor.fetchone()[0]
            
            print(f"\n⚠️  TABLA YA EXISTE: {table_name}")
            print(f"📊 Registros actuales: {registros_existentes:,}")
            print("\n¿Qué deseas hacer?")
            print("1. Reemplazar tabla completa (DROP + CREATE)")
            print("2. Truncar tabla y cargar nuevos datos (TRUNCATE)")
            print("3. Agregar datos a la tabla existente (APPEND)")
            print("4. Cancelar operación")
            
            while True:
                try:
                    opcion = input("\nElige una opción (1-4): ").strip()
                    if opcion in ['1', '2', '3', '4']:
                        break
                    else:
                        print("Por favor, ingresa 1, 2, 3 o 4")
                except KeyboardInterrupt:
                    print("\nOperación cancelada")
                    cursor.close()
                    conn.close()
                    return 'cancel'
            
            cursor.close()
            conn.close()
            
            return ['replace', 'truncate', 'append', 'cancel'][int(opcion) - 1]
        
        cursor.close()
        conn.close()
        return 'create'
        
    except Exception as e:
        print(f"❌ Error verificando tabla: {str(e)}")
        return 'create'

def preparar_dataframe_para_postgresql(df):
    """Prepara el DataFrame optimizado para PostgreSQL"""
    df_prep = df.copy()
    
    for col in df_prep.columns:
        if df_prep[col].dtype == 'object':
            df_prep[col] = df_prep[col].astype(str)
            df_prep[col] = df_prep[col].replace(['<NA>', 'nan', 'NaN', 'None', 'NULL'], None)
            df_prep[col] = df_prep[col].str.replace('\x00', '', regex=False)
        elif pd.api.types.is_datetime64_any_dtype(df_prep[col]):
            df_prep[col] = df_prep[col].where(pd.notnull(df_prep[col]), None)
        elif pd.api.types.is_numeric_dtype(df_prep[col]):
            if df_prep[col].dtype == 'Int64':
                df_prep[col] = df_prep[col].where(pd.notnull(df_prep[col]), None)
            else:
                df_prep[col] = df_prep[col].replace([np.inf, -np.inf], None)
                df_prep[col] = df_prep[col].where(pd.notnull(df_prep[col]), None)
    
    return df_prep

def cargar_dataframe_a_postgresql_optimizado(df, table_name):
    """Carga optimizada a PostgreSQL usando execute_values"""
    print(f"\n🚀 Carga OPTIMIZADA a PostgreSQL...")
    print(f"   Tabla: {table_name}")
    print(f"   Registros: {len(df):,}")
    
    try:
        # 1. Preparar DataFrame
        df_prep = preparar_dataframe_para_postgresql(df)
        
        # 2. Verificar acción a tomar
        accion = verificar_tabla_existente(table_name)
        
        if accion == 'cancel':
            print("❌ Operación cancelada")
            return False
        
        # 3. Conectar a PostgreSQL
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        
        # Crear tabla si no existe
        crear_tabla_bd_segmentacion(cursor, table_name)
        conn.commit()
        
        # 4. MANEJAR TABLA SEGÚN ACCIÓN
        if accion == 'replace':
            print(f"🗑️  Reemplazando tabla {table_name}...")
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.commit()
        elif accion == 'truncate':
            print(f"🗑️  Truncando tabla {table_name}...")
            cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
            conn.commit()
        
        # 5. CARGAR EN CHUNKS OPTIMIZADO
        columnas = df_prep.columns.tolist()
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columnas)}) VALUES %s"
        
        chunksize = 5000
        total_chunks_loaded = 0
        
        print(f"\n📊 Cargando en chunks de {chunksize:,}...")
        
        for i in range(0, len(df_prep), chunksize):
            chunk_df = df_prep.iloc[i:i+chunksize].copy()
            batch = [tuple(row) for row in chunk_df.values]
            
            extras.execute_values(cursor, insert_sql, batch, page_size=chunksize)
            conn.commit()
            
            total_chunks_loaded += 1
            registros_cargados = min(total_chunks_loaded * chunksize, len(df_prep))
            print(f"  Chunk {total_chunks_loaded}: {registros_cargados:,} de {len(df_prep):,} registros")
        
        # 6. VERIFICAR CARGA
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count_final = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        print(f"\n✅ Carga OPTIMIZADA completada!")
        print(f"   📊 Registros en tabla: {count_final:,}")
        print(f"   🗂️  Tabla: {table_name}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error en carga optimizada: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def mostrar_resumen_proceso(df, archivo_original, csv_generado=None):
    """Muestra resumen completo del proceso"""
    if df is None:
        return
    
    print("\n" + "=" * 60)
    print("        RESUMEN DEL PROCESO - SEGMENTACIÓN")
    print("=" * 60)
    print(f"📁 Archivo original: {os.path.basename(archivo_original)}")
    if csv_generado:
        print(f"📄 CSV generado: {os.path.basename(csv_generado)}")
    print(f"📊 Registros procesados: {len(df):,}")
    print(f"📋 Columnas: {len(df.columns)}")
    print(f"   {', '.join(df.columns.tolist())}")
    print("=" * 60)

# ======================
# FUNCIÓN PRINCIPAL
# ======================

def procesar_archivo_completo(archivo_txt, generar_csv=True, cargar_postgresql=True, csv_path=None):
    """Proceso completo: TXT → Limpieza → CSV → PostgreSQL"""
    print("🚀 INICIANDO PROCESO ETL - SEGMENTACIÓN HISTÓRICA")
    print("=" * 60)
    print(f"📁 Archivo de entrada: {archivo_txt}")
    
    if not os.path.exists(archivo_txt):
        print(f"❌ Error: El archivo no existe: {archivo_txt}")
        return None
    
    try:
        # 1. ANÁLISIS DEL ARCHIVO
        analizar_archivo_txt(archivo_txt)
        
        # 2. LIMPIEZA DEL ARCHIVO TXT
        print("\n" + "=" * 30)
        print("FASE 1: LIMPIEZA DE ARCHIVO TXT")
        print("=" * 30)
        
        df_limpio = limpiar_archivo_txt(archivo_txt)
        
        if df_limpio is None:
            print("❌ Error: No se pudo limpiar el archivo")
            return None
        
        # 3. GENERAR CSV (opcional)
        csv_generado = None
        if generar_csv:
            print("\n" + "=" * 30)
            print("FASE 2: GENERACIÓN DE CSV")
            print("=" * 30)
            
            if csv_path is None:
                csv_path = archivo_txt.replace('.txt', '_limpio.csv')
            
            df_limpio.to_csv(csv_path, sep=',', index=False, encoding='utf-8-sig')
            csv_generado = csv_path
            print(f"✅ CSV generado: {csv_path}")
        
        # 4. CARGAR A POSTGRESQL
        if cargar_postgresql:
            print("\n" + "=" * 30)
            print("FASE 3: CARGA A POSTGRESQL")
            print("=" * 30)
            
            df_postgresql = df_limpio.copy()
            df_postgresql = limpiar_nombres_columnas_postgresql(df_postgresql)
            df_postgresql = convertir_tipos_datos_postgresql(df_postgresql)
            df_postgresql = limpiar_datos_postgresql(df_postgresql)
            
            table_name = generar_nombre_tabla(archivo_txt)
            
            exito_postgresql = cargar_dataframe_a_postgresql_optimizado(df_postgresql, table_name)
            
            if not exito_postgresql:
                print("⚠️  La carga a PostgreSQL falló")
        
        # 5. MOSTRAR RESUMEN
        mostrar_resumen_proceso(df_limpio, archivo_txt, csv_generado)
        
        print(f"\n🎉 PROCESO ETL SEGMENTACIÓN COMPLETADO!")
        
        return df_limpio
        
    except Exception as e:
        print(f"❌ Error durante el proceso ETL: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Función principal - Proceso automático SEGMENTACIÓN"""
    print("=" * 80)
    print("     SISTEMA ETL - SEGMENTACIÓN HISTÓRICA FNB - CALIDDA")
    print("=" * 80)
    
    # Usar directamente el archivo por defecto sin preguntar
    archivo = ARCHIVO_TXT_DEFAULT
    print(f"\n📁 Procesando archivo: {archivo}")
    
    resultado = procesar_archivo_completo(
        archivo_txt=archivo,
        generar_csv=True,
        cargar_postgresql=True
    )
    
    if resultado is not None:
        print("\n✅ Proceso SEGMENTACIÓN finalizado exitosamente")
    else:
        print("\n❌ El proceso falló")

# ======================
# PUNTO DE ENTRADA
# ======================

if __name__ == "__main__":
    print("🔍 Validando sistema...")
    try:
        import pandas
        import numpy
        import psycopg2
        print("✅ Todas las dependencias están instaladas\n")
    except ImportError as e:
        print(f"❌ Falta dependencia: {e}")
        print("Instala con: pip install pandas numpy psycopg2-binary")
        exit(1)
    
    main()

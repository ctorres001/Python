import pandas as pd
import numpy as np
import re
import chardet
import pyodbc
from sqlalchemy import create_engine, text
import os
from datetime import datetime
import warnings

# Suprimir warnings específicos
warnings.filterwarnings('ignore', category=UserWarning, message='.*dayfirst.*')

# ======================
# CONFIGURACIÓN GLOBAL - SCORING RIESGOS
# ======================

# Archivo por defecto para SCORING RIESGOS
ARCHIVO_TXT_DEFAULT = r"D:\FNB\Reportes\26. Scoring Riesgos\JV_SCORING_RIESGOS_HISTORICO_01112025.txt"

# Conexión SQL Server
SQL_CONFIG = {
    "server": "192.168.64.250",
    "database": "BD_CALIDDA_FNB",
    "username": "ctorres",
    "password": "ibr2025"
}

# Mapeo de tipos de datos SQL Server - SCORING RIESGOS
COLUMN_TYPES_SQL = {
    "PERIODO": "DATETIME",
    "CTA_CONTR": "BIGINT",
    "DNI": "VARCHAR(20)",
    "SEGMENTO_RIESGO": "VARCHAR(500)",
    "INTERLOCUTOR": "BIGINT",
    "FLAG_SEGMENTO_CORREGIDO": "VARCHAR(500)"
}

# Mapeo de nombres originales a nombres SQL-compatibles - SCORING RIESGOS
MAPEO_NOMBRES_COLUMNAS = {
    "PERIODO": "PERIODO",
    "CTA_CONTR": "CTA_CONTR",
    "DNI": "DNI",
    "SEGMENTO_RIESGO": "SEGMENTO_RIESGO",
    "INTERLOCUTOR": "INTERLOCUTOR",
    "FLAG_SEGMENTO_CORREGIDO": "FLAG_SEGMENTO_CORREGIDO"
}

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
        if linea_cabecera.count(sep) > 3:  # Al menos 4 columnas
            return sep
    return '\t'

def convertir_periodo_a_fecha(periodo_str):
    """
    Convierte PERIODO en formato YYYYMM a fecha DATETIME
    Ejemplo: '202510' -> datetime(2025, 10, 1)
    """
    try:
        if pd.isna(periodo_str) or periodo_str == '' or str(periodo_str) == 'nan':
            return None
        
        periodo_str = str(periodo_str).strip()
        
        # Si ya es fecha, retornar
        if '-' in periodo_str or '/' in periodo_str:
            return pd.to_datetime(periodo_str, errors='coerce')
        
        # Convertir YYYYMM a fecha (primer día del mes)
        if len(periodo_str) == 6 and periodo_str.isdigit():
            anio = int(periodo_str[:4])
            mes = int(periodo_str[4:6])
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
    """Limpia un archivo TXT de SCORING RIESGOS y retorna DataFrame"""
    print("=== INICIANDO LIMPIEZA DE ARCHIVO TXT SCORING RIESGOS ===")
    
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
    
    # CONVERSIÓN ESPECIAL DE PERIODO (YYYYMM -> DATETIME)
    if 'PERIODO' in df.columns:
        print("\n🔄 Convirtiendo columna PERIODO de YYYYMM a DATETIME...")
        periodo_original = df['PERIODO'].head(3).tolist()
        print(f"   Ejemplo antes: {periodo_original}")
        
        df['PERIODO'] = df['PERIODO'].apply(convertir_periodo_a_fecha)
        
        periodo_convertido = df['PERIODO'].head(3).tolist()
        print(f"   Ejemplo después: {periodo_convertido}")
        
        # Contar conversiones exitosas
        convertidos = df['PERIODO'].notna().sum()
        total = len(df)
        print(f"   ✅ Conversiones exitosas: {convertidos:,} de {total:,} ({convertidos/total*100:.1f}%)")
    
    # Limpieza general
    df = aplicar_limpieza_general(df)
    
    print(f"✅ DataFrame creado: {len(df)} filas x {len(df.columns)} columnas")
    
    return df

def aplicar_limpieza_general(df):
    """Aplica limpieza general a los datos del DataFrame"""
    for col in df.columns:
        # Saltar PERIODO ya que ya fue convertido
        if col == 'PERIODO':
            continue
            
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
        # Saltar PERIODO ya que ya fue convertido a datetime
        if col == 'PERIODO':
            continue
            
        if df[col].dtype == 'object':
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
# FUNCIONES OPTIMIZADAS PARA SQL SERVER
# ======================

def generar_nombre_tabla(archivo_path):
    """Genera nombre de tabla basado en el archivo - SCORING RIESGOS"""
    nombre_archivo = os.path.basename(archivo_path)
    
    # Buscar patrón de fecha en el nombre del archivo (DDMMYYYY)
    fecha_match = re.search(r'(\d{8})', nombre_archivo)
    if fecha_match:
        fecha = fecha_match.group(1)
        # Convertir de DDMMYYYY a YYYYMMDD
        dia = fecha[0:2]
        mes = fecha[2:4]
        anio = fecha[4:8]
        fecha_sql = f"{anio}{mes}{dia}"
        return f"BD_Scoring_Historico_{fecha_sql}"
    
    # Fallback: usar fecha actual
    timestamp = datetime.now().strftime("%Y%m%d")
    return f"BD_Scoring_Historico_{timestamp}"

def limpiar_nombres_columnas_sql(df):
    """Limpia nombres de columnas para SQL Server"""
    print("\n=== Limpiando nombres de columnas para SQL ===")
    
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
            nuevo_nombre = nuevo_nombre.strip('_')
        
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

def convertir_tipos_datos_sql(df):
    """Convierte tipos de datos según mapeo SQL - SCORING RIESGOS"""
    print("\n=== Conversión de tipos para SQL Server ===")
    
    for col in df.columns:
        # IGNORAR COLUMNAS NO MAPEADAS
        if col not in COLUMN_TYPES_SQL:
            continue
            
        sql_type = COLUMN_TYPES_SQL[col]
        
        try:
            if sql_type.startswith("VARCHAR"):
                df[col] = df[col].astype(str)
                # Aplicar trim (strip) para eliminar espacios
                df[col] = df[col].str.strip()
                # Limpiar valores nulos
                df[col] = df[col].replace(['nan', 'NaN', 'None', 'null'], pd.NA)
                
            elif sql_type in ("INT", "BIGINT"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
                df[col] = df[col].round(0).astype("Int64")
                
            elif "DECIMAL" in sql_type:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                df[col] = df[col].round(2)
                
            elif sql_type == "DATETIME":
                # PERIODO ya fue convertido en limpiar_archivo_txt
                if col == 'PERIODO':
                    # Ya está como datetime, solo verificar
                    if not pd.api.types.is_datetime64_any_dtype(df[col]):
                        df[col] = pd.to_datetime(df[col], errors="coerce")
                else:
                    df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
            
            print(f"✓ {col}: {sql_type}")
            
        except Exception as e:
            print(f"❌ Error convirtiendo {col}: {str(e)}")
    
    return df

def limpiar_datos_sql(df):
    """Limpia datos para SQL Server"""
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
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_CONFIG['server']};DATABASE={SQL_CONFIG['database']};UID={SQL_CONFIG['username']};PWD={SQL_CONFIG['password']};TrustServerCertificate=yes;"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = ? AND TABLE_TYPE = 'BASE TABLE'
        """, table_name)
        
        tabla_existe = cursor.fetchone()[0] > 0
        
        if tabla_existe:
            cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
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

def preparar_dataframe_para_sql(df):
    """Prepara el DataFrame optimizado para SQL Server"""
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

def cargar_dataframe_a_sql_optimizado(df, table_name):
    """Carga optimizada basada en CSV temporal"""
    print(f"\n🚀 Carga OPTIMIZADA a SQL Server...")
    print(f"   Tabla: {table_name}")
    print(f"   Registros: {len(df):,}")
    
    temp_csv = None
    
    try:
        # 1. Preparar DataFrame
        df_prep = preparar_dataframe_para_sql(df)
        
        # 2. Verificar acción a tomar
        accion = verificar_tabla_existente(table_name)
        
        if accion == 'cancel':
            print("❌ Operación cancelada")
            return False
        
        # 3. CREAR CSV TEMPORAL
        temp_csv = f"temp_{table_name}_{datetime.now().strftime('%H%M%S')}.csv"
        
        df_prep.to_csv(temp_csv, index=False, sep="¬", encoding="utf-8")
        
        # 4. CONFIGURAR ENGINE OPTIMIZADO
        conn_str = f"mssql+pyodbc://{SQL_CONFIG['username']}:{SQL_CONFIG['password']}@{SQL_CONFIG['server']}/{SQL_CONFIG['database']}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
        engine = create_engine(conn_str, fast_executemany=True)
        
        # 5. MANEJAR TABLA SEGÚN ACCIÓN
        if accion == 'replace':
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS [{table_name}]"))
            if_exists_mode = 'fail'
        elif accion == 'truncate':
            with engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE [{table_name}]"))
            if_exists_mode = 'append'
        else:
            if_exists_mode = 'append' if accion == 'append' else 'fail'
        
        # 6. CARGAR EN CHUNKS OPTIMIZADO
        chunksize = 50000
        total_chunks_loaded = 0
        
        print(f"\n📊 Cargando en chunks de {chunksize:,}...")
        
        for chunk in pd.read_csv(temp_csv, sep="¬", encoding="utf-8", chunksize=chunksize, engine="python"):
            chunk.to_sql(
                table_name, 
                engine, 
                if_exists=if_exists_mode, 
                index=False,
                method=None
            )
            
            total_chunks_loaded += 1
            registros_cargados = min(total_chunks_loaded * chunksize, len(df))
            print(f"  Chunk {total_chunks_loaded}: {registros_cargados:,} de {len(df):,} registros", end='\r')
            
            if_exists_mode = 'append'
        
        print()  # Nueva línea después del progreso
        
        # 7. VERIFICAR CARGA Y LIMPIAR
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM [{table_name}]"))
            count_final = result.fetchone()[0]
        
        engine.dispose()
        
        print(f"\n✅ Carga OPTIMIZADA completada!")
        print(f"   📊 Registros en tabla: {count_final:,}")
        print(f"   🗂️  Tabla: {table_name}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error en carga optimizada: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Limpiar archivo temporal
        if temp_csv and os.path.exists(temp_csv):
            try:
                os.remove(temp_csv)
                print(f"🗑️  Archivo temporal eliminado")
            except:
                print(f"⚠️  No se pudo eliminar: {temp_csv}")

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
    if 'PERIODO' in df.columns:
        periodos_validos = df['PERIODO'].dropna()
        if len(periodos_validos) > 0:
            print(f"📅 Rango de periodos: {periodos_validos.min()} a {periodos_validos.max()}")
    
    print("=" * 60)

# ======================
# FUNCIÓN PRINCIPAL
# ======================

def procesar_archivo_completo(archivo_txt, generar_csv=True, cargar_sql=True, csv_path=None):
    """Proceso completo: TXT → Limpieza → CSV → SQL Server"""
    print("🚀 INICIANDO PROCESO ETL - SCORING RIESGOS HISTÓRICO")
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
        
        # 4. CARGAR A SQL SERVER
        if cargar_sql:
            print("\n" + "=" * 30)
            print("FASE 3: CARGA A SQL SERVER")
            print("=" * 30)
            
            df_sql = df_limpio.copy()
            df_sql = limpiar_nombres_columnas_sql(df_sql)
            df_sql = convertir_tipos_datos_sql(df_sql)
            df_sql = limpiar_datos_sql(df_sql)
            
            table_name = generar_nombre_tabla(archivo_txt)
            
            exito_sql = cargar_dataframe_a_sql_optimizado(df_sql, table_name)
            
            if not exito_sql:
                print("⚠️  La carga a SQL Server falló")
        
        # 5. MOSTRAR RESUMEN
        mostrar_resumen_proceso(df_limpio, archivo_txt, csv_generado)
        
        print(f"\n🎉 PROCESO ETL SCORING RIESGOS COMPLETADO!")
        
        return df_limpio
        
    except Exception as e:
        print(f"❌ Error durante el proceso ETL: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Función principal - Proceso automático SCORING RIESGOS"""
    print("=" * 80)
    print("     SISTEMA ETL - SCORING RIESGOS HISTÓRICO - CALIDDA")
    print("=" * 80)
    
    # Usar directamente el archivo por defecto sin preguntar
    archivo = ARCHIVO_TXT_DEFAULT
    print(f"\n📁 Procesando archivo: {archivo}")
    
    resultado = procesar_archivo_completo(
        archivo_txt=archivo,
        generar_csv=True,
        cargar_sql=True
    )
    
    if resultado is not None:
        print("\n✅ Proceso SCORING RIESGOS finalizado exitosamente")
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
        import chardet
        import pyodbc
        import sqlalchemy
        print("✅ Todas las dependencias están instaladas\n")
    except ImportError as e:
        print(f"❌ Falta dependencia: {e}")
        print("Instala con: pip install pandas numpy chardet pyodbc sqlalchemy")
        exit(1)
    
    main()
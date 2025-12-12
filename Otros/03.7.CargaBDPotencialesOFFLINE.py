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
# CONFIGURACIÓN GLOBAL - OFFLINE
# ======================

# Archivo por defecto para OFFLINE
ARCHIVO_TXT_DEFAULT = r"D:\FNB\Reportes\04 Reporte Clientes Potenciales\Historico OFFLINE\ZSDR032_OFF_LI_20251101.txt"

# Conexión SQL Server
SQL_CONFIG = {
    "server": "192.168.64.250",
    "database": "BD_CALIDDA_FNB",
    "username": "ctorres",
    "password": "ibr2025"
}

# Mapeo de tipos de datos SQL Server - OFFLINE
COLUMN_TYPES_SQL = {
    "Fecha_Eval": "DATETIME",
    "Tipo_Docum": "VARCHAR(500)",
    "N_I_F_1": "VARCHAR(20)",
    "Int_cial": "BIGINT",
    "Nombre": "VARCHAR(500)",
    "Saldo_Cred": "DECIMAL(18,2)",
    "Cta_Contr": "BIGINT",
    "Distrito": "VARCHAR(500)",
    "Direccion": "VARCHAR(500)",
    "CaCta": "VARCHAR(500)",
    "Texto_categ_cuenta": "VARCHAR(500)"
}

# Mapeo de nombres originales a nombres SQL-compatibles - OFFLINE
MAPEO_NOMBRES_COLUMNAS = {
    "Fecha Eval": "Fecha_Eval",
    "Tipo Docum": "Tipo_Docum", 
    "N.I.F.1": "N_I_F_1",
    "Int.cial.": "Int_cial",
    "Nombre": "Nombre",
    "Saldo Créd": "Saldo_Cred",
    "Cta.Contr.": "Cta_Contr",
    "Distrito": "Distrito",
    "Dirección": "Direccion",
    "CaCta": "CaCta",
    "Texto categ.cuenta": "Texto_categ_cuenta"
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
        if linea_cabecera.count(sep) > 5:
            return sep
    return '\t'

def analizar_archivo_txt(archivo_entrada):
    """Analiza la estructura del archivo antes de la limpieza"""
    print("=== ANÁLISIS DEL ARCHIVO TXT OFFLINE ===")
    
    codificacion = detectar_codificacion(archivo_entrada)
    
    with open(archivo_entrada, 'r', encoding=codificacion) as file:
        lineas = file.readlines()
    
    print(f"Total de líneas: {len(lineas)}")
    
    if len(lineas) > 8:
        separador = detectar_separador(lineas[8])
        cabecera = lineas[8].strip().split(separador)
        print(f"Cabecera detectada: {len(cabecera)} columnas")
        
        problemas = []
        for i, linea in enumerate(lineas[10:], start=11):
            if linea.strip() == '':
                continue
            campos = linea.strip().split(separador)
            if len(campos) != len(cabecera):
                problemas.append((i, len(campos)))
        
        if problemas:
            print(f"Filas con problemas de estructura: {len(problemas)}")
        else:
            print("✅ No se detectaron problemas de estructura")
    
    print("=" * 50)

def limpiar_archivo_txt(archivo_entrada):
    """Limpia un archivo TXT de OFFLINE y retorna DataFrame"""
    print("=== INICIANDO LIMPIEZA DE ARCHIVO TXT OFFLINE ===")
    
    codificacion = detectar_codificacion(archivo_entrada)
    
    with open(archivo_entrada, 'r', encoding=codificacion) as file:
        lineas = file.readlines()
    
    print(f"Total de líneas leídas: {len(lineas)}")
    
    if len(lineas) <= 8:
        print("❌ Error: El archivo no tiene suficientes líneas para procesar")
        return None
    
    separador = detectar_separador(lineas[8])
    cabecera_raw = lineas[8].strip().split(separador)
    
    # Limpiar cabecera
    cabecera = []
    for i, col in enumerate(cabecera_raw):
        if col.strip() == '':
            cabecera.append(f'Columna_{i+1}')
        else:
            cabecera.append(col.strip())
    
    num_columnas_esperadas = len(cabecera)
    print(f"Cabecera extraída: {num_columnas_esperadas} columnas")
    
    # Procesar datos desde fila 11 (índice 10)
    datos_raw = lineas[10:]
    datos_limpios = []
    filas_corregidas = 0
    filas_omitidas = 0
    
    for i, linea in enumerate(datos_raw, start=11):
        if linea.strip() == '':
            continue
            
        campos = linea.strip().split(separador)
        
        if len(campos) == num_columnas_esperadas + 1:
            # Manejar problema de columna extra
            cta_contr_index = None
            direccion_index = None
            
            for j, col in enumerate(cabecera):
                if 'Cta.Contr' in col or 'Cta Contr' in col:
                    cta_contr_index = j
                elif 'Dirección' in col or 'Direccion' in col:
                    if direccion_index is None:
                        direccion_index = j
            
            if cta_contr_index is not None and len(campos) > cta_contr_index:
                cuenta = campos[cta_contr_index]
                
                # Intentar corregir uniendo campos de dirección
                if direccion_index is not None and direccion_index < len(campos) - 1:
                    direccion_completa = str(campos[direccion_index]) + ' ' + str(campos[direccion_index + 1])
                    campos_corregidos = campos[:direccion_index] + [direccion_completa] + campos[direccion_index + 2:]
                    
                    if len(campos_corregidos) == num_columnas_esperadas:
                        datos_limpios.append(campos_corregidos)
                        filas_corregidas += 1
                        continue
            
            # Si no se pudo corregir, eliminar la última columna
            datos_limpios.append(campos[:num_columnas_esperadas])
            filas_corregidas += 1
            
        elif len(campos) == num_columnas_esperadas:
            datos_limpios.append(campos)
        else:
            filas_omitidas += 1
    
    if filas_corregidas > 0:
        print(f"Filas corregidas: {filas_corregidas}")
    if filas_omitidas > 0:
        print(f"Filas omitidas: {filas_omitidas}")
    print(f"Total de filas procesadas: {len(datos_limpios)}")
    
    if len(datos_limpios) == 0:
        print("❌ No se procesaron datos válidos")
        return None
    
    # Crear DataFrame
    df = pd.DataFrame(datos_limpios, columns=cabecera)
    
    # Eliminar columnas vacías
    columnas_a_eliminar = [col for col in df.columns if 'Columna' in col]
    if columnas_a_eliminar:
        df = df.drop(columns=columnas_a_eliminar)
    
    # FILTRAR SOLO COLUMNAS MAPEADAS
    columnas_originales_mapeadas = list(MAPEO_NOMBRES_COLUMNAS.keys())
    columnas_existentes = [col for col in df.columns if col in columnas_originales_mapeadas]
    
    if len(columnas_existentes) < len(columnas_originales_mapeadas):
        columnas_faltantes = set(columnas_originales_mapeadas) - set(columnas_existentes)
        print(f"⚠️  Columnas esperadas pero no encontradas: {', '.join(columnas_faltantes)}")
    
    # Mantener solo las columnas mapeadas
    df = df[columnas_existentes]
    print(f"✅ Columnas seleccionadas (mapeadas): {len(columnas_existentes)} columnas")
    
    # Reemplazar "LA ALBORADA" por "COMAS"
    if 'Distrito' in df.columns:
        antes = df['Distrito'].str.contains('LA ALBORADA', na=False).sum()
        if antes > 0:
            df['Distrito'] = df['Distrito'].str.replace('LA ALBORADA', 'COMAS', regex=False)
            print(f"Distritos actualizados: {antes} registros 'LA ALBORADA' → 'COMAS'")
    
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
# FUNCIONES OPTIMIZADAS PARA SQL SERVER
# ======================

def generar_nombre_tabla(archivo_path):
    """Genera nombre de tabla basado en el archivo - OFFLINE"""
    nombre_archivo = os.path.basename(archivo_path)
    
    # Buscar patrón de fecha en el nombre del archivo (YYYYMMDD)
    fecha_match = re.search(r'(\d{8})', nombre_archivo)
    if fecha_match:
        fecha = fecha_match.group(1)
        # La fecha ya viene en formato YYYYMMDD
        return f"BD_Potenciales_OFFLINE_{fecha}"
    
    # Fallback: usar fecha actual
    timestamp = datetime.now().strftime("%Y%m%d")
    return f"BD_Potenciales_OFFLINE_{timestamp}"

def limpiar_nombres_columnas_sql(df):
    """Limpia nombres de columnas para SQL Server"""
    print("\n=== Limpiando nombres de columnas para SQL ===")
    
    nuevos_nombres = []
    cambios = []
    
    for col in df.columns:
        # PRIMERO: Verificar mapeo directo con espacios
        col_con_espacios = col.strip()
        
        if col_con_espacios in MAPEO_NOMBRES_COLUMNAS:
            nuevo_nombre = MAPEO_NOMBRES_COLUMNAS[col_con_espacios]
        else:
            # SEGUNDO: Verificar mapeo sin espacios extra (normalizar)
            col_normalizado = ' '.join(col.split())
            
            if col_normalizado in MAPEO_NOMBRES_COLUMNAS:
                nuevo_nombre = MAPEO_NOMBRES_COLUMNAS[col_normalizado]
            else:
                # TERCERO: Limpieza automática si no está en el mapeo
                nuevo_nombre = col
                nuevo_nombre = re.sub(r'[^\w\s]', '_', nuevo_nombre)
                nuevo_nombre = re.sub(r'\s+', '_', nuevo_nombre)
                nuevo_nombre = re.sub(r'_+', '_', nuevo_nombre)
                nuevo_nombre = nuevo_nombre.strip('_')
        
        if col != nuevo_nombre:
            cambios.append((col, nuevo_nombre))
        
        nuevos_nombres.append(nuevo_nombre)
    
    if cambios:
        print(f"Columnas renombradas: {len(cambios)}")
        for orig, nuevo in cambios[:5]:
            print(f"  {orig} → {nuevo}")
        if len(cambios) > 5:
            print(f"  ... y {len(cambios) - 5} más")
    
    df.columns = nuevos_nombres
    return df

def convertir_tipos_datos_sql(df):
    """Convierte tipos de datos según mapeo SQL - OFFLINE"""
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
    print("          RESUMEN DEL PROCESO - OFFLINE")
    print("=" * 60)
    print(f"📁 Archivo original: {os.path.basename(archivo_original)}")
    if csv_generado:
        print(f"📄 CSV generado: {os.path.basename(csv_generado)}")
    print(f"📊 Registros procesados: {len(df):,}")
    print(f"📋 Columnas: {len(df.columns)}")
    print("=" * 60)

# ======================
# FUNCIÓN PRINCIPAL
# ======================

def procesar_archivo_completo(archivo_txt, generar_csv=True, cargar_sql=True, csv_path=None):
    """Proceso completo: TXT → Limpieza → CSV → SQL Server"""
    print("🚀 INICIANDO PROCESO ETL - OFFLINE")
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
        
        print(f"\n🎉 PROCESO ETL OFFLINE COMPLETADO!")
        
        return df_limpio
        
    except Exception as e:
        print(f"❌ Error durante el proceso ETL: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Función principal - Proceso automático OFFLINE"""
    print("=" * 80)
    print("     SISTEMA ETL - CLIENTES POTENCIALES OFFLINE - CALIDDA")
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
        print("\n✅ Proceso OFFLINE finalizado exitosamente")
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
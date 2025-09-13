import pandas as pd
import numpy as np
import re
import chardet
import pyodbc
from sqlalchemy import create_engine, text
import os
from datetime import datetime
from sqlalchemy import create_engine, text

# ======================
# CONFIGURACIÓN GLOBAL
# ======================

# Archivos por defecto
ARCHIVO_TXT_DEFAULT = r"D:\FNB\Reportes\04 Reporte Clientes Potenciales\2025\09. Setiembre\BD07092025\BD07092025.txt"

# Conexión SQL Server
SQL_CONFIG = {
    "server": "192.168.64.250",
    "database": "BD_CALIDDA_FNB",
    "username": "ctorres",
    "password": "ibr2025"
}

# Mapeo de tipos de datos SQL Server
COLUMN_TYPES_SQL = {
    "Fecha_Eval": "DATETIME",
    "Tipo_Docum": "VARCHAR(500)",
    "N_I_F_1": "BIGINT",
    "Int_cial": "BIGINT",
    "Nombre": "VARCHAR(500)",
    "Saldo_Cred": "BIGINT",
    "LC_Mod": "VARCHAR(500)",
    "Cta_Contr": "BIGINT",
    "Distrito": "VARCHAR(500)",
    "Direccion": "VARCHAR(500)",
    "NSE_1": "INT",
    "Fecha_Alta": "DATETIME",
    "Cta_Ctto_2": "BIGINT",
    "Distrito_2": "VARCHAR(500)",
    "Direccion_2": "VARCHAR(500)",
    "NSE_2": "INT",
    "FechaAlta2": "DATETIME",
    "Cta_Ctto_3": "BIGINT",
    "Distrito_3": "VARCHAR(500)",
    "Direccion_3": "VARCHAR(500)",
    "NSE_3": "INT",
    "FechaAlta3": "DATETIME",
    "Cta_Ctto_4": "BIGINT",
    "Distrito_4": "VARCHAR(500)",
    "Direccion_4": "VARCHAR(500)",
    "NSE_4": "INT",
    "FechaAlta4": "DATETIME",
    "Cta_Ctto_5": "BIGINT",
    "Distrito_5": "VARCHAR(500)",
    "Direccion_5": "VARCHAR(500)",
    "NSE_5": "INT",
    "FechaAlta5": "DATETIME",
    "CaCta": "VARCHAR(500)",
    "Texto_categ_cuenta": "VARCHAR(500)"
}

# Mapeo de nombres originales a nombres SQL-compatibles
MAPEO_NOMBRES_COLUMNAS = {
    "Fecha Eval": "Fecha_Eval",
    "Tipo Docum": "Tipo_Docum", 
    "N.I.F.1": "N_I_F_1",
    "Int.cial.": "Int_cial",
    "Nombre": "Nombre",
    "Saldo Créd": "Saldo_Cred",
    "LC Mod": "LC_Mod",
    "Cta.Contr.": "Cta_Contr",
    "Distrito": "Distrito",
    "Dirección": "Direccion",
    "NSE 1": "NSE_1",
    "Fecha Alta": "Fecha_Alta",
    "Cta.Ctto 2": "Cta_Ctto_2",
    "Distrito 2": "Distrito_2",
    "Dirección 2": "Direccion_2",
    "NSE 2": "NSE_2",
    "FechaAlta2": "FechaAlta2",
    "Cta.Ctto 3": "Cta_Ctto_3",
    "Distrito 3": "Distrito_3",
    "Dirección 3": "Direccion_3",
    "NSE 3": "NSE_3",
    "FechaAlta3": "FechaAlta3",
    "Cta.Ctto 4": "Cta_Ctto_4",
    "Distrito 4": "Distrito_4",
    "Dirección 4": "Direccion_4",
    "NSE 4": "NSE_4",
    "FechaAlta4": "FechaAlta4",
    "Cta.Ctto 5": "Cta_Ctto_5",
    "Distrito 5": "Distrito_5",
    "Dirección 5": "Direccion_5",
    "NSE 5": "NSE_5",
    "FechaAlta5": "FechaAlta5",
    "CaCta": "CaCta",
    "Texto categ.cuenta": "Texto_categ_cuenta"
}

# ======================
# FUNCIONES DE LIMPIEZA DE ARCHIVOS TXT (MANTENIDAS ORIGINALES)
# ======================

def detectar_codificacion(archivo):
    """
    Detecta la codificación del archivo con mejor manejo de codificaciones problemáticas
    """
    # Lista de codificaciones más comunes para archivos en español
    codificaciones_comunes = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1', 'utf-16']
    
    # Primero probar codificaciones comunes directamente
    print("Probando codificaciones comunes...")
    for encoding in codificaciones_comunes:
        try:
            with open(archivo, 'r', encoding=encoding) as file:
                # Leer una muestra más grande para verificar
                sample = file.read(5000)
                # Verificar que no hay caracteres extraños
                if len(sample) > 0 and not any(ord(char) > 65535 for char in sample[:100]):
                    print(f"Codificación funcional encontrada: {encoding}")
                    return encoding
        except (UnicodeDecodeError, UnicodeError):
            continue
    
    # Si las codificaciones comunes no funcionan, intentar con chardet pero con validación
    try:
        with open(archivo, 'rb') as file:
            raw_data = file.read(50000)  # Leer más datos para mejor detección
            resultado = chardet.detect(raw_data)
            
            if resultado and resultado['encoding']:
                detected_encoding = resultado['encoding'].lower()
                confidence = resultado.get('confidence', 0)
                
                print(f"Chardet detectó: {resultado['encoding']} (confianza: {confidence:.2f})")
                
                # Filtrar codificaciones problemáticas o poco comunes
                codificaciones_problematicas = [
                    'johab', 'euc-kr', 'cp949',  # Coreano
                    'big5', 'gb2312', 'gbk',     # Chino
                    'shift_jis', 'euc-jp',       # Japonés
                    'koi8-r', 'cp1251',          # Cirílico
                ]
                
                # Si la codificación detectada es problemática, no usarla
                if any(prob in detected_encoding for prob in codificaciones_problematicas):
                    print(f"⚠️  Codificación {resultado['encoding']} es problemática para archivos en español")
                    print("Usando latin-1 como alternativa segura")
                    return 'latin-1'
                
                # Si la confianza es alta y no es problemática, probar la codificación detectada
                if confidence > 0.8:
                    try:
                        with open(archivo, 'r', encoding=resultado['encoding']) as file:
                            test_read = file.read(1000)
                        print(f"Usando codificación detectada: {resultado['encoding']}")
                        return resultado['encoding']
                    except (UnicodeDecodeError, UnicodeError):
                        print(f"⚠️  Codificación detectada {resultado['encoding']} falló en la prueba")
    except Exception as e:
        print(f"Error en detección con chardet: {str(e)}")
    
    # Fallback: usar latin-1 que es la más compatible
    print("⚠️  No se pudo detectar codificación confiable, usando latin-1 como fallback")
    print("   (latin-1 puede leer cualquier archivo de 8 bits sin errores)")
    return 'latin-1'

def detectar_separador(linea_cabecera):
    """
    Detecta el separador utilizado en el archivo
    """
    separadores = ['\t', ';', ',', '|']
    for sep in separadores:
        if linea_cabecera.count(sep) > 10:
            return sep
    return '\t'

def analizar_archivo_txt(archivo_entrada):
    """
    Analiza la estructura del archivo antes de la limpieza
    """
    print("=== ANÁLISIS DEL ARCHIVO TXT ===")
    
    codificacion = detectar_codificacion(archivo_entrada)
    
    with open(archivo_entrada, 'r', encoding=codificacion) as file:
        lineas = file.readlines()
    
    print(f"Total de líneas: {len(lineas)}")
    print(f"Metadatos (filas 0-8):")
    for i in range(min(9, len(lineas))):
        print(f"  Fila {i}: {lineas[i].strip()[:100]}...")
    
    if len(lineas) > 8:
        separador = detectar_separador(lineas[8])
        cabecera = lineas[8].strip().split(separador)
        print(f"\nCabecera (fila 9): {len(cabecera)} columnas")
        
        problemas = []
        for i, linea in enumerate(lineas[10:], start=11):
            if linea.strip() == '':
                continue
            campos = linea.strip().split(separador)
            if len(campos) != len(cabecera):
                problemas.append((i, len(campos)))
                if len(problemas) <= 5:
                    print(f"  Problema en fila {i}: {len(campos)} columnas (esperadas: {len(cabecera)})")
        
        print(f"\nFilas con problemas de estructura: {len(problemas)}")
    
    print("=" * 50)

def limpiar_archivo_txt(archivo_entrada):
    """
    Limpia un archivo TXT con problemas de estructura y retorna DataFrame
    """
    print("=== INICIANDO LIMPIEZA DE ARCHIVO TXT ===")
    
    # Detectar codificación del archivo
    codificacion = detectar_codificacion(archivo_entrada)
    
    # Leer el archivo completo
    with open(archivo_entrada, 'r', encoding=codificacion) as file:
        lineas = file.readlines()
    
    print(f"Total de líneas leídas: {len(lineas)}")
    
    if len(lineas) <= 8:
        print("❌ Error: El archivo no tiene suficientes líneas para procesar")
        return None
    
    # Detectar separador y extraer cabecera
    separador = detectar_separador(lineas[8])
    print(f"Separador detectado: {'TAB' if separador == '\\t' else repr(separador)}")
    
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
    
    # Procesar datos desde fila 11
    datos_raw = lineas[10:]
    datos_limpios = []
    filas_corregidas = 0
    filas_omitidas = 0
    
    print("Procesando filas de datos...")
    
    for i, linea in enumerate(datos_raw, start=11):
        if linea.strip() == '':
            continue
            
        campos = linea.strip().split(separador)
        
        if len(campos) == num_columnas_esperadas + 1:
            # Manejar problema de columna extra
            cta_contr_index = None
            direccion_index = None
            
            for j, col in enumerate(cabecera):
                if 'Cta.Contr' in col:
                    cta_contr_index = j
                elif 'Dirección' in col and direccion_index is None:
                    direccion_index = j
            
            if cta_contr_index is not None and len(campos) > cta_contr_index:
                cuenta = campos[cta_contr_index]
                
                if cuenta in ['5199463', '5320440']:
                    print(f"Fila {i}: Corrigiendo cuenta {cuenta}")
                    
                    if direccion_index is not None and direccion_index < len(campos) - 1:
                        direccion_completa = str(campos[direccion_index]) + ' ' + str(campos[direccion_index + 1])
                        campos_corregidos = campos[:direccion_index] + [direccion_completa] + campos[direccion_index + 2:]
                        
                        if len(campos_corregidos) == num_columnas_esperadas:
                            datos_limpios.append(campos_corregidos)
                            filas_corregidas += 1
                            continue
            
            print(f"Fila {i}: Eliminando columna extra")
            datos_limpios.append(campos[:num_columnas_esperadas])
            filas_corregidas += 1
            
        elif len(campos) == num_columnas_esperadas:
            datos_limpios.append(campos)
        else:
            print(f"Advertencia: Fila {i} tiene {len(campos)} columnas, se omite")
            filas_omitidas += 1
    
    print(f"Filas corregidas: {filas_corregidas}")
    print(f"Filas omitidas: {filas_omitidas}")
    print(f"Total de filas procesadas: {len(datos_limpios)}")
    
    if len(datos_limpios) == 0:
        print("❌ No se procesaron datos válidos")
        return None
    
    # Crear DataFrame
    df = pd.DataFrame(datos_limpios, columns=cabecera)
    print(f"DataFrame creado: {df.shape[0]} filas x {df.shape[1]} columnas")
    
    # Eliminar columnas vacías
    columnas_a_eliminar = [col for col in df.columns if 'Columna' in col]
    if columnas_a_eliminar:
        print(f"Eliminando columnas vacías: {columnas_a_eliminar}")
        df = df.drop(columns=columnas_a_eliminar)
    
    # Reemplazar "LA ALBORADA" por "COMAS"
    columnas_distrito = [col for col in df.columns if 'Distrito' in col]
    for col in columnas_distrito:
        if col in df.columns:
            antes = df[col].str.contains('LA ALBORADA', na=False).sum()
            df[col] = df[col].str.replace('LA ALBORADA', 'COMAS', regex=False)
            if antes > 0:
                print(f"En columna '{col}': {antes} registros cambiados de 'LA ALBORADA' a 'COMAS'")
    
    # Limpieza general
    df = aplicar_limpieza_general(df)
    
    return df

def aplicar_limpieza_general(df):
    """
    Aplica limpieza general a los datos del DataFrame
    """
    print("Aplicando limpieza general de datos...")
    
    # Limpiar datos de texto
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).str.replace('\n', ' ', regex=False)
            df[col] = df[col].str.replace('\r', ' ', regex=False)
            df[col] = df[col].str.replace('\t', ' ', regex=False)
            df[col] = df[col].str.strip()
            df[col] = df[col].replace(['nan', 'NaN', 'NULL', ''], np.nan)
    
    # Convertir tipos de datos
    df = convertir_tipos_datos_basicos(df)
    
    return df

def convertir_tipos_datos_basicos(df):
    """
    Convierte tipos de datos básicos
    """
    print("Convirtiendo tipos de datos básicos...")
    
    for col in df.columns:
        if df[col].dtype == 'object':
            # Verificar si parece fecha
            muestra = df[col].dropna().head(10)
            if any(re.match(r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}', str(val)) for val in muestra):
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    print(f"  Columna '{col}' convertida a datetime")
                    continue
                except:
                    pass
            
            # Verificar si parece numérico
            try:
                col_limpia = df[col].astype(str).str.replace(',', '').str.replace(' ', '')
                numeric_df = pd.to_numeric(col_limpia, errors='coerce')
                
                if numeric_df.notna().sum() / len(df) > 0.8:
                    df[col] = numeric_df
                    print(f"  Columna '{col}' convertida a numérico")
            except:
                pass
    
    return df

# ======================
# FUNCIONES OPTIMIZADAS PARA SQL SERVER
# ======================

def generar_nombre_tabla(archivo_path):
    """
    Genera nombre de tabla basado en el archivo
    """
    nombre_archivo = os.path.basename(archivo_path)
    if "BD" in nombre_archivo and any(c.isdigit() for c in nombre_archivo):
        fecha_match = re.search(r'(\d{8})', nombre_archivo)
        if fecha_match:
            fecha = fecha_match.group(1)
            return f"BD_Potenciales_{fecha}"
    
    timestamp = datetime.now().strftime("%d%m%Y")
    return f"BD_Potenciales_{timestamp}"

def limpiar_nombres_columnas_sql(df):
    """
    Limpia nombres de columnas para SQL Server
    """
    print("Limpiando nombres de columnas para SQL...")
    
    nuevos_nombres = []
    for col in df.columns:
        if col in MAPEO_NOMBRES_COLUMNAS:
            nuevos_nombres.append(MAPEO_NOMBRES_COLUMNAS[col])
        else:
            # Limpieza automática
            nuevo_nombre = col
            nuevo_nombre = re.sub(r'[^\w\s]', '_', nuevo_nombre)
            nuevo_nombre = re.sub(r'\s+', '_', nuevo_nombre)
            nuevo_nombre = re.sub(r'_+', '_', nuevo_nombre)
            nuevo_nombre = nuevo_nombre.strip('_')
            nuevos_nombres.append(nuevo_nombre)
    
    # Mostrar cambios
    cambios = [(orig, nuevo) for orig, nuevo in zip(df.columns, nuevos_nombres) if orig != nuevo]
    if cambios:
        print("Cambios en nombres de columnas:")
        for orig, nuevo in cambios[:10]:  # Mostrar solo los primeros 10
            print(f"  {orig} → {nuevo}")
        if len(cambios) > 10:
            print(f"  ... y {len(cambios) - 10} más")
    
    df.columns = nuevos_nombres
    return df

def convertir_tipos_datos_sql(df):
    """
    Convierte tipos de datos según mapeo SQL
    """
    print("\n=== Conversión de tipos para SQL Server ===")
    
    for col in df.columns:
        if col not in COLUMN_TYPES_SQL:
            print(f"⚠️  Columna '{col}' no está en el mapeo de tipos")
            continue
            
        sql_type = COLUMN_TYPES_SQL[col]
        
        try:
            if sql_type.startswith("VARCHAR"):
                df[col] = df[col].astype(str)
                df[col] = df[col].replace(['nan', 'NaN', 'None', 'null'], pd.NA)
                
            elif sql_type in ("INT", "BIGINT"):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                
            elif "DECIMAL" in sql_type:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                
            elif sql_type == "DATETIME":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            
            print(f"✓ {col}: {sql_type}")
            
        except Exception as e:
            print(f"❌ Error convirtiendo {col}: {str(e)}")
    
    return df

def limpiar_datos_sql(df):
    """
    Limpia datos para SQL Server
    """
    print("Limpiando datos para SQL Server...")
    
    # Limpiar caracteres problemáticos similar a tu código rápido
    for col in df.select_dtypes(include=["object"]).columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str)
            df[col] = df[col].str.replace("¬", "-", regex=False)  # ← Clave como tu código rápido
            df[col] = df[col].str.replace("\n", " ", regex=False)
            df[col] = df[col].str.replace("\r", " ", regex=False)
            df[col] = df[col].str.replace("\t", " ", regex=False)
            df[col] = df[col].str.replace('"', '""', regex=False)
            df[col] = df[col].replace(['nan', 'NaN', 'None'], None)
    
    return df

def verificar_tabla_existente(table_name):
    """
    Verifica si la tabla existe y consulta acción
    """
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
    """
    Prepara el DataFrame optimizado para SQL Server (simplificado como tu código rápido)
    """
    print("🔧 Preparando DataFrame para SQL Server...")
    
    df_prep = df.copy()
    
    # Limpieza básica pero efectiva
    for col in df_prep.columns:
        if df_prep[col].dtype == 'object':
            df_prep[col] = df_prep[col].astype(str)
            df_prep[col] = df_prep[col].replace(['<NA>', 'nan', 'NaN', 'None', 'NULL'], None)
            # Remover solo los caracteres más problemáticos
            df_prep[col] = df_prep[col].str.replace('\x00', '', regex=False)  
        elif pd.api.types.is_datetime64_any_dtype(df_prep[col]):
            df_prep[col] = df_prep[col].where(pd.notnull(df_prep[col]), None)
        elif pd.api.types.is_numeric_dtype(df_prep[col]):
            if df_prep[col].dtype == 'Int64':
                df_prep[col] = df_prep[col].where(pd.notnull(df_prep[col]), None)
            else:
                df_prep[col] = df_prep[col].replace([np.inf, -np.inf], None)
                df_prep[col] = df_prep[col].where(pd.notnull(df_prep[col]), None)
    
    print(f"✅ DataFrame preparado: {len(df_prep)} filas, {len(df_prep.columns)} columnas")
    return df_prep

def cargar_dataframe_a_sql_optimizado(df, table_name):
    """
    NUEVA FUNCIÓN: Carga optimizada basada en tu código rápido + manejo de CSV temporal
    """
    print(f"\n🚀 Carga OPTIMIZADA a SQL Server...")
    print(f"   Tabla: {table_name}")
    print(f"   Registros: {len(df):,}")
    
    try:
        # 1. Preparar DataFrame
        df_prep = preparar_dataframe_para_sql(df)
        
        # 2. Verificar acción a tomar
        accion = verificar_tabla_existente(table_name)
        
        if accion == 'cancel':
            print("❌ Operación cancelada")
            return False
        
        # 3. CREAR CSV TEMPORAL (como tu código rápido)
        temp_csv = f"temp_{table_name}_{datetime.now().strftime('%H%M%S')}.csv"
        print(f"🔄 Creando CSV temporal: {temp_csv}")
        
        # Usar separador no conflictivo como tu código rápido
        df_prep.to_csv(temp_csv, index=False, sep="¬", encoding="utf-8")
        
        # 4. CONFIGURAR ENGINE OPTIMIZADO
        conn_str = f"mssql+pyodbc://{SQL_CONFIG['username']}:{SQL_CONFIG['password']}@{SQL_CONFIG['server']}/{SQL_CONFIG['database']}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
        engine = create_engine(conn_str, fast_executemany=True)
        
        # 5. MANEJAR TABLA SEGÚN ACCIÓN
        if accion == 'replace':
            # Drop tabla si existe
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS [{table_name}]"))
            if_exists_mode = 'fail'  # Primera carga creará la tabla
        elif accion == 'truncate':
            # Truncate tabla
            with engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE [{table_name}]"))
            if_exists_mode = 'append'
        else:  # append o create
            if_exists_mode = 'append' if accion == 'append' else 'fail'
        
        # 6. CARGAR EN CHUNKS OPTIMIZADO (como tu código rápido)
        chunksize = 50000  # Tamaño optimizado
        total_chunks_loaded = 0
        
        print(f"📊 Cargando desde CSV temporal en chunks de {chunksize:,}...")
        
        for chunk in pd.read_csv(temp_csv, sep="¬", encoding="utf-8", chunksize=chunksize, engine="python"):
            chunk.to_sql(
                table_name, 
                engine, 
                if_exists=if_exists_mode, 
                index=False,
                method=None  # Usar método por defecto que es rápido
            )
            
            total_chunks_loaded += 1
            registros_cargados = total_chunks_loaded * chunksize
            if registros_cargados > len(df):
                registros_cargados = len(df)
                
            print(f"  Chunk {total_chunks_loaded}: {len(chunk):,} registros (Total: {registros_cargados:,})")
            
            # Después del primer chunk, siempre append
            if_exists_mode = 'append'
        
        # 7. VERIFICAR CARGA Y LIMPIAR
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM [{table_name}]"))
            count_final = result.fetchone()[0]
        
        # Limpiar archivo temporal
        try:
            os.remove(temp_csv)
            print(f"🗑️  Archivo temporal eliminado: {temp_csv}")
        except:
            print(f"⚠️  No se pudo eliminar archivo temporal: {temp_csv}")
        
        engine.dispose()
        
        print(f"\n✅ Carga OPTIMIZADA completada!")
        print(f"   📊 Registros en tabla: {count_final:,}")
        print(f"   🗂️  Tabla: {table_name}")
        print(f"   ⚡ Método: CSV temporal + chunks optimizados")
        
        return True
        
    except Exception as e:
        print(f"❌ Error en carga optimizada: {str(e)}")
        # Limpiar archivo temporal en caso de error
        try:
            if 'temp_csv' in locals():
                os.remove(temp_csv)
        except:
            pass
        return False

def cargar_dataframe_a_sql(df, table_name):
    """
    Función principal que usa el método optimizado
    """
    return cargar_dataframe_a_sql_optimizado(df, table_name)

# ======================
# FUNCIONES PRINCIPALES ACTUALIZADAS
# ======================

def mostrar_resumen_proceso(df, archivo_original, csv_generado=None):
    """
    Muestra resumen completo del proceso
    """
    if df is None:
        print("❌ No se pudo generar resumen - DataFrame es None")
        return
    
    print("\n" + "=" * 60)
    print("                RESUMEN DEL PROCESO")
    print("=" * 60)
    print(f"📁 Archivo original: {os.path.basename(archivo_original)}")
    if csv_generado:
        print(f"📄 CSV generado: {os.path.basename(csv_generado)}")
    print(f"📊 Registros procesados: {len(df):,}")
    print(f"📋 Columnas finales: {len(df.columns)}")
    
    print(f"\n📈 Tipos de datos:")
    tipo_counts = df.dtypes.value_counts()
    for tipo, count in tipo_counts.items():
        print(f"   {tipo}: {count} columnas")
    
    # Verificar cuentas problemáticas
    col_cuenta = next((col for col in df.columns if 'Cta' in col and 'Contr' in col), None)
    if col_cuenta:
        cuentas_problema = df[df[col_cuenta].isin(['5199463', '5320440'])]
        print(f"\n🔧 Cuentas problemáticas corregidas: {len(cuentas_problema)}")
    
    # Verificar cambios en distritos
    columnas_distrito = [col for col in df.columns if 'Distrito' in col]
    if columnas_distrito:
        total_comas = sum(df[col].str.contains('COMAS', na=False).sum() for col in columnas_distrito)
        print(f"🏘️  Registros con distrito 'COMAS': {total_comas}")
    
    print("=" * 60)

def procesar_archivo_completo(archivo_txt, generar_csv=True, cargar_sql=True, csv_path=None):
    """
    Proceso completo: TXT → Limpieza → CSV → SQL Server (OPTIMIZADO)
    
    Parámetros:
    archivo_txt: archivo TXT de entrada
    generar_csv: si generar archivo CSV intermedio
    cargar_sql: si cargar a SQL Server
    csv_path: ruta específica para CSV (opcional)
    """
    print("🚀 INICIANDO PROCESO ETL COMPLETO OPTIMIZADO")
    print("=" * 60)
    print(f"📁 Archivo de entrada: {archivo_txt}")
    
    # Verificar que el archivo existe
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
        
        # 4. CARGAR A SQL SERVER (OPTIMIZADO)
        if cargar_sql:
            print("\n" + "=" * 30)
            print("FASE 3: CARGA OPTIMIZADA A SQL SERVER")
            print("=" * 30)
            
            # Preparar datos para SQL
            df_sql = df_limpio.copy()
            df_sql = limpiar_nombres_columnas_sql(df_sql)
            df_sql = convertir_tipos_datos_sql(df_sql)
            df_sql = limpiar_datos_sql(df_sql)
            
            # Generar nombre de tabla
            table_name = generar_nombre_tabla(archivo_txt)
            
            # Cargar a SQL usando método optimizado
            exito_sql = cargar_dataframe_a_sql_optimizado(df_sql, table_name)
            
            if not exito_sql:
                print("⚠️  La carga a SQL Server falló, pero el archivo fue limpiado correctamente")
        
        # 5. MOSTRAR RESUMEN
        mostrar_resumen_proceso(df_limpio, archivo_txt, csv_generado)
        
        print(f"\n🎉 PROCESO ETL OPTIMIZADO COMPLETADO!")
        if generar_csv:
            print(f"✅ CSV generado: {csv_generado}")
        if cargar_sql:
            print(f"✅ Datos cargados en SQL Server con método optimizado")
        
        return df_limpio
        
    except Exception as e:
        print(f"❌ Error durante el proceso ETL: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """
    Función principal con menú interactivo
    """
    print("=" * 80)
    print("       SISTEMA ETL OPTIMIZADO - CLIENTES POTENCIALES CALIDDA")
    print("=" * 80)
    print("Opciones disponibles:")
    print("1. Proceso completo OPTIMIZADO (TXT → Limpieza → CSV → SQL Server)")
    print("2. Solo limpieza y generación de CSV")
    print("3. Solo carga de CSV existente a SQL Server (OPTIMIZADA)")
    print("4. Análisis de archivo TXT")
    print("5. Configuración personalizada")
    print("6. Salir")
    
    while True:
        try:
            opcion = input("\nSelecciona una opción (1-6): ").strip()
            
            if opcion == '1':
                # Proceso completo optimizado
                archivo = input(f"Ruta del archivo TXT (Enter para usar por defecto): ").strip()
                if not archivo:
                    archivo = ARCHIVO_TXT_DEFAULT
                
                resultado = procesar_archivo_completo(
                    archivo_txt=archivo,
                    generar_csv=True,
                    cargar_sql=True
                )
                
                if resultado is not None:
                    print("\n✅ Proceso completo OPTIMIZADO finalizado exitosamente")
                else:
                    print("\n❌ El proceso falló")
                
                break
                
            elif opcion == '2':
                # Solo limpieza y CSV
                archivo = input(f"Ruta del archivo TXT (Enter para usar por defecto): ").strip()
                if not archivo:
                    archivo = ARCHIVO_TXT_DEFAULT
                
                csv_salida = input("Ruta del CSV de salida (Enter para automático): ").strip()
                if not csv_salida:
                    csv_salida = None
                
                resultado = procesar_archivo_completo(
                    archivo_txt=archivo,
                    generar_csv=True,
                    cargar_sql=False,
                    csv_path=csv_salida
                )
                
                if resultado is not None:
                    print("\n✅ Limpieza y generación de CSV completada")
                else:
                    print("\n❌ El proceso falló")
                
                break
                
            elif opcion == '3':
                # Solo carga SQL optimizada
                csv_archivo = input("Ruta del archivo CSV: ").strip()
                
                if not os.path.exists(csv_archivo):
                    print(f"❌ El archivo CSV no existe: {csv_archivo}")
                    continue
                
                # Leer CSV y preparar para SQL
                print("📖 Leyendo CSV...")
                df = pd.read_csv(csv_archivo, encoding='utf-8')
                
                df = limpiar_nombres_columnas_sql(df)
                df = convertir_tipos_datos_sql(df)
                df = limpiar_datos_sql(df)
                
                table_name = generar_nombre_tabla(csv_archivo)
                exito = cargar_dataframe_a_sql_optimizado(df, table_name)
                
                if exito:
                    print("\n✅ Carga OPTIMIZADA a SQL Server completada")
                else:
                    print("\n❌ La carga falló")
                
                break
                
            elif opcion == '4':
                # Solo análisis
                archivo = input(f"Ruta del archivo TXT (Enter para usar por defecto): ").strip()
                if not archivo:
                    archivo = ARCHIVO_TXT_DEFAULT
                
                if os.path.exists(archivo):
                    analizar_archivo_txt(archivo)
                    print("\n✅ Análisis completado")
                else:
                    print(f"❌ El archivo no existe: {archivo}")
                
                break
                
            elif opcion == '5':
                # Configuración personalizada
                mostrar_configuracion_personalizada()
                break
                
            elif opcion == '6':
                print("👋 Saliendo del sistema...")
                break
                
            else:
                print("❌ Opción inválida. Por favor, selecciona 1-6.")
                
        except KeyboardInterrupt:
            print("\n\n👋 Saliendo del sistema...")
            break
        except Exception as e:
            print(f"❌ Error: {str(e)}")

def mostrar_configuracion_personalizada():
    """
    Permite configuración personalizada del proceso
    """
    print("\n" + "=" * 50)
    print("        CONFIGURACIÓN PERSONALIZADA OPTIMIZADA")
    print("=" * 50)
    
    # Archivo de entrada
    archivo_txt = input(f"Archivo TXT (Enter para usar por defecto): ").strip()
    if not archivo_txt:
        archivo_txt = ARCHIVO_TXT_DEFAULT
    
    # CSV de salida
    generar_csv = input("¿Generar archivo CSV? (s/N): ").strip().lower() in ['s', 'si', 'y', 'yes']
    csv_path = None
    if generar_csv:
        csv_path = input("Ruta CSV de salida (Enter para automático): ").strip()
        if not csv_path:
            csv_path = None
    
    # Carga SQL
    cargar_sql = input("¿Cargar a SQL Server con método OPTIMIZADO? (s/N): ").strip().lower() in ['s', 'si', 'y', 'yes']
    
    # Mostrar configuración
    print(f"\n📋 Configuración OPTIMIZADA:")
    print(f"   📁 Archivo TXT: {archivo_txt}")
    print(f"   📄 Generar CSV: {'Sí' if generar_csv else 'No'}")
    if generar_csv and csv_path:
        print(f"   📄 Ruta CSV: {csv_path}")
    print(f"   🗃️  Cargar SQL (OPTIMIZADO): {'Sí' if cargar_sql else 'No'}")
    
    confirmar = input(f"\n¿Proceder con esta configuración? (S/n): ").strip().lower()
    if confirmar not in ['n', 'no']:
        resultado = procesar_archivo_completo(
            archivo_txt=archivo_txt,
            generar_csv=generar_csv,
            cargar_sql=cargar_sql,
            csv_path=csv_path
        )
        
        if resultado is not None:
            print("\n✅ Proceso personalizado OPTIMIZADO completado exitosamente")
        else:
            print("\n❌ El proceso falló")
    else:
        print("❌ Operación cancelada")

# ======================
# FUNCIONES DE UTILIDAD ADICIONALES
# ======================

def validar_dependencias():
    """
    Valida que todas las dependencias estén instaladas
    """
    dependencias = {
        'pandas': 'pandas',
        'numpy': 'numpy', 
        'chardet': 'chardet',
        'pyodbc': 'pyodbc',
        'sqlalchemy': 'sqlalchemy'
    }
    
    faltantes = []
    for nombre, modulo in dependencias.items():
        try:
            __import__(modulo)
        except ImportError:
            faltantes.append(nombre)
    
    if faltantes:
        print("❌ Dependencias faltantes:")
        for dep in faltantes:
            print(f"   - {dep}")
        print("\nInstala con: pip install " + " ".join(faltantes))
        return False
    
    print("✅ Todas las dependencias están instaladas")
    return True

def mostrar_informacion_sistema():
    """
    Muestra información del sistema y configuración
    """
    print("\n" + "=" * 60)
    print("           INFORMACIÓN DEL SISTEMA OPTIMIZADO")
    print("=" * 60)
    print(f"📁 Archivo TXT por defecto: {ARCHIVO_TXT_DEFAULT}")
    print(f"🔗 Servidor SQL: {SQL_CONFIG['server']}")
    print(f"🗃️  Base de datos: {SQL_CONFIG['database']}")
    print(f"👤 Usuario SQL: {SQL_CONFIG['username']}")
    print(f"📋 Columnas esperadas: {len(COLUMN_TYPES_SQL)}")
    print(f"⚡ Optimizaciones: CSV temporal + fast_executemany + chunks 50k")
    print("=" * 60)

# ======================
# FUNCIONES PARA USO DIRECTO OPTIMIZADAS
# ======================

def limpiar_txt_a_csv(archivo_txt, archivo_csv=None):
    """
    Función directa para limpiar TXT y generar CSV
    
    Uso: df = limpiar_txt_a_csv("archivo.txt", "salida.csv")
    """
    return procesar_archivo_completo(
        archivo_txt=archivo_txt,
        generar_csv=True,
        cargar_sql=False,
        csv_path=archivo_csv
    )

def cargar_csv_a_sql_directo_optimizado(archivo_csv, tabla=None):
    """
    Función directa OPTIMIZADA para cargar CSV a SQL Server
    
    Uso: exito = cargar_csv_a_sql_directo_optimizado("archivo.csv", "mi_tabla")
    """
    if not os.path.exists(archivo_csv):
        print(f"❌ Archivo no encontrado: {archivo_csv}")
        return False
    
    df = pd.read_csv(archivo_csv, encoding='utf-8')
    df = limpiar_nombres_columnas_sql(df)
    df = convertir_tipos_datos_sql(df)
    df = limpiar_datos_sql(df)
    
    if tabla is None:
        tabla = generar_nombre_tabla(archivo_csv)
    
    return cargar_dataframe_a_sql_optimizado(df, tabla)

def proceso_completo_directo_optimizado(archivo_txt):
    """
    Función directa para proceso completo OPTIMIZADO
    
    Uso: df = proceso_completo_directo_optimizado("archivo.txt")
    """
    return procesar_archivo_completo(
        archivo_txt=archivo_txt,
        generar_csv=True,
        cargar_sql=True
    )

# ======================
# PUNTO DE ENTRADA
# ======================

if __name__ == "__main__":
    print("🔍 Validando dependencias...")
    if validar_dependencias():
        mostrar_informacion_sistema()
        main()
    else:
        print("❌ Por favor instala las dependencias faltantes antes de continuar")

# ======================
# EJEMPLOS DE USO DIRECTO OPTIMIZADO
# ======================

"""
EJEMPLOS DE USO OPTIMIZADO:

1. Proceso completo OPTIMIZADO (recomendado):
   df = proceso_completo_directo_optimizado("BD24082025.txt")

2. Solo limpieza a CSV:
   df = limpiar_txt_a_csv("BD24082025.txt", "salida.csv")

3. Solo carga de CSV existente OPTIMIZADA:
   exito = cargar_csv_a_sql_directo_optimizado("archivo.csv", "mi_tabla")

4. Configuración personalizada OPTIMIZADA:
   resultado = procesar_archivo_completo(
       archivo_txt="mi_archivo.txt",
       generar_csv=True,
       cargar_sql=True,
       csv_path="mi_salida.csv"
   )

OPTIMIZACIONES IMPLEMENTADAS:
✅ CSV temporal con separador ¬ (como tu código rápido)
✅ fast_executemany=True en SQLAlchemy
✅ Chunks de 50,000 registros optimizados
✅ Limpieza de caracteres problemáticos simplificada
✅ Manejo correcto de TRUNCATE con text()
✅ Eliminación automática de archivos temporales
✅ Detección y manejo de errores mejorado

DEPENDENCIAS REQUERIDAS:
pip install pandas numpy chardet pyodbc sqlalchemy

ESTRUCTURA DEL ARCHIVO TXT ESPERADA:
- Metadatos en filas 0-8
- Cabecera en fila 9 (índice 8)
- Datos desde fila 11 (índice 10)
- Separador detectado automáticamente

CAMBIOS PRINCIPALES DE OPTIMIZACIÓN:
1. CSV temporal eliminado automáticamente
2. Uso de text() para comandos SQL directos
3. Método de carga por chunks más eficiente
4. Limpieza de datos simplificada pero efectiva
5. fast_executemany habilitado por defecto
"""
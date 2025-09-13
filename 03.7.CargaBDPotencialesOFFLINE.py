import pandas as pd
import numpy as np
import re
import chardet
import pyodbc
from sqlalchemy import create_engine, text
import os
from datetime import datetime

# ======================
# CONFIGURACIÓN GLOBAL SIMPLIFICADA
# ======================

# Archivos por defecto
ARCHIVO_TXT_DEFAULT = r"D:\FNB\Reportes\04 Reporte Clientes Potenciales\Comparativo OFFLINE Setiembre 2025\ZSDR032_OFF_LI_20250901.txt"

# Conexión SQL Server
SQL_CONFIG = {
    "server": "192.168.64.250",
    "database": "BD_CALIDDA_FNB",
    "username": "ctorres",
    "password": "ibr2025"
}

# MAPEO COMPLETO DE COLUMNAS - Base tipo 1 y Base tipo 2
MAPEO_COLUMNAS_COMPLETO = {
    # Columna SQL: [Base tipo 1, Base tipo 2, variaciones]
    "Item": ["Item", "item"],
    "FechaEvaluacion": ["Fecha de evaluación", "Fecha Eval", "fecha de evaluación", "fecha eval"],
    "TipoDocumento": ["Tipo de documento", "Tipo Docum", "tipo de documento", "tipo docum"],
    "NumeroIdentFis1": ["Nº ident.fis.1", "N.I.F.1", "numero ident fis 1", "nif1"],
    "InterlocComercial": ["Interloc.comercial", "Int.cial.", "interloc comercial", "int cial"],
    "Nombre": ["Nombre", "nombre"],
    "SaldoCredito": ["Saldo Crédito", "Saldo Créd", "saldo credito", "saldo cred"],
    "CuentaContrato": ["Cuenta contrato", "Cta.Contr.", "cuenta contrato", "cta contr"],
    "Distrito": ["Distrito", "distrito"],
    "Direccion": ["Dirección", "direccion"],
    "CategoriaCuenta": ["Categoría de cuenta", "CaCta", "categoria de cuenta", "cacta"],
    "TextoCatCuenta": ["Texto categ.cuenta", "texto categ cuenta", "textocategcuenta"]
}

# Tipos de datos SQL
COLUMN_TYPES_SQL = {
    "Item": "BIGINT",
    "FechaEvaluacion": "DATETIME",
    "TipoDocumento": "VARCHAR(500)",
    "NumeroIdentFis1": "VARCHAR(500)",
    "InterlocComercial": "BIGINT",
    "Nombre": "VARCHAR(500)",
    "SaldoCredito": "DECIMAL(18,2)",
    "CuentaContrato": "BIGINT",
    "Distrito": "VARCHAR(500)",
    "Direccion": "VARCHAR(500)",
    "CategoriaCuenta": "VARCHAR(500)",
    "TextoCatCuenta": "VARCHAR(500)"
}

# ======================
# FUNCIONES BÁSICAS
# ======================

def detectar_codificacion(archivo):
    """Detecta la codificación del archivo"""
    codificaciones_comunes = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    
    for encoding in codificaciones_comunes:
        try:
            with open(archivo, 'r', encoding=encoding) as file:
                sample = file.read(5000)
                if len(sample) > 0:
                    print(f"Codificación detectada: {encoding}")
                    return encoding
        except (UnicodeDecodeError, UnicodeError):
            continue
    
    print("Usando latin-1 como fallback")
    return 'latin-1'

def detectar_separador(linea):
    """Detecta el separador del archivo"""
    separadores = ['\t', ';', ',', '|']
    conteos = {}
    
    for sep in separadores:
        conteos[sep] = linea.count(sep)
    
    separador_elegido = max(conteos, key=conteos.get)
    if conteos[separador_elegido] >= 5:
        return separador_elegido
    
    return '\t'  # Default

# ======================
# DETECCIÓN SIMPLIFICADA DE ESTRUCTURA
# ======================

def detectar_estructura_simple(archivo_entrada):
    """
    Detecta si la cabecera está en fila 1 o fila 9
    """
    print("=== DETECCIÓN DE ESTRUCTURA SIMPLIFICADA ===")
    
    codificacion = detectar_codificacion(archivo_entrada)
    
    with open(archivo_entrada, 'r', encoding=codificacion) as file:
        lineas = file.readlines()
    
    print(f"Total de líneas: {len(lineas):,}")
    
    def evaluar_cabecera(campos):
        """Evalúa si una línea es cabecera contando coincidencias exactas"""
        coincidencias = 0
        for col_sql, variantes in MAPEO_COLUMNAS_COMPLETO.items():
            for variante in variantes:
                for campo in campos:
                    if (variante.lower().strip() == campo.lower().strip() or
                        variante.lower().strip() in campo.lower().strip()):
                        coincidencias += 1
                        break
        return coincidencias
    
    # CASO 1: Verificar fila 1
    if len(lineas) > 0:
        linea_1 = lineas[0].strip()
        if linea_1:
            separador_1 = detectar_separador(linea_1)
            campos_1 = [campo.strip() for campo in linea_1.split(separador_1)]
            coincidencias_1 = evaluar_cabecera(campos_1)
            
            print(f"Fila 1: {len(campos_1)} campos, {coincidencias_1} coincidencias")
            print(f"Muestra: {' | '.join(campos_1[:6])}")
            
            # Si tiene al menos 6 coincidencias y 8+ columnas, es cabecera en fila 1
            if coincidencias_1 >= 6 and len(campos_1) >= 8:
                print("✓ CABECERA EN FILA 1")
                return "fila1", 0, separador_1, campos_1, lineas
    
    # CASO 2: Verificar fila 9
    if len(lineas) > 8:
        linea_9 = lineas[8].strip()  # índice 8 = fila 9
        if linea_9:
            separador_9 = detectar_separador(linea_9)
            campos_9 = [campo.strip() for campo in linea_9.split(separador_9)]
            coincidencias_9 = evaluar_cabecera(campos_9)
            
            print(f"Fila 9: {len(campos_9)} campos, {coincidencias_9} coincidencias")
            print(f"Muestra: {' | '.join(campos_9[:6])}")
            
            # Si tiene al menos 6 coincidencias y 8+ columnas, es cabecera en fila 9
            if coincidencias_9 >= 6 and len(campos_9) >= 8:
                print("✓ CABECERA EN FILA 9")
                return "fila9", 8, separador_9, campos_9, lineas
    
    # Si no se detecta ninguna, usar fila 1 por defecto
    print("⚠ No se detectó cabecera clara, usando fila 1")
    separador = detectar_separador(lineas[0])
    campos = lineas[0].strip().split(separador)
    return "fila1", 0, separador, campos, lineas

def mapear_columnas_simple(campos_cabecera):
    """
    Mapea las columnas del archivo a nombres SQL
    """
    print("=== MAPEO DE COLUMNAS ===")
    print(f"Campos en cabecera: {len(campos_cabecera)}")
    
    for i, campo in enumerate(campos_cabecera):
        print(f"  {i:2d}: '{campo}'")
    
    mapeo = {}
    columnas_faltantes = []
    
    # Mapeo específico mejorado para Base tipo 2
    mapeos_especificos = {
        "Item": ["item"],
        "FechaEvaluacion": ["fecha eval", "fecha de evaluación"], 
        "TipoDocumento": ["tipo docum", "tipo de documento"],
        "NumeroIdentFis1": ["n.i.f.1", "nº ident.fis.1", "nif"],
        "InterlocComercial": ["int.cial.", "interloc.comercial", "int cial"],
        "Nombre": ["nombre"],
        "SaldoCredito": ["saldo créd", "saldo crédito", "saldo cred"],
        "CuentaContrato": ["cta.contr.", "cuenta contrato", "cta contr"],
        "Distrito": ["distrito"],
        "Direccion": ["dirección", "direccion"],
        "CategoriaCuenta": ["cacta", "categoría de cuenta", "categoria de cuenta"],
        "TextoCatCuenta": ["texto categ.cuenta", "texto categ cuenta"]
    }
    
    for col_sql, variantes in mapeos_especificos.items():
        encontrada = False
        
        for i, campo_archivo in enumerate(campos_cabecera):
            campo_limpio = campo_archivo.lower().strip()
            
            # Saltar campos vacíos
            if not campo_limpio:
                continue
            
            for variante in variantes:
                variante_limpia = variante.lower().strip()
                
                # Coincidencias exactas o parciales
                if (variante_limpia == campo_limpio or
                    variante_limpia in campo_limpio or
                    campo_limpio in variante_limpia or
                    # Coincidencias específicas para nombres complicados
                    (col_sql == "NumeroIdentFis1" and "nif" in campo_limpio) or
                    (col_sql == "InterlocComercial" and "int" in campo_limpio and "cial" in campo_limpio) or
                    (col_sql == "SaldoCredito" and "saldo" in campo_limpio and "cré" in campo_limpio) or
                    (col_sql == "CuentaContrato" and "cta" in campo_limpio and "contr" in campo_limpio) or
                    (col_sql == "CategoriaCuenta" and "cacta" in campo_limpio) or
                    (col_sql == "TextoCatCuenta" and "texto" in campo_limpio and "categ" in campo_limpio)):
                    
                    mapeo[col_sql] = i
                    print(f"✓ {col_sql} <- '{campo_archivo}' (posición {i})")
                    encontrada = True
                    break
            
            if encontrada:
                break
        
        if not encontrada:
            columnas_faltantes.append(col_sql)
    
    if columnas_faltantes:
        print(f"⚠ Columnas no encontradas: {columnas_faltantes}")
        
        # Si hay muchas columnas faltantes, intentar mapeo por posición como fallback
        if len(columnas_faltantes) > 6:
            print("⚠ Muchas columnas faltantes, intentando mapeo por posición...")
            
            # Mapeo por posición para archivos Base tipo 2 estándar
            mapeo_posicion_tipo2 = {
                "Item": 0,
                "FechaEvaluacion": 1, 
                "TipoDocumento": 3,  # Saltando posición 2 que está vacía
                "NumeroIdentFis1": 4,
                "InterlocComercial": 5,
                "Nombre": 6,
                "SaldoCredito": 7,
                "CuentaContrato": 8,
                "Distrito": 9,
                "Direccion": 10,
                "CategoriaCuenta": 12,  # Saltando posición 11 que está vacía
                "TextoCatCuenta": 13
            }
            
            print("Usando mapeo por posición estándar para Base tipo 2:")
            mapeo_final = {}
            columnas_faltantes_final = []
            
            for col_sql, pos in mapeo_posicion_tipo2.items():
                if pos < len(campos_cabecera) and campos_cabecera[pos].strip():
                    mapeo_final[col_sql] = pos
                    print(f"✓ {col_sql} <- '{campos_cabecera[pos]}' (pos {pos})")
                else:
                    columnas_faltantes_final.append(col_sql)
                    print(f"⚠ {col_sql} <- No disponible en posición {pos}")
            
            return mapeo_final, columnas_faltantes_final
    
    print(f"Mapeo completado: {len(mapeo)} columnas encontradas")
    return mapeo, columnas_faltantes

def encontrar_inicio_datos_simple(lineas, indice_cabecera):
    """
    Encuentra donde empiezan los datos, saltando líneas vacías
    """
    inicio = indice_cabecera + 1
    
    # Saltar líneas vacías
    while inicio < len(lineas) and lineas[inicio].strip() == '':
        print(f"Saltando línea vacía en posición {inicio + 1}")
        inicio += 1
    
    if inicio >= len(lineas):
        print("Error: No hay datos después de la cabecera")
        return len(lineas)
    
    print(f"Datos inician en línea: {inicio + 1}")
    return inicio

# ======================
# PROCESAMIENTO SIMPLIFICADO
# ======================

def procesar_archivo_simple(archivo_entrada):
    """
    Procesa el archivo con la lógica simplificada
    """
    print("=== PROCESAMIENTO SIMPLIFICADO ===")
    
    # 1. Detectar estructura
    tipo, indice_cabecera, separador, campos_cabecera, lineas = detectar_estructura_simple(archivo_entrada)
    
    # 2. Mapear columnas
    mapeo, faltantes = mapear_columnas_simple(campos_cabecera)
    
    # 3. Encontrar inicio de datos
    inicio_datos = encontrar_inicio_datos_simple(lineas, indice_cabecera)
    
    if inicio_datos >= len(lineas):
        return None
    
    # 4. Procesar datos
    datos_procesados = []
    filas_omitidas = 0
    
    print(f"Procesando desde línea {inicio_datos + 1}...")
    
    for i in range(inicio_datos, len(lineas)):
        linea = lineas[i].strip()
        if not linea:  # Saltar líneas vacías
            continue
        
        campos = linea.split(separador)
        
        # Validar que no sea una cabecera repetida
        palabras_cabecera = ['item', 'fecha', 'tipo', 'nombre', 'saldo']
        if any(palabra in campo.lower() for palabra in palabras_cabecera for campo in campos[:5]):
            filas_omitidas += 1
            continue
        
        # Crear fila con todas las columnas
        fila = {}
        
        # Mapear columnas encontradas
        for col_sql, pos in mapeo.items():
            if pos < len(campos):
                valor = campos[pos].strip()
                fila[col_sql] = None if valor.upper() in ['NULL', '', 'NONE'] else valor
            else:
                fila[col_sql] = None
        
        # Agregar columnas faltantes
        for col_sql in faltantes:
            fila[col_sql] = None
        
        # Ordenar columnas según esquema SQL
        fila_ordenada = []
        for col_sql in COLUMN_TYPES_SQL.keys():
            fila_ordenada.append(fila.get(col_sql, None))
        
        datos_procesados.append(fila_ordenada)
    
    print(f"Filas procesadas: {len(datos_procesados):,}")
    print(f"Filas omitidas: {filas_omitidas}")
    
    if len(datos_procesados) == 0:
        print("Error: No se procesaron datos válidos")
        return None
    
    # 5. Crear DataFrame
    columnas = list(COLUMN_TYPES_SQL.keys())
    df = pd.DataFrame(datos_procesados, columns=columnas)
    
    print(f"DataFrame creado: {df.shape}")
    
    # 6. Mostrar muestra
    print("\n=== MUESTRA DE DATOS ===")
    for col in df.columns:
        valores_no_nulos = df[col].dropna()
        if len(valores_no_nulos) > 0:
            print(f"{col}: {len(valores_no_nulos)} valores no nulos, ejemplo: {valores_no_nulos.iloc[0]}")
        else:
            print(f"{col}: Sin datos")
    
    return df

def detectar_filas_corridas(df, archivo_txt, campos_cabecera):
    """
    Detecta filas que no tienen la misma cantidad de columnas que la cabecera
    y las guarda en un CSV de errores.
    """
    print("\n=== DETECCIÓN DE FILAS CORRIDAS ===")

    # Número de columnas esperado
    expected_cols = len(campos_cabecera)

    # Detectar filas con NaN en todas las columnas numéricas clave
    claves = ["Item", "CuentaContrato", "InterlocComercial"]
    bad_rows = df[df[claves].isnull().any(axis=1)]

    if len(bad_rows) > 0:
        print(f"⚠ Se detectaron {len(bad_rows):,} filas potencialmente corridas o incompletas")
        
        # Guardar en CSV de errores
        error_csv = archivo_txt.replace(".txt", "_errores_corridos.csv")
        bad_rows.to_csv(error_csv, index=False, encoding="utf-8-sig")
        print(f"✓ Filas corridas guardadas en: {error_csv}")

        # Eliminar esas filas del dataframe principal
        df = df.drop(bad_rows.index).reset_index(drop=True)
        print(f"✓ DataFrame limpio: {len(df):,} filas restantes")

    else:
        print("✓ No se detectaron filas corridas")

    return df


def limpiar_datos_simple(df):
    """
    Limpieza reforzada de datos para asegurar compatibilidad con SQL Server
    """
    print("\n=== LIMPIEZA DE DATOS ===")
    
    df_clean = df.copy()

    # 1. Cambio específico de distrito
    if 'Distrito' in df_clean.columns:
        cambios = df_clean['Distrito'].str.contains('LA ALBORADA', na=False).sum()
        df_clean['Distrito'] = df_clean['Distrito'].str.replace('LA ALBORADA', 'COMAS', regex=False)
        if cambios > 0:
            print(f"✓ Cambiados {cambios} distritos de LA ALBORADA a COMAS")
    
    # 2. Limpieza básica de strings
    for col in df_clean.columns:
        if df_clean[col].dtype == 'object' or str(df_clean[col].dtype).startswith("string"):
            df_clean[col] = df_clean[col].astype(str).str.strip()
            df_clean[col] = df_clean[col].str.replace('¬', '-', regex=False)
            df_clean[col] = df_clean[col].str.replace('\n', ' ', regex=False)
            df_clean[col] = df_clean[col].str.replace('\r', ' ', regex=False)
            df_clean[col] = df_clean[col].replace(['nan', 'NaN', 'None', 'NULL', ''], None)

    # 3. Conversión estricta según esquema SQL
    for col, sql_type in COLUMN_TYPES_SQL.items():
        if col not in df_clean.columns:
            df_clean[col] = None
            continue

        try:
            valores_antes = df_clean[col].dropna().nunique()
            print(f"Procesando {col}: {valores_antes} valores únicos")

            if sql_type.startswith("VARCHAR"):
                df_clean[col] = df_clean[col].astype(str).replace("None", None)

            elif sql_type in ("BIGINT", "INT"):
                # Mantener el número completo, eliminando solo caracteres no numéricos
                df_clean[col] = df_clean[col].astype(str).str.replace(r'[^\d-]', '', regex=True)
                df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce").astype("Int64")

            elif "DECIMAL" in sql_type:
                # Remover separadores de miles y mantener decimales
                df_clean[col] = df_clean[col].astype(str).str.replace(',', '', regex=False)
                df_clean[col] = df_clean[col].str.extract(r'(-?\d+\.?\d*)')[0]
                df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce")

            elif sql_type == "DATETIME":
                df_clean[col] = pd.to_datetime(df_clean[col], errors="coerce")

            print(f"✓ {col}: convertido a {sql_type}")

        except Exception as e:
            print(f"⚠ Error limpiando {col}: {e}")
            df_clean[col] = None

    # 4. Verificación extra
    for col, sql_type in COLUMN_TYPES_SQL.items():
        if sql_type in ("BIGINT", "INT", "DECIMAL(18,2)", "DECIMAL"):
            invalids = df_clean[col].isna().sum()
            print(f"  {col}: {invalids:,} valores nulos tras limpieza")
    
    # 5. Verificación SaldoCredito
    if 'SaldoCredito' in df_clean.columns:
        valores_validos = df_clean['SaldoCredito'].dropna()
        nulos = df_clean['SaldoCredito'].isnull().sum()
        print(f"Verificación SaldoCredito: {len(valores_validos)} válidos, {nulos} nulos")
        if len(valores_validos) > 0:
            print(f"  Ejemplo valores: {valores_validos.iloc[:3].tolist()}")

    return df_clean



# ======================
# CARGA A SQL
# ======================

def generar_nombre_tabla(archivo_path):
    """Genera nombre de tabla"""
    nombre_archivo = os.path.basename(archivo_path)
    fecha_match = re.search(r'(\d{8})', nombre_archivo)
    if fecha_match:
        fecha = fecha_match.group(1)
        return f"BD_Potenciales_OFFLINE_{fecha}"
    
    timestamp = datetime.now().strftime("%d%m%Y")
    return f"BD_Potenciales_OFFLINE_{timestamp}"

def cargar_sql_simple(df, table_name):
    """Carga optimizada a SQL Server"""
    print(f"\n=== CARGA A SQL SERVER ===")
    print(f"Tabla: {table_name}")
    print(f"Registros: {len(df):,}")
    
    try:
        # Configurar engine con fast_executemany
        engine_str = (
            f"mssql+pyodbc://{SQL_CONFIG['username']}:{SQL_CONFIG['password']}"
            f"@{SQL_CONFIG['server']}/{SQL_CONFIG['database']}"
            f"?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
        )
        engine = create_engine(
            engine_str, 
            fast_executemany=False,
            connect_args={"autocommit": True}
        )

        # Verificar si la tabla existe con SQLAlchemy
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = :tbl AND TABLE_TYPE = 'BASE TABLE'
            """), {"tbl": table_name})
            tabla_existe = result.scalar() > 0

            if tabla_existe:
                registros = conn.execute(text(f"SELECT COUNT(*) FROM [{table_name}]")).scalar()
                print(f"Tabla existe: {registros:,} registros")
                respuesta = input("¿Reemplazar tabla? (s/N): ").strip().lower()
                accion = 'replace' if respuesta in ['s', 'si', 'y', 'yes'] else 'append'
            else:
                accion = 'fail'

        # Crear CSV temporal
        temp_csv = f"temp_simple_{datetime.now().strftime('%H%M%S')}.csv"
        df.to_csv(temp_csv, index=False, sep="¬", encoding="utf-8")

        # Manejar tabla según acción
        if accion == 'replace':
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS [{table_name}]"))
            if_exists_mode = 'fail'
        else:
            if_exists_mode = 'append' if accion == 'append' else 'fail'

        # Cargar en chunks
        chunksize = 50000
        chunks_loaded = 0
        for chunk in pd.read_csv(temp_csv, sep="¬", encoding="utf-8", chunksize=chunksize, engine="python"):
            chunk.to_sql(table_name, engine, if_exists=if_exists_mode, index=False, method=None)
            chunks_loaded += 1
            registros_cargados = min(chunks_loaded * chunksize, len(df))
            print(f"  Chunk {chunks_loaded}: {registros_cargados:,} registros")
            if_exists_mode = 'append'

        # Verificar registros finales
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM [{table_name}]"))
            count_final = result.scalar()

        os.remove(temp_csv)
        engine.dispose()

        print(f"✓ Carga completada: {count_final:,} registros")
        return True

    except Exception as e:
        print(f"Error en carga: {e}")
        return False


# ======================
# PROCESO PRINCIPAL
# ======================

def proceso_etl_simple(archivo_txt, generar_csv=True, cargar_sql=True):
    """
    Proceso ETL simplificado para 2 estructuras únicamente
    """
    print("=" * 70)
    print("PROCESO ETL SIMPLIFICADO - SOLO 2 ESTRUCTURAS")
    print("Cabecera en fila 1 o fila 9")
    print("=" * 70)
    print(f"Archivo: {archivo_txt}")
    
    if not os.path.exists(archivo_txt):
        print(f"Error: Archivo no existe")
        return None
    
    try:
        # 1. Procesar archivo
        df = procesar_archivo_simple(archivo_txt)
        if df is None:
            return None
        
        df = detectar_filas_corridas(df, archivo_txt, list(COLUMN_TYPES_SQL.keys()))

        # 2. Limpiar datos
        df_clean = limpiar_datos_simple(df)
        
        # 3. Generar CSV
        if generar_csv:
            csv_path = archivo_txt.replace('.txt', '_simple_clean.csv')
            df_clean.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print(f"✓ CSV generado: {csv_path}")
        
        # 4. Cargar SQL
        if cargar_sql:
            table_name = generar_nombre_tabla(archivo_txt)
            exito = cargar_sql_simple(df_clean, table_name)
            if not exito:
                print("Error en carga SQL")
        
        # 5. Resumen
        print("\n" + "=" * 70)
        print("RESUMEN FINAL")
        print("=" * 70)
        print(f"Registros: {len(df_clean):,}")
        print(f"Columnas: {len(df_clean.columns)}")
        
        for col in df_clean.columns:
            nulos = df_clean[col].isnull().sum()
            pct = (nulos / len(df_clean)) * 100
            print(f"  {col}: {nulos:,} nulos ({pct:.1f}%)")
        
        return df_clean
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None

# ======================
# MENÚ PRINCIPAL
# ======================

def main_simple():
    """Menú principal simplificado"""
    print("=" * 70)
    print("ETL SIMPLIFICADO - SOLO 2 ESTRUCTURAS")
    print("Cabecera en fila 1 o fila 9 (con posible línea vacía)")
    print("=" * 70)
    print("1. Proceso completo (TXT → CSV → SQL)")
    print("2. Solo extracción y CSV")
    print("3. Salir")
    
    while True:
        try:
            opcion = input("\nSelecciona opción (1-3): ").strip()
            
            if opcion == '1':
                archivo = input(f"Archivo TXT (Enter=default): ").strip()
                if not archivo:
                    archivo = ARCHIVO_TXT_DEFAULT
                
                resultado = proceso_etl_simple(archivo, generar_csv=True, cargar_sql=True)
                if resultado is not None:
                    print("✓ Proceso completo terminado")
                break
                
            elif opcion == '2':
                archivo = input(f"Archivo TXT (Enter=default): ").strip()
                if not archivo:
                    archivo = ARCHIVO_TXT_DEFAULT
                
                resultado = proceso_etl_simple(archivo, generar_csv=True, cargar_sql=False)
                if resultado is not None:
                    print("✓ Extracción terminada")
                break
                
            elif opcion == '3':
                print("Saliendo...")
                break
                
            else:
                print("Opción inválida")
                
        except KeyboardInterrupt:
            print("\nSaliendo...")
            break

if __name__ == "__main__":
    main_simple()

# ======================
# FUNCIONES DIRECTAS
# ======================

def extraer_datos(archivo_txt):
    """Función directa para extraer datos"""
    return proceso_etl_simple(archivo_txt, generar_csv=False, cargar_sql=False)

def cargar_datos_sql(archivo_txt):
    """Función directa para cargar a SQL"""
    return proceso_etl_simple(archivo_txt, generar_csv=False, cargar_sql=True)
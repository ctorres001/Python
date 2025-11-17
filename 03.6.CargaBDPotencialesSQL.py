import pandas as pd
import numpy as np
import re
import chardet
import pyodbc
from sqlalchemy import create_engine, text
from sqlalchemy import types as satypes
import os
from datetime import datetime
from sqlalchemy import create_engine, text
import warnings
import unicodedata

# Suprimir warnings específicos
warnings.filterwarnings('ignore', category=UserWarning, message='.*dayfirst.*')

# ======================
# CONFIGURACIÓN GLOBAL
# ======================

# Archivos por defecto
ARCHIVO_TXT_DEFAULT = r"D:\FNB\Reportes\04 Reporte Clientes Potenciales\2025\11. Noviembre\BD16112025\BD16112025.txt"

# Conexión SQL Server
SQL_CONFIG = {
    "server": "192.168.64.250",
    "database": "BD_CALIDDA_FNB",
    "username": "ctorres",
    "password": "ibr2025"
}

# Mapeo de tipos de datos SQL Server (ACTUALIZADO)
COLUMN_TYPES_SQL = {
    "Fecha_Eval": "DATETIME",
    "Tipo_Docum": "VARCHAR(500)",
    "N_I_F_1": "VARCHAR(20)",  # CORREGIDO: Ahora es texto para soportar letras
    "Int_cial": "BIGINT",
    "Nombre": "VARCHAR(500)",
    "Saldo_Cred": "DECIMAL(18,2)",  # CORREGIDO: 2 decimales
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

# Mapeo de nombres originales a nombres SQL-compatibles (ACTUALIZADO)
MAPEO_NOMBRES_COLUMNAS = {
    "Fecha Eval": "Fecha_Eval",
    "Tipo Docum": "Tipo_Docum", 
    "N.I.F.1": "N_I_F_1",
    "Int.cial.": "Int_cial",
    "Soc.cial.": "Int_cial",
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
    "Dirección3": "Direccion_3",
    "NSE 3": "NSE_3",
    "FechaAlta3": "FechaAlta3",
    "Cta.Ctto 4": "Cta_Ctto_4",
    "Distrito 4": "Distrito_4",
    "Dirección 4": "Direccion_4",
    "Dirección4": "Direccion_4",
    "NSE 4": "NSE_4",
    "FechaAlta4": "FechaAlta4",
    "Cta.Ctto 5": "Cta_Ctto_5",
    "Distrito 5": "Distrito_5",
    "Dirección 5": "Direccion_5",
    "Dirección5": "Direccion_5",
    "NSE 5": "NSE_5",
    "FechaAlta5": "FechaAlta5",
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
        if linea_cabecera.count(sep) > 10:
            return sep
    return '\t'

def _normalizar_texto(s: str) -> str:
    """Normaliza texto: minúsculas, sin acentos, sin puntuación extra, colapsa espacios."""
    if s is None:
        return ''
    s = str(s).strip()
    # Quitar acentos
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    # Reemplazar separadores/puntuación por espacio
    s = re.sub(r"[\.:;\\/\\-]+", " ", s)
    # Colapsar espacios
    s = re.sub(r"\s+", " ", s).strip()
    return s

def detectar_fila_cabecera(lineas, max_busqueda=50):
    """Intenta detectar dinámicamente la fila de cabecera escaneando las primeras líneas.
    Retorna: (idx_cabecera, separador, columnas_cabecera) o (None, None, None)
    """
    # Conjunto de nombres esperados normalizados
    nombres_esperados_norm = set(_normalizar_texto(k) for k in MAPEO_NOMBRES_COLUMNAS.keys())
    mejor = {
        'idx': None,
        'sep': None,
        'cols': None,
        'score': -1,
        'total_cols': 0
    }

    limite = min(len(lineas), max_busqueda)
    for i in range(limite):
        l = lineas[i].strip('\n')
        if not l or len(l.strip()) == 0:
            continue
        # Probar separadores comunes y elegir el que más columnas aporte
        candidatos = ['\t', ';', ',', '|']
        mejor_local = None
        for sep in candidatos:
            partes = l.split(sep)
            if len(partes) < 5:
                continue
            score = 0
            for p in partes:
                pn = _normalizar_texto(p)
                if pn in nombres_esperados_norm:
                    score += 1
            if mejor_local is None or score > mejor_local['score'] or (score == mejor_local['score'] and len(partes) > mejor_local['cols']):
                mejor_local = {'sep': sep, 'cols': len(partes), 'score': score, 'partes': partes}

        if mejor_local and (mejor_local['score'] > mejor['score'] or (mejor_local['score'] == mejor['score'] and mejor_local['cols'] > mejor['total_cols'])):
            mejor.update({'idx': i, 'sep': mejor_local['sep'], 'cols': mejor_local['partes'], 'score': mejor_local['score'], 'total_cols': mejor_local['cols']})

        # Criterio de corte: si encontramos una cabecera con match alto
        if mejor['score'] >= 12 and mejor['total_cols'] >= 25:
            break

    if mejor['idx'] is not None:
        return mejor['idx'], mejor['sep'], mejor['cols']
    return None, None, None

def analizar_archivo_txt(archivo_entrada):
    """Analiza la estructura del archivo antes de la limpieza"""
    print("=== ANÁLISIS DEL ARCHIVO TXT ===")
    
    codificacion = detectar_codificacion(archivo_entrada)
    
    with open(archivo_entrada, 'r', encoding=codificacion) as file:
        lineas = file.readlines()
    
    print(f"Total de líneas: {len(lineas)}")
    # Detectar dinámicamente la cabecera
    idx_cab, sep, cabecera_partes = detectar_fila_cabecera(lineas)
    if idx_cab is not None:
        print(f"Cabecera detectada en fila: {idx_cab + 1}")
        print(f"Separador detectado: {'TAB' if sep == '\\t' else sep}")
        print(f"Cabecera detectada: {len(cabecera_partes)} columnas")

        problemas = []
        # Revisar estructura en una muestra y luego general (para no ser tan costoso)
        inicio_datos = idx_cab + 1
        for i, linea in enumerate(lineas[inicio_datos:], start=inicio_datos + 1):
            if linea.strip() == '':
                continue
            campos = linea.strip().split(sep)
            if len(campos) != len(cabecera_partes):
                problemas.append((i, len(campos)))
        if problemas:
            print(f"Filas con problemas de estructura: {len(problemas)}")
        else:
            print("✅ No se detectaron problemas de estructura")
    else:
        print("⚠️  No se pudo detectar dinámicamente la cabecera. Se usará la heurística anterior (fila 9).")
    
    print("=" * 50)

def limpiar_archivo_txt(archivo_entrada):
    """Limpia un archivo TXT con problemas de estructura y retorna DataFrame"""
    print("=== INICIANDO LIMPIEZA DE ARCHIVO TXT ===")
    
    codificacion = detectar_codificacion(archivo_entrada)
    
    with open(archivo_entrada, 'r', encoding=codificacion) as file:
        lineas = file.readlines()
    
    print(f"Total de líneas leídas: {len(lineas)}")
    
    if len(lineas) < 2:
        print("❌ Error: El archivo no tiene suficientes líneas para procesar")
        return None

    # Detectar cabecera dinámicamente
    idx_cab, separador, cabecera_raw = detectar_fila_cabecera(lineas)
    if idx_cab is None:
        # Fallback a la lógica anterior (fila 9, índice 8)
        print("⚠️  No se detectó cabecera dinámicamente; se usará fila 9 como cabecera.")
        base_idx = 8 if len(lineas) > 8 else 0
        separador = detectar_separador(lineas[base_idx])
        cabecera_raw = lineas[base_idx].strip().split(separador)
        idx_cab = base_idx
    else:
        print(f"✅ Cabecera localizada en fila {idx_cab + 1} con separador {'TAB' if separador == '\\t' else separador}")
    
    # Limpiar cabecera
    cabecera = []
    for i, col in enumerate(cabecera_raw):
        if col.strip() == '':
            cabecera.append(f'Columna_{i+1}')
        else:
            cabecera.append(col.strip())
    
    num_columnas_esperadas = len(cabecera)
    print(f"Cabecera extraída: {num_columnas_esperadas} columnas")

    # Identificar y remover columnas en blanco de la cabecera (placeholders)
    idx_cabeceras_vacias = [i for i, col in enumerate(cabecera) if col.startswith('Columna_') or col.strip() == '']
    if idx_cabeceras_vacias:
        print(f"🧹 Columnas vacías en cabecera detectadas: {len(idx_cabeceras_vacias)}. Serán removidas del esquema.")
    cabecera_filtrada = [col for i, col in enumerate(cabecera) if i not in idx_cabeceras_vacias]
    num_cols_esperadas_final = len(cabecera_filtrada)
    
    
    # Procesar datos a partir de la línea siguiente a la cabecera
    datos_raw = lineas[idx_cab + 1:]
    datos_limpios = []
    filas_corregidas = 0
    filas_omitidas = 0
    
    for i, linea in enumerate(datos_raw, start=idx_cab + 2):
        if linea.strip() == '':
            continue
            
        campos = linea.strip().split(separador)

        # Tolerancia: caso de 1 columna extra conocida por dirección dividida
        if len(campos) == num_columnas_esperadas + 1:
            cta_contr_index = None
            direccion_index = None
            for j, col in enumerate(cabecera):
                if 'Cta.Contr' in col:
                    cta_contr_index = j
                elif 'Dirección' in col and direccion_index is None:
                    direccion_index = j
            if cta_contr_index is not None and cta_contr_index < len(campos):
                cuenta = campos[cta_contr_index]
                if cuenta in ['5199463', '5320440'] and direccion_index is not None and direccion_index < len(campos) - 1:
                    direccion_completa = str(campos[direccion_index]) + ' ' + str(campos[direccion_index + 1])
                    campos = campos[:direccion_index] + [direccion_completa] + campos[direccion_index + 2:]

        # Alinear longitud mínima al tamaño de cabecera original (padding)
        if len(campos) < num_columnas_esperadas:
            campos = campos + [''] * (num_columnas_esperadas - len(campos))

        # Filtrar columnas vacías de la cabecera (eliminar posiciones)
        campos_filtrados = [campos[k] for k in range(min(len(campos), num_columnas_esperadas)) if k not in idx_cabeceras_vacias]

        # Tolerancia: si sobran columnas por separadores extra vacíos, recorta si son vacías
        if len(campos_filtrados) > num_cols_esperadas_final:
            extras = campos_filtrados[num_cols_esperadas_final:]
            if all(str(x).strip() == '' for x in extras):
                campos_filtrados = campos_filtrados[:num_cols_esperadas_final]
            else:
                # Forzar corte estricto si traen datos
                campos_filtrados = campos_filtrados[:num_cols_esperadas_final]

        # Padding si falta alguna
        if len(campos_filtrados) < num_cols_esperadas_final:
            campos_filtrados += [''] * (num_cols_esperadas_final - len(campos_filtrados))

        if len(campos_filtrados) == num_cols_esperadas_final:
            datos_limpios.append(campos_filtrados)
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
    
    # Crear DataFrame con cabecera filtrada (sin columnas en blanco)
    df = pd.DataFrame(datos_limpios, columns=cabecera_filtrada)
    
    # Eliminar columnas vacías
    columnas_a_eliminar = [col for col in df.columns if 'Columna' in col]
    if columnas_a_eliminar:
        df = df.drop(columns=columnas_a_eliminar)
    
    # FILTRAR SOLO COLUMNAS MAPEADAS (AGRUPANDO SINÓNIMOS POR COLUMNA DESTINO)
    grupos_por_destino = {}
    for original, destino in MAPEO_NOMBRES_COLUMNAS.items():
        grupos_por_destino.setdefault(destino, []).append(original)

    seleccionadas = []
    faltantes = []
    for destino, originales in grupos_por_destino.items():
        # elige la primera cabecera que exista en el archivo para este destino
        encontrada = next((col for col in df.columns if col in originales), None)
        if encontrada is not None:
            seleccionadas.append(encontrada)
        else:
            # reporta un nombre canónico (el primero del grupo) como faltante
            faltantes.append(originales[0])

    if seleccionadas:
        df = df[seleccionadas]
        print(f"✅ Columnas seleccionadas (mapeadas): {', '.join(seleccionadas)}")
    else:
        print("❌ No se encontraron coincidencias con el mapeo de columnas. Se conservarán todas las columnas originales para evitar pérdida total de estructura.")
        print("   Revisa si la línea usada como cabecera es realmente la correcta o si hay desplazamiento.")
        print("   También valida si los nombres del archivo tienen pequeñas variaciones (acentos, espacios dobles, caracteres ocultos).")

    # Reporte de faltantes reales por grupo (solo si ninguna variante apareció)
    if faltantes:
        print(f"⚠️  Columnas esperadas pero no encontradas: {', '.join(faltantes)}")
    
    # Reemplazar "LA ALBORADA" por "COMAS" solo si el valor es exactamente igual (no substring)
    columnas_distrito = [col for col in df.columns if 'Distrito' in col]
    total_cambios = 0
    for col in columnas_distrito:
        if col in df.columns:
            mask_eq = df[col].astype(str).str.strip().str.upper() == 'LA ALBORADA'
            cambios = int(mask_eq.sum())
            if cambios > 0:
                df.loc[mask_eq, col] = 'COMAS'
                total_cambios += cambios
    
    if total_cambios > 0:
        print(f"Distritos actualizados: {total_cambios} registros 'LA ALBORADA' → 'COMAS'")
    
    # Limpieza general
    df = aplicar_limpieza_general(df)
    
    return df

def aplicar_limpieza_general(df):
    """Aplica limpieza general a los datos del DataFrame"""
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).str.replace('\n', ' ', regex=False)
            df[col] = df[col].str.replace('\r', ' ', regex=False)
            df[col] = df[col].str.replace('\t', ' ', regex=False)
            df[col] = df[col].str.strip()
            mask_nulls = df[col].isin(['nan', 'NaN', 'NULL', ''])
            if mask_nulls.any():
                df.loc[mask_nulls, col] = np.nan
    
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
                    df[col] = _parse_datetime_series(df[col])
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

def _parse_datetime_series(series: pd.Series) -> pd.Series:
    """Parsea fechas intentando formatos comunes para evitar UserWarning de inferencia.
    Intenta dd/mm/yyyy, dd-mm-yyyy, dd/mm/yy, dd-mm-yy con dayfirst. Si falla, cae a to_datetime genérico silencioso.
    """
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
        if sample.apply(lambda x: bool(re.match(pat, str(x)))).mean() > 0.6:
            fmt = f
            break
    if fmt:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=UserWarning)
            return pd.to_datetime(s, format=fmt, errors='coerce')
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', category=UserWarning)
        return pd.to_datetime(s, errors='coerce', dayfirst=True)

# ======================
# FUNCIONES OPTIMIZADAS PARA SQL SERVER
# ======================

def generar_nombre_tabla(archivo_path):
    """Genera nombre de tabla basado en el archivo"""
    nombre_archivo = os.path.basename(archivo_path)
    if "BD" in nombre_archivo and any(c.isdigit() for c in nombre_archivo):
        fecha_match = re.search(r'(\d{8})', nombre_archivo)
        if fecha_match:
            fecha = fecha_match.group(1)
            # Convertir de DDMMYYYY a YYYYMMDD
            dia = fecha[0:2]
            mes = fecha[2:4]
            anio = fecha[4:8]
            fecha_sql = f"{anio}{mes}{dia}"
            return f"BD_Potenciales_{fecha_sql}"
    
    timestamp = datetime.now().strftime("%Y%m%d")
    return f"BD_Potenciales_{timestamp}"

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
            # Esto maneja casos como "Dirección4" vs "Dirección 4"
            col_normalizado = ' '.join(col.split())  # Normaliza espacios múltiples
            
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
        # Mostrar cambios importantes
        for orig, nuevo in cambios[:5]:
            print(f"  {orig} → {nuevo}")
        if len(cambios) > 5:
            print(f"  ... y {len(cambios) - 5} más")
    
    df.columns = nuevos_nombres
    return df

def convertir_tipos_datos_sql(df):
    """Convierte tipos de datos según mapeo SQL"""
    print("\n=== Conversión de tipos para SQL Server ===")
    
    for col in df.columns:
        # IGNORAR COLUMNAS NO MAPEADAS (como "Item")
        if col not in COLUMN_TYPES_SQL:
            continue
            
        sql_type = COLUMN_TYPES_SQL[col]
        
        try:
            if sql_type.startswith("VARCHAR"):
                df[col] = df[col].astype(str)
                df[col] = df[col].replace(['nan', 'NaN', 'None', 'null'], pd.NA)
                
            elif sql_type in ("INT", "BIGINT"):
                # CORREGIDO: Manejar valores decimales en columnas INT/BIGINT
                df[col] = pd.to_numeric(df[col], errors="coerce")
                # Redondear valores decimales antes de convertir a Int64
                df[col] = df[col].round(0).astype("Int64")
                
            elif "DECIMAL" in sql_type:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                
            elif sql_type == "DATETIME":
                df[col] = _parse_datetime_series(df[col])
            
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
            mask_nulls = df[col].isin(['nan', 'NaN', 'None'])
            if mask_nulls.any():
                df.loc[mask_nulls, col] = None
    
    return df

def verificar_espacio_sql():
    """Verifica si hay espacio disponible en la base de datos"""
    try:
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_CONFIG['server']};DATABASE={SQL_CONFIG['database']};UID={SQL_CONFIG['username']};PWD={SQL_CONFIG['password']};TrustServerCertificate=yes;"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Consultar espacio disponible
        cursor.execute("""
            SELECT 
                DB_NAME() as DatabaseName,
                SUM(size) * 8 / 1024 AS SizeMB,
                SUM(CAST(FILEPROPERTY(name, 'SpaceUsed') AS INT)) * 8 / 1024 AS UsedMB,
                (SUM(size) - SUM(CAST(FILEPROPERTY(name, 'SpaceUsed') AS INT))) * 8 / 1024 AS FreeMB
            FROM sys.database_files
            WHERE type = 0
        """)
        
        resultado = cursor.fetchone()
        if resultado:
            db_name, size_mb, used_mb, free_mb = resultado
            print(f"\n📊 ESPACIO EN BASE DE DATOS:")
            print(f"   Tamaño total: {size_mb:,.0f} MB")
            print(f"   Espacio usado: {used_mb:,.0f} MB")
            print(f"   Espacio libre: {free_mb:,.0f} MB")
            
            # Advertir si hay menos de 500 MB libres
            if free_mb < 500:
                print(f"   ⚠️  ADVERTENCIA: Poco espacio disponible ({free_mb:.0f} MB)")
                
            cursor.close()
            conn.close()
            
            # Retornar False si hay menos de 100 MB
            return free_mb >= 100
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"⚠️  No se pudo verificar espacio disponible: {str(e)}")
        # Continuar de todas formas
        return True

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

def _build_sqlalchemy_dtype_map(columns):
    """Construye un mapa dtype para to_sql usando COLUMN_TYPES_SQL."""
    dtype_map = {}
    for col in columns:
        sql_t = COLUMN_TYPES_SQL.get(col)
        if not sql_t:
            continue
        if sql_t.startswith('VARCHAR'):
            m = re.search(r'VARCHAR\((\d+)\)', sql_t)
            length = int(m.group(1)) if m else 500
            dtype_map[col] = satypes.String(length=length)
        elif sql_t == 'BIGINT':
            dtype_map[col] = satypes.BigInteger()
        elif sql_t == 'INT':
            dtype_map[col] = satypes.Integer()
        elif sql_t.startswith('DECIMAL'):
            m = re.search(r'DECIMAL\((\d+),(\d+)\)', sql_t)
            precision = int(m.group(1)) if m else 18
            scale = int(m.group(2)) if m else 2
            dtype_map[col] = satypes.Numeric(precision=precision, scale=scale)
        elif sql_t == 'DATETIME':
            dtype_map[col] = satypes.DateTime()
    return dtype_map

def cargar_dataframe_a_sql_optimizado(df, table_name):
    """Carga optimizada en memoria con chunks y dtypes explícitos (evita CSV)."""
    print(f"\n🚀 Carga OPTIMIZADA a SQL Server...")
    print(f"   Tabla: {table_name}")
    print(f"   Registros: {len(df):,}")

    try:
        # 0. VERIFICAR ESPACIO DISPONIBLE
        if not verificar_espacio_sql():
            print("❌ ERROR: No hay suficiente espacio en la base de datos")
            print("   Contacta al administrador para liberar espacio o elimina tablas antiguas")
            return False
        
        # 1. Preparar DataFrame
        df_prep = preparar_dataframe_para_sql(df)
        
        # 2. Verificar acción a tomar
        accion = verificar_tabla_existente(table_name)
        
        if accion == 'cancel':
            print("❌ Operación cancelada")
            return False

        # 3. CONFIGURAR ENGINE OPTIMIZADO
        conn_str = f"mssql+pyodbc://{SQL_CONFIG['username']}:{SQL_CONFIG['password']}@{SQL_CONFIG['server']}/{SQL_CONFIG['database']}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
        engine = create_engine(conn_str, fast_executemany=True)
        
        # 4. MANEJAR TABLA SEGÚN ACCIÓN
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
        
        # 5. CARGAR EN CHUNKS OPTIMIZADO (IN-MEMORY)
        chunksize = 50000
        total_chunks_loaded = 0
        dtype_map = _build_sqlalchemy_dtype_map(df_prep.columns)
        print(f"\n📊 Cargando en chunks de {chunksize:,}...")

        n = len(df_prep)
        for start in range(0, n, chunksize):
            end = min(start + chunksize, n)
            chunk = df_prep.iloc[start:end]
            chunk.to_sql(
                table_name,
                engine,
                if_exists=if_exists_mode,
                index=False,
                method=None,
                dtype=dtype_map if if_exists_mode == 'fail' else None
            )

            total_chunks_loaded += 1
            print(f"  Chunk {total_chunks_loaded}: {end:,} de {n:,} registros", end='\r')

            if_exists_mode = 'append'
        
        print()  # Nueva línea después del progreso
        
        # 6. VERIFICAR CARGA
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

def mostrar_resumen_proceso(df, archivo_original, csv_generado=None):
    """Muestra resumen completo del proceso"""
    if df is None:
        return
    
    print("\n" + "=" * 60)
    print("                RESUMEN DEL PROCESO")
    print("=" * 60)
    print(f"📁 Archivo original: {os.path.basename(archivo_original)}")
    if csv_generado:
        print(f"📄 CSV generado: {os.path.basename(csv_generado)}")
    print(f"📊 Registros procesados: {len(df):,}")
    print(f"📋 Columnas finales: {len(df.columns)}")
    print("=" * 60)

# ======================
# FUNCIÓN PRINCIPAL
# ======================

def procesar_archivo_completo(archivo_txt, generar_csv=True, cargar_sql=True, csv_path=None):
    """Proceso completo: TXT → Limpieza → CSV → SQL Server"""
    print("🚀 INICIANDO PROCESO ETL COMPLETO OPTIMIZADO")
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

        # Validar si se perdió toda la estructura (0 columnas)
        if df_limpio.shape[1] == 0:
            print("⚠️  El DataFrame resultante no tiene columnas. Se aborta la carga a SQL para evitar errores.")
            generar_csv = False
            cargar_sql = False
        
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
            print("FASE 3: CARGA OPTIMIZADA A SQL SERVER")
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
        
        print(f"\n🎉 PROCESO ETL OPTIMIZADO COMPLETADO!")
        
        return df_limpio
        
    except Exception as e:
        print(f"❌ Error durante el proceso ETL: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Función principal simplificada - Solo opción 1"""
    print("=" * 80)
    print("       SISTEMA ETL OPTIMIZADO - CLIENTES POTENCIALES CALIDDA")
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
        print("\n✅ Proceso completo OPTIMIZADO finalizado exitosamente")
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
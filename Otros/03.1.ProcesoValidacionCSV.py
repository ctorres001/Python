import pandas as pd
import numpy as np
import os
import warnings
from tqdm import tqdm
from sqlalchemy import create_engine

warnings.filterwarnings('ignore', category=FutureWarning)

CONFIG_PATH = r"D:\FNB\Proyectos\Exportado.xlsx"
CONFIG_SHEET = "Configuracion"
MAPEO_HOJA = "Columnas"
SEPARADOR = "|"
CSV_OUT = "Datos_Validados.csv"
OBS_OUT = r"D:\FNB\Proyectos\Observaciones_Validacion.xlsx"
AUDITORIA_CSV_VALIDADO = r"D:\FNB\Proyectos\Auditoria_Validacion_Final.csv"


def leer_configuracion():
    cfg = pd.read_excel(CONFIG_PATH, sheet_name=CONFIG_SHEET, header=None)
    return {
        "ruta_archivo": str(cfg.iloc[3, 2]),
        "nombre_archivo": str(cfg.iloc[4, 2]),
        "hoja": str(cfg.iloc[5, 2]),
        "fila_inicio": int(cfg.iloc[6, 2]),
        "servidor": str(cfg.iloc[10, 2]),
        "base": str(cfg.iloc[11, 2]),
        "usuario": str(cfg.iloc[12, 2]),
        "clave": str(cfg.iloc[13, 2]),
        "tabla": str(cfg.iloc[17, 2])
    }


def obtener_mapeo_y_nulos():
    columnas = pd.read_excel(CONFIG_PATH, sheet_name=MAPEO_HOJA)
    mapeo = dict(zip(columnas['Nombre_Columna'], columnas['Nombre_Columna_BD']))
    nulos = dict(zip(columnas['Nombre_Columna_BD'], columnas['Nulos'].fillna("Sí")))
    return mapeo, nulos


def obtener_estructura_sql(cfg):
    engine = create_engine(
        f"mssql+pyodbc://{cfg['usuario']}:{cfg['clave']}@{cfg['servidor']}/{cfg['base']}?driver=ODBC+Driver+17+for+SQL+Server"
    )
    query = f"""
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
           NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{cfg['tabla']}'
    """
    return pd.read_sql(query, engine)


def limpiar_valores_vectorizado(series):
    mask_no_nulos = ~pd.isna(series)
    if not mask_no_nulos.any():
        return series
    valores_limpios = series.copy()
    valores_limpios[mask_no_nulos] = (
        series[mask_no_nulos]
        .astype(str)
        .str.replace(r'[\t\r\n]', '-', regex=True)
        .str.replace(r'[\x00-\x1F\u200B]', '', regex=True)
        .str.replace(';', '/', regex=False)
        .str.strip()
        .str.replace(r'\s{2,}', ' ', regex=True)
        .str.replace(',', '', regex=False)
    )
    valores_limpios = valores_limpios.replace(['', 'nan', 'none'], np.nan)
    return valores_limpios


def validar_columna(df_col, col_info, reglas_nulos, nombre_col):
    errores_col = {}
    tipo = col_info['DATA_TYPE']
    largo = col_info['CHARACTER_MAXIMUM_LENGTH']
    regla_nulo_excel = reglas_nulos.get(nombre_col, "Sí").strip().lower()

    col_limpia = limpiar_valores_vectorizado(df_col)
    mask_nulos = pd.isna(col_limpia)

    if regla_nulo_excel == 'no' and mask_nulos.any():
        indices_nulos = df_col.index[mask_nulos].tolist()
        for idx in indices_nulos:
            errores_col[idx + 2] = "Valor nulo no permitido"

    mask_no_nulos = ~mask_nulos
    if not mask_no_nulos.any():
        return col_limpia, errores_col

    valores_no_nulos = col_limpia[mask_no_nulos]

    try:
        if tipo in ['int', 'bigint']:
            valores_float = pd.to_numeric(valores_no_nulos, errors='coerce')
            mask_validos = ~pd.isna(valores_float)
            mask_enteros = valores_float[mask_validos] == valores_float[mask_validos].astype(int)
            indices_invalidos = valores_no_nulos.index[~mask_validos].tolist()
            for idx in indices_invalidos:
                errores_col[idx + 2] = f"Tipo incorrecto para {tipo}"
            if mask_validos.any():
                indices_no_enteros = valores_float[mask_validos].index[~mask_enteros].tolist()
                for idx in indices_no_enteros:
                    errores_col[idx + 2] = "No es entero"
            valores_enteros = valores_float[mask_validos & mask_enteros].astype("Int64")
            col_limpia.loc[valores_enteros.index] = valores_enteros

        elif tipo in ['float', 'decimal', 'numeric', 'money']:
            valores_float = pd.to_numeric(valores_no_nulos, errors='coerce')
            mask_validos = ~pd.isna(valores_float)
            indices_invalidos = valores_no_nulos.index[~mask_validos].tolist()
            for idx in indices_invalidos:
                errores_col[idx + 2] = f"Tipo incorrecto para {tipo}"
            col_limpia.loc[mask_no_nulos] = valores_float

        elif tipo in ['date', 'datetime']:
            fechas_parseadas = pd.to_datetime(valores_no_nulos, errors='coerce')
            mask_validos = ~pd.isna(fechas_parseadas)
            indices_invalidos = valores_no_nulos.index[~mask_validos].tolist()
            for idx in indices_invalidos:
                errores_col[idx + 2] = "Fecha inválida"
            if mask_validos.any():
                col_limpia.loc[valores_no_nulos.index[mask_validos]] = (
                    fechas_parseadas[mask_validos].dt.date.astype(str)
                )

        elif tipo in ['varchar', 'nvarchar']:
            # FORZAR CONVERSIÓN A STRING PARA EVITAR INFERENCIA AUTOMÁTICA
            col_texto = valores_no_nulos.astype(str)
            if largo:
                longitudes = col_texto.str.len()
                mask_excede = longitudes > int(largo)
                indices_excede = valores_no_nulos.index[mask_excede].tolist()
                for idx in indices_excede:
                    errores_col[idx + 2] = f"Excede longitud {int(largo)}"

            # ASIGNAR EXPLÍCITAMENTE COMO STRING
            col_limpia.loc[valores_no_nulos.index] = col_texto

    except Exception as e:
        for idx in valores_no_nulos.index:
            errores_col[idx + 2] = f"Error de validación: {str(e)}"

    # ✅ FORZAR EL TIPO DE DATO FINAL SEGÚN EL TIPO SQL
    if tipo in ['varchar', 'nvarchar']:
        # Convertir toda la columna a string object, incluso los NaN
        col_limpia = col_limpia.astype('object')
        # Asegurar que los valores no nulos sean strings
        mask_no_nulos_final = ~pd.isna(col_limpia)
        if mask_no_nulos_final.any():
            col_limpia.loc[mask_no_nulos_final] = col_limpia.loc[mask_no_nulos_final].astype(str)

    return col_limpia, errores_col


def limpiar_y_validar(df, estructura_sql, reglas_nulos):
    errores_globales = {}
    df_resultado = df.copy()

    # ✅ CREAR DICCIONARIO DE TIPOS SQL PARA REFERENCIA
    tipos_sql = dict(zip(estructura_sql['COLUMN_NAME'], estructura_sql['DATA_TYPE']))

    for _, col_info in tqdm(estructura_sql.iterrows(), total=len(estructura_sql),
                            desc="🔍 Validando columnas", ncols=80, leave=True):
        nombre_col = col_info['COLUMN_NAME']
        if nombre_col not in df_resultado.columns:
            continue

        col_validada, errores_col = validar_columna(
            df_resultado[nombre_col],
            col_info,
            reglas_nulos,
            nombre_col
        )

        # ✅ ASEGURAR QUE SE MANTENGA EL TIPO CORRECTO
        df_resultado[nombre_col] = col_validada

        # ✅ FORZAR TIPO EXPLÍCITO PARA COLUMNAS DE TEXTO
        if col_info['DATA_TYPE'] in ['varchar', 'nvarchar']:
            df_resultado[nombre_col] = df_resultado[nombre_col].astype('object')

        for fila, error in errores_col.items():
            if fila not in errores_globales:
                errores_globales[fila] = {}
            errores_globales[fila][nombre_col] = error

    if errores_globales:
        todas_columnas = estructura_sql['COLUMN_NAME'].tolist()
        filas = sorted(errores_globales.keys())
        data = []
        for fila in filas:
            fila_data = {"Fila": fila}
            for col in todas_columnas:
                fila_data[col] = errores_globales[fila].get(col, "")
            data.append(fila_data)
        df_obs = pd.DataFrame(data)
        return df_resultado, df_obs
    else:
        return df_resultado, None


def main():
    try:
        print("🚀 Iniciando proceso de validación...")
        cfg = leer_configuracion()
        mapeo, reglas_nulos = obtener_mapeo_y_nulos()
        estructura_sql = obtener_estructura_sql(cfg)

        ruta_full = os.path.join(cfg["ruta_archivo"], cfg["nombre_archivo"])
        print(f"📁 Leyendo archivo: {ruta_full}")

        df = pd.read_excel(ruta_full, sheet_name=cfg["hoja"], skiprows=cfg["fila_inicio"] - 1)
        print(f"📊 Datos leídos: {len(df):,} filas, {len(df.columns)} columnas")

        df = df.rename(columns=mapeo)
        columnas_sql = estructura_sql['COLUMN_NAME'].tolist()
        df = df[[col for col in df.columns if col in columnas_sql]]
        print(f"🔧 Columnas después del mapeo: {len(df.columns)}")

        df_limpio, df_errores = limpiar_y_validar(df, estructura_sql, reglas_nulos)

        if df_errores is not None:
            df_errores.to_excel(OBS_OUT, index=False)
            print(f"\n❌ Se encontraron {len(df_errores):,} errores. Revisa: {OBS_OUT}")
            print("\n📋 Resumen de errores por columna:")
            for col in df_errores.columns:
                if col != 'Fila':
                    errores_col = (df_errores[col] != "").sum()
                    if errores_col > 0:
                        print(f"   • {col}: {errores_col:,} errores")
        else:
            ruta_csv = os.path.join(r"D:\FNB\Proyectos", CSV_OUT)

            # ✅ FORZAR TIPOS ANTES DE GUARDAR CSV
            estructura_tipos = dict(zip(estructura_sql['COLUMN_NAME'], estructura_sql['DATA_TYPE']))
            for col in df_limpio.columns:
                if col in estructura_tipos and estructura_tipos[col] in ['varchar', 'nvarchar']:
                    # Forzar como string y eliminar decimales innecesarios
                    mask_no_nulos = ~pd.isna(df_limpio[col])
                    if mask_no_nulos.any():
                        df_limpio.loc[mask_no_nulos, col] = df_limpio.loc[mask_no_nulos, col].astype(str)
                        # Eliminar .0 de números que se convirtieron a string
                        df_limpio[col] = df_limpio[col].str.replace(r'\.0$', '', regex=True)

            df_limpio.to_csv(ruta_csv, sep=SEPARADOR, index=False, encoding='utf-8-sig')
            print(f"\n✅ Validación exitosa. CSV generado en: {ruta_csv}")
            print(f"📊 Registros procesados: {len(df_limpio):,}")
            print("✅ Validación completa. Listo para carga en SQL Server.")

    except Exception as e:
        print(f"\n💥 Error en el procesamiento: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
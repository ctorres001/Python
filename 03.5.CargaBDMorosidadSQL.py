import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import numpy as np

# ======================
# Parámetros
# ======================
excel_path = r"D:\FNB\Reportes\06 Reporte de Morosidad\01. Archivos Cartera\2025-09.xlsx"
sheet_name = "BASE"
csv_path = r"D:\FNB\Reportes\06 Reporte de Morosidad\Reporte_Cartera.csv"

server = "192.168.64.250"
database = "BD_CALIDDA_FNB"
username = "ctorres"
password = "ibr2025"
table_name = "BD_Morosidad_202509"

# Mapeo esperado SQL
column_types_sql = {
    "CTA_CONTR": "BIGINT",
    "TIPO_CLIENTE": "VARCHAR(500)",
    "SEGMENTO": "VARCHAR(500)",
    "NUM_PLAN_R3": "BIGINT",
    "NUM_PLAN_S4": "BIGINT",
    "GRUPO": "VARCHAR(500)",
    "MARCA_CAMP": "VARCHAR(500)",
    "TOTAL": "DECIMAL(18,2)",
    "DEUDA 90-360 S/": "DECIMAL(18,2)",
    "CARTERA <360 S/": "DECIMAL(18,2)",
    "DEUDA 90-360 $": "DECIMAL(18,2)",
    "CARTERA <360 $": "DECIMAL(18,2)",
}

# ======================
# Leer Excel (solo columnas necesarias)
# ======================
print("Leyendo Excel...")
df = pd.read_excel(excel_path, sheet_name=sheet_name, usecols=list(column_types_sql.keys()), dtype=str)

# ======================
# Análisis de tipos
# ======================
print("\n=== Análisis de tipos de datos (Excel vs SQL) ===")
for col in df.columns:
    excel_dtype = str(df[col].dtype)
    sql_dtype = column_types_sql[col]

    if sql_dtype.startswith("VARCHAR"):
        df[col] = df[col].astype(str)  # Forzar texto
    elif "DECIMAL" in sql_dtype or sql_dtype in ("INT", "BIGINT"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    elif sql_dtype == "DATETIME":
        df[col] = pd.to_datetime(df[col], errors="coerce")

    print(f"Columna {col}: Excel={excel_dtype} | SQL={sql_dtype}")

# ======================
# Reemplazar caracteres conflictivos
# ======================
print("\nLimpiando caracteres conflictivos...")
for col in df.select_dtypes(include="object").columns:
    df[col] = df[col].str.replace("¬", "-", regex=False)

# ======================
# Exportar a CSV temporal
# ======================
print("\nExportando a CSV intermedio...")
df.to_csv(csv_path, index=False, sep="¬", encoding="utf-8")

# ======================
# Conexión a SQL Server
# ======================
conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(conn_str, fast_executemany=True)

# ======================
# Cargar CSV en chunks a SQL
# ======================
print("\nCargando datos a SQL Server...")
chunksize = 50000
for chunk in pd.read_csv(csv_path, sep="¬", encoding="utf-8", chunksize=chunksize, engine="python"):
    chunk.to_sql(table_name, engine, if_exists="append", index=False)

print("\n✅ Proceso completado con éxito")

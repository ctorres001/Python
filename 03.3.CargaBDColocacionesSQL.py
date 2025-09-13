import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import numpy as np

# ======================
# Parámetros
# ======================
excel_path = r"D:\FNB\Reportes\01. Reporte Diario\Reporte de Ventas FNB.xlsx"
sheet_name = "BD_Colocaciones"
csv_path = r"D:\FNB\Reportes\01. Reporte Diario\Reporte_Ventas.csv"

server = "192.168.64.250"
database = "BD_CALIDDA_FNB"
username = "ctorres"
password = "ibr2025"
table_name = "BD_Colocaciones_Cierre_20250731"

# Mapeo esperado SQL
column_types_sql = {
    "F_REGISTRO": "DATETIME",
    "F_ENTREGA": "DATETIME",
    "CUENTA_CONTRATO": "BIGINT",
    "DOC_IDENTIDAD": "VARCHAR(50)",
    "NOMBRE_APELLIDO_CLIENTE": "VARCHAR(200)",
    "TELEFONO": "VARCHAR(50)",
    "CORREO_ELECTRONICO": "VARCHAR(200)",
    "DISTRITO": "VARCHAR(200)",
    "NSE": "INT",
    "NRO_CONTRATO": "VARCHAR(50)",
    "NRO_BOLETA": "VARCHAR(50)",
    "PEDIDO_VENTA": "BIGINT",
    "COLOCACION_SOL": "DECIMAL(18,2)",
    "FINANCIAMIENTO_SOL": "DECIMAL(18,2)",
    "CUOTAS": "INT",
    "RESPONSABLE_DE_VENTA": "VARCHAR(200)",
    "PROVEEDOR": "VARCHAR(200)",
    "SEDE": "VARCHAR(200)",
    "MODALIDAD_DE_ENTREGA": "VARCHAR(200)",
    "ESTADO_ENTREGA": "VARCHAR(200)",
    "PRODUCTO_1": "VARCHAR(200)",
    "SKU_1": "VARCHAR(200)",
    "PRODUCTO_2": "VARCHAR(200)",
    "SKU_2": "VARCHAR(200)",
    "PRODUCTO_3": "VARCHAR(200)",
    "SKU_3": "VARCHAR(200)",
    "PRODUCTO_4": "VARCHAR(200)",
    "SKU_4": "VARCHAR(200)",
    "CONCATENAR": "VARCHAR(500)",
    "ASESOR": "VARCHAR(200)",
    "TIEMPO_DE_ENTREGA": "INT",
    "RANGOS": "VARCHAR(50)",
    "ZONA_DE_VENTA": "VARCHAR(50)",
    "MARCA": "VARCHAR(50)",
    "MODELO": "VARCHAR(50)",
    "CANAL": "VARCHAR(50)",
    "TIPO_DE_PRODUCTO": "VARCHAR(50)",
    "TIPO_INSTALACION": "VARCHAR(50)",
    "TIPO_VALIDACION_IDENTIDAD": "VARCHAR(50)",
    "CATEGORIA_PRINCIPAL": "VARCHAR(50)",
    "NRO_TRANSACCIONES": "INT",
    "FEE_PORCENTAJE": "DECIMAL(5,2)",
    "FEE_SOL": "DECIMAL(18,2)",
    "TEA": "DECIMAL(5,2)",
    "TEM": "DECIMAL(7,4)",
    "TC": "DECIMAL(5,2)",
    "VALOR_CUOTA_MES_USD": "DECIMAL(18,2)",
    "VALOR_CUOTA_MES_SOL": "DECIMAL(18,2)",
    "COLOCACION_USD": "DECIMAL(18,2)",
    "FINANCIAMIENTO_USD": "DECIMAL(18,2)",
    "FEE_USD": "DECIMAL(18,2)",
    "FEE_SIN_IGV_USD": "DECIMAL(18,2)",
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

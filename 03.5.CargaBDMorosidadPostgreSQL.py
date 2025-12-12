import pandas as pd
import psycopg2
from psycopg2 import extras
from sqlalchemy import create_engine
import numpy as np

# ======================
# Parámetros
# ======================
excel_path = r"D:\FNB\Reportes\06 Reporte de Morosidad\01. Archivos Cartera\2025-09.xlsx"
sheet_name = "BASE"
csv_path = r"D:\FNB\Reportes\06 Reporte de Morosidad\Reporte_Cartera.csv"

# Configuración PostgreSQL
db_config = {
    "host": "localhost",
    "port": 5432,
    "database": "bd_calidda_fnb",
    "user": "postgres",
    "password": "ibr2025"
}

table_name = "bd_morosidad_202509"

# Mapeo esperado PostgreSQL (tipos de datos ajustados)
column_types_postgresql = {
    "cta_contr": "BIGINT",
    "tipo_cliente": "VARCHAR(500)",
    "segmento": "VARCHAR(500)",
    "num_plan_r3": "BIGINT",
    "num_plan_s4": "BIGINT",
    "grupo": "VARCHAR(500)",
    "marca_camp": "VARCHAR(500)",
    "total": "NUMERIC(18,2)",
    "deuda_90_360_soles": "NUMERIC(18,2)",
    "cartera_menor_360_soles": "NUMERIC(18,2)",
    "deuda_90_360_dolares": "NUMERIC(18,2)",
    "cartera_menor_360_dolares": "NUMERIC(18,2)",
}

# Mapeo de nombres Excel a PostgreSQL
column_mapping = {
    "CTA_CONTR": "cta_contr",
    "TIPO_CLIENTE": "tipo_cliente",
    "SEGMENTO": "segmento",
    "NUM_PLAN_R3": "num_plan_r3",
    "NUM_PLAN_S4": "num_plan_s4",
    "GRUPO": "grupo",
    "MARCA_CAMP": "marca_camp",
    "TOTAL": "total",
    "DEUDA 90-360 S/": "deuda_90_360_soles",
    "CARTERA <360 S/": "cartera_menor_360_soles",
    "DEUDA 90-360 $": "deuda_90_360_dolares",
    "CARTERA <360 $": "cartera_menor_360_dolares",
}

def crear_tabla_bd_morosidad(cursor, table_name):
    """Crea la tabla bd_morosidad si no existe"""
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        cta_contr BIGINT,
        tipo_cliente VARCHAR(500),
        segmento VARCHAR(500),
        num_plan_r3 BIGINT,
        num_plan_s4 BIGINT,
        grupo VARCHAR(500),
        marca_camp VARCHAR(500),
        total NUMERIC(18,2),
        deuda_90_360_soles NUMERIC(18,2),
        cartera_menor_360_soles NUMERIC(18,2),
        deuda_90_360_dolares NUMERIC(18,2),
        cartera_menor_360_dolares NUMERIC(18,2)
    )
    """
    cursor.execute(create_table_sql)
    print(f"✅ Tabla {table_name} verificada/creada")

# ======================
# Leer Excel
# ======================
print("Leyendo Excel...")
df = pd.read_excel(excel_path, sheet_name=sheet_name, usecols=list(column_mapping.keys()), dtype=str)

# Renombrar columnas a formato PostgreSQL
df.rename(columns=column_mapping, inplace=True)

# ======================
# Análisis de tipos
# ======================
print("\n=== Análisis de tipos de datos (Excel vs PostgreSQL) ===")
for col in df.columns:
    excel_dtype = str(df[col].dtype)
    sql_dtype = column_types_postgresql[col]

    if sql_dtype.startswith("VARCHAR"):
        df[col] = df[col].astype(str)
    elif "NUMERIC" in sql_dtype or sql_dtype in ("INT", "BIGINT"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    elif sql_dtype == "TIMESTAMP":
        df[col] = pd.to_datetime(df[col], errors="coerce")

    print(f"Columna {col}: Excel={excel_dtype} | PostgreSQL={sql_dtype}")

# ======================
# Reemplazar caracteres conflictivos
# ======================
print("\nLimpiando caracteres conflictivos...")
for col in df.select_dtypes(include="object").columns:
    df[col] = df[col].str.replace("¬", "-", regex=False)

# Conversión final: evitar NAType
df = df.astype(object).where(pd.notnull(df), None)

# ======================
# Exportar a CSV temporal
# ======================
print("\nExportando a CSV intermedio...")
df.to_csv(csv_path, index=False, sep="|", encoding="utf-8")

# ======================
# Conexión a PostgreSQL
# ======================
print("\nConectando a PostgreSQL...")
conn = psycopg2.connect(**db_config)
cursor = conn.cursor()

# Crear tabla si no existe
crear_tabla_bd_morosidad(cursor, table_name)
conn.commit()

# Preguntar si truncar
truncate = input("¿Desea truncar la tabla antes de cargar los datos? (s/n): ").lower() == "s"
if truncate:
    cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
    conn.commit()
    print("✓ Tabla truncada")

# ======================
# Cargar datos en lotes usando execute_values
# ======================
print("\nCargando datos a PostgreSQL...")
sql_columns = list(column_types_postgresql.keys())
insert_sql = f"INSERT INTO {table_name} ({', '.join(sql_columns)}) VALUES %s"

batch_size = 5000
total = len(df)
print(f"Insertando {total:,} filas en lotes de {batch_size}...")

for i in range(0, total, batch_size):
    batch_df = df.iloc[i:i+batch_size].copy()
    batch = [tuple(row) for row in batch_df.values]
    
    extras.execute_values(cursor, insert_sql, batch, page_size=batch_size)
    conn.commit()
    print(f"Lote {i//batch_size+1}: {len(batch)} filas insertadas ({(i+len(batch))/total:.1%})")

cursor.close()
conn.close()

print("\n✅ Proceso completado con éxito")

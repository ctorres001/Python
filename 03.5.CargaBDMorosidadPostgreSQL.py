import pandas as pd
import psycopg2
from psycopg2 import extras
from sqlalchemy import create_engine
import numpy as np
import os
import re
import sys

# ======================
# Parámetros dinámicos
# ======================
# Si se pasa argumento, usarlo; si no, usar ruta por defecto
if len(sys.argv) > 1:
    excel_path = sys.argv[1]
else:
    excel_path = input("Ingrese la ruta del archivo Excel: ").strip().strip('"')

sheet_name = "BASE"
csv_path = r"D:\FNB\Reportes\06 Reporte de Morosidad\Reporte_Cartera.csv"

# Nombre de tabla se genera automáticamente
table_name = None

# Configuración PostgreSQL
db_config = {
    "host": "localhost",
    "port": 5432,
    "database": "bd_calidda_fnb",
    "user": "postgres",
    "password": "ibr2025"
}

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

def verificar_tabla_existe(cursor, table_name):
    """Verifica si la tabla ya existe en la base de datos"""
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = %s
        )
    """, (table_name,))
    return cursor.fetchone()[0]

def crear_tabla_bd_morosidad(cursor, table_name):
    """Crea la tabla bd_morosidad si no existe"""
    existe = verificar_tabla_existe(cursor, table_name)
    
    if existe:
        print(f"ℹ️  La tabla {table_name} ya existe")
    else:
        print(f"🔨 Creando nueva tabla {table_name}...")
        
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
    
    if not existe:
        print(f"✅ Tabla {table_name} creada exitosamente")
    else:
        print(f"✅ Tabla {table_name} lista para usar")

def extraer_nombre_tabla(ruta_archivo):
    """Extrae el nombre de la tabla del nombre del archivo"""
    nombre_archivo = os.path.basename(ruta_archivo)
    match = re.search(r'(\d{4})-(\d{2})', nombre_archivo)
    if match:
        año = match.group(1)
        mes = match.group(2)
        tabla = f"bd_morosidad_{año}{mes}"
        print(f"\n📋 Archivo detectado: {nombre_archivo}")
        print(f"📊 Tabla generada automáticamente: {tabla}")
        return tabla
    else:
        raise ValueError(f"❌ No se pudo extraer año-mes del archivo: {nombre_archivo}")

# ======================
# Verificar que el archivo existe
# ======================
if not os.path.exists(excel_path):
    print(f"❌ Error: El archivo no existe: {excel_path}")
    sys.exit(1)

# ======================
# Extraer nombre de tabla
# ======================
table_name = extraer_nombre_tabla(excel_path)

# ======================
# Leer Excel
# ======================
print("\n📖 Leyendo Excel...")
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
print("\n🧹 Limpiando caracteres conflictivos...")
for col in df.select_dtypes(include="object").columns:
    df[col] = df[col].str.replace("¬", "-", regex=False)

# Conversión final: evitar NAType
df = df.astype(object).where(pd.notnull(df), None)

# ======================
# Exportar a CSV temporal
# ======================
print("\n💾 Exportando a CSV intermedio...")
df.to_csv(csv_path, index=False, sep="|", encoding="utf-8")

# ======================
# Conexión a PostgreSQL
# ======================
print("\n🔌 Conectando a PostgreSQL...")
conn = psycopg2.connect(**db_config)
cursor = conn.cursor()

# Crear tabla si no existe
crear_tabla_bd_morosidad(cursor, table_name)
conn.commit()

# Preguntar si truncar
truncate = input("\n¿Desea truncar la tabla antes de cargar los datos? (s/n): ").lower() == "s"
if truncate:
    cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
    conn.commit()
    print("✓ Tabla truncada")

# ======================
# Cargar datos en lotes usando execute_values
# ======================
print("\n⬆️  Cargando datos a PostgreSQL...")
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
print(f"📊 Tabla cargada: {table_name}")
print(f"📁 Archivo procesado: {excel_path}")
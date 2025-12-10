import pandas as pd
import psycopg2
from psycopg2 import extras
import numpy as np
from datetime import datetime
import logging

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("carga_categorias_postgresql.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def crear_tabla_bd_categorias(cursor):
    """Crea la tabla bd_categorias si no existe"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS bd_categorias (
        id SERIAL PRIMARY KEY,
        f_registro TIMESTAMP,
        f_entrega TIMESTAMP,
        cuenta_contrato BIGINT,
        doc_identidad VARCHAR(50),
        nombre_apellido_cliente VARCHAR(255),
        distrito VARCHAR(100),
        nse INT,
        categoria VARCHAR(255),
        pedido_venta BIGINT,
        colocacion_sol NUMERIC(18, 2),
        financiamiento_sol NUMERIC(18, 2),
        cuotas INT,
        proveedor VARCHAR(255),
        sede VARCHAR(100),
        responsable_de_venta VARCHAR(255),
        estado_entrega VARCHAR(100),
        marca VARCHAR(100),
        canal VARCHAR(100),
        tc NUMERIC(10, 4),
        colocacion_usd NUMERIC(18, 2),
        concatenar VARCHAR(500),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    cursor.execute(create_table_sql)
    logging.info("✅ Tabla bd_categorias verificada/creada")

def cargar_excel_a_postgresql():
    # Parámetros de conexión PostgreSQL
    db_config = {
        "host": "localhost",
        "port": 5432,
        "database": "bd_calidda_fnb",
        "user": "postgres",
        "password": "ibr2025"
    }

    excel_file = r"D:\FNB\Reportes\01. Reporte Diario\Reporte Diario FNB.xlsx"
    sheet_name = "AcumuladoCat"
    table_name = "bd_categorias"

    try:
        logging.info("=== INICIO DEL PROCESO DE CARGA DE DATOS A BD_Categorias PostgreSQL ===")

        # Definir columnas exactas (minúsculas para PostgreSQL)
        sql_columns = [
            "f_registro", "f_entrega", "cuenta_contrato", "doc_identidad",
            "nombre_apellido_cliente", "distrito", "nse", "categoria",
            "pedido_venta", "colocacion_sol", "financiamiento_sol", "cuotas",
            "proveedor", "sede", "responsable_de_venta", "estado_entrega",
            "marca", "canal", "tc", "colocacion_usd", "concatenar"
        ]

        # Mapeo de nombres Excel a PostgreSQL
        column_mapping = {
            "F_REGISTRO": "f_registro", "F_ENTREGA": "f_entrega",
            "CUENTA_CONTRATO": "cuenta_contrato", "DOC_IDENTIDAD": "doc_identidad",
            "NOMBRE_APELLIDO_CLIENTE": "nombre_apellido_cliente", "DISTRITO": "distrito",
            "NSE": "nse", "CATEGORIA": "categoria", "PEDIDO_VENTA": "pedido_venta",
            "COLOCACION_SOL": "colocacion_sol", "FINANCIAMIENTO_SOL": "financiamiento_sol",
            "CUOTAS": "cuotas", "PROVEEDOR": "proveedor", "SEDE": "sede",
            "RESPONSABLE_DE_VENTA": "responsable_de_venta", "ESTADO_ENTREGA": "estado_entrega",
            "MARCA": "marca", "CANAL": "canal", "TC": "tc",
            "COLOCACION_USD": "colocacion_usd", "CONCATENAR": "concatenar"
        }

        # Leer Excel
        logging.info("Leyendo archivo Excel...")
        df = pd.read_excel(excel_file, sheet_name=sheet_name)

        # Verificar columnas
        excel_columns = list(column_mapping.keys())
        missing = [c for c in excel_columns if c not in df.columns]
        if missing:
            raise ValueError(f"Faltan columnas en Excel: {missing}")

        # Renombrar columnas a formato PostgreSQL
        df = df[excel_columns].rename(columns=column_mapping)

        # Conversión de tipos
        date_columns = ["f_registro", "f_entrega"]
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

        int_columns = ["cuenta_contrato", "nse", "pedido_venta", "cuotas"]
        for col in int_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        decimal_columns = ["colocacion_sol", "financiamiento_sol", "tc", "colocacion_usd"]
        for col in decimal_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").replace([np.inf, -np.inf], np.nan)

        # Forzar DOC_IDENTIDAD siempre como texto
        if "doc_identidad" in df.columns:
            df["doc_identidad"] = df["doc_identidad"].apply(
                lambda x: str(int(x)) if pd.notnull(x) and isinstance(x, (int, float)) and x == int(x)
                else (str(x) if pd.notnull(x) else None)
            )

        # Limpiar strings
        def limpiar_texto(x):
            if pd.isna(x): return None
            s = str(x).strip()
            return s if s and s.lower() not in ("nan", "none", "null") else None

        string_columns = [c for c in sql_columns if c not in date_columns + int_columns + decimal_columns]
        for col in string_columns:
            df[col] = df[col].apply(limpiar_texto)

        # Conversión final: evitar NAType
        df = df.astype(object).where(pd.notnull(df), None)

        # Conectar a PostgreSQL
        logging.info("Conectando a PostgreSQL...")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Crear tabla si no existe
        crear_tabla_bd_categorias(cursor)
        conn.commit()

        # Preguntar si truncar
        truncate = input("¿Desea truncar la tabla bd_categorias antes de cargar los datos? (s/n): ").lower() == "s"
        if truncate:
            cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
            conn.commit()
            logging.info("Tabla truncada")

        # Insertar en lotes usando execute_values
        insert_sql = f"INSERT INTO {table_name} ({', '.join(sql_columns)}) VALUES %s"

        batch_size = 5000
        total = len(df)
        logging.info(f"Insertando {total:,} filas en lotes de {batch_size}...")

        for i in range(0, total, batch_size):
            batch_df = df.iloc[i:i+batch_size].copy()
            batch = [tuple(row) for row in batch_df.values]
            
            extras.execute_values(cursor, insert_sql, batch, page_size=batch_size)
            conn.commit()
            logging.info(f"Lote {i//batch_size+1}: {len(batch)} filas insertadas ({(i+len(batch))/total:.1%})")

        cursor.close()
        conn.close()
        logging.info("=== PROCESO COMPLETADO ===")

    except Exception as e:
        logging.error(f"Error durante la carga: {e}")
        raise

if __name__ == "__main__":
    print("Script de carga de datos de Excel a PostgreSQL - BD_Categorias")
    print("=" * 50)
    confirm = input("¿Desea proceder con la carga de datos? (s/n): ").lower()
    if confirm == "s":
        start = datetime.now()
        cargar_excel_a_postgresql()
        end = datetime.now()
        print(f"\nProceso completado en: {end - start}")
    else:
        print("Proceso cancelado")

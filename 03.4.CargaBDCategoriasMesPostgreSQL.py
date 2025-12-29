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
        logging.FileHandler("carga_categorias_mes_postgresql.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def crear_tabla_bd_categorias_mes(cursor):
    """Crea la tabla bd_categorias_mes si no existe"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS bd_categorias_mes (
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
        concatenar VARCHAR(500)
    )
    """
    cursor.execute(create_table_sql)
    logging.info("✅ Tabla bd_categorias_mes verificada/creada")

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
    sheet_name = "BaseCat"
    table_name = "bd_categorias_mes"

    try:
        logging.info("=== INICIO DEL PROCESO DE CARGA DE DATOS A bd_categorias_mes PostgreSQL ===")

        # Columnas finales en la tabla destino
        sql_columns = [
            "f_registro", "f_entrega", "cuenta_contrato", "doc_identidad",
            "nombre_apellido_cliente", "distrito", "nse", "categoria",
            "pedido_venta", "colocacion_sol", "financiamiento_sol", "cuotas",
            "proveedor", "sede", "responsable_de_venta", "estado_entrega",
            "marca", "canal", "tc", "colocacion_usd", "concatenar"
        ]

        # Leer Excel - BaseCat
        logging.info("Leyendo hoja BaseCat del archivo Excel...")
        df_basecat = pd.read_excel(excel_file, sheet_name=sheet_name)

        # Verificar columnas esperadas en BaseCat
        expected_columns = [
            "PEDIDO_VENTA", "CATE_SAP", "CATE_HOMO", "CATEGORIA",
            "COLOCACION_SOL", "FINANCIAMIENTO_SOL", "CUOTAS",
            "TC", "COLOCACION_USD", "CONCATENAR", "COL_CRUC"
        ]
        missing = [c for c in expected_columns if c not in df_basecat.columns]
        if missing:
            logging.warning(f"⚠️ Columnas faltantes en BaseCat: {missing}")

        # Seleccionar solo las columnas que necesitamos de BaseCat
        basecat_cols = ["PEDIDO_VENTA", "CATEGORIA", "COLOCACION_SOL", 
                        "FINANCIAMIENTO_SOL", "CUOTAS", "TC", 
                        "COLOCACION_USD", "CONCATENAR"]
        df_basecat = df_basecat[basecat_cols].copy()

        logging.info(f"Registros en BaseCat: {len(df_basecat):,}")

        # Conectar a PostgreSQL para leer bd_colocaciones_mes
        logging.info("Conectando a PostgreSQL para leer bd_colocaciones_mes...")
        conn = psycopg2.connect(**db_config)
        
        # Leer bd_colocaciones_mes
        query_colocaciones = """
        SELECT 
            f_registro, f_entrega, cuenta_contrato, doc_identidad,
            nombre_apellido_cliente, distrito, nse, pedido_venta,
            proveedor, sede, responsable_de_venta, estado_entrega,
            marca, canal
        FROM bd_colocaciones_mes
        """
        logging.info("Leyendo tabla bd_colocaciones_mes...")
        df_colocaciones = pd.read_sql(query_colocaciones, conn)
        logging.info(f"Registros en bd_colocaciones_mes: {len(df_colocaciones):,}")

        # Renombrar columnas de BaseCat ANTES del merge
        df_basecat.rename(columns={
            "PEDIDO_VENTA": "pedido_venta",
            "CATEGORIA": "categoria",
            "COLOCACION_SOL": "colocacion_sol",
            "FINANCIAMIENTO_SOL": "financiamiento_sol",
            "CUOTAS": "cuotas",
            "TC": "tc",
            "COLOCACION_USD": "colocacion_usd",
            "CONCATENAR": "concatenar"
        }, inplace=True)

        # Realizar LEFT JOIN por PEDIDO_VENTA
        logging.info("Realizando LEFT JOIN entre BaseCat y bd_colocaciones_mes por PEDIDO_VENTA...")
        df_merged = pd.merge(
            df_basecat,
            df_colocaciones,
            on="pedido_venta",
            how="left",
            suffixes=('_basecat', '_colocaciones')
        )

        # Priorizar columnas de BaseCat para los datos que vienen del Excel
        # y de bd_colocaciones_mes para el resto
        df = pd.DataFrame()
        for col in sql_columns:
            if col in ['categoria', 'colocacion_sol', 'financiamiento_sol', 'cuotas', 
                      'tc', 'colocacion_usd', 'concatenar', 'pedido_venta']:
                # Estas vienen de BaseCat
                if col in df_merged.columns:
                    df[col] = df_merged[col]
                elif f"{col}_basecat" in df_merged.columns:
                    df[col] = df_merged[f"{col}_basecat"]
                else:
                    logging.warning(f"⚠️ Columna {col} no encontrada")
                    df[col] = None
            else:
                # Estas vienen de bd_colocaciones_mes
                if col in df_merged.columns:
                    df[col] = df_merged[col]
                elif f"{col}_colocaciones" in df_merged.columns:
                    df[col] = df_merged[f"{col}_colocaciones"]
                else:
                    logging.warning(f"⚠️ Columna {col} no encontrada")
                    df[col] = None

        logging.info(f"Registros después del JOIN: {len(df):,}")
        logging.info(f"Registros con match: {df['cuenta_contrato'].notna().sum():,}")
        logging.info(f"Registros sin match: {df['cuenta_contrato'].isna().sum():,}")

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
            if pd.isna(x):
                return None
            s = str(x)
            s = s.replace(";", " ")
            s = " ".join(s.split())
            s = s.strip()
            return s if s and s.lower() not in ("nan", "none", "null") else None

        string_columns = [c for c in sql_columns if c not in date_columns + int_columns + decimal_columns]
        for col in string_columns:
            df[col] = df[col].apply(limpiar_texto)

        # Conversión final: evitar NAType
        df = df.astype(object).where(pd.notnull(df), None)

        # Crear tabla si no existe
        cursor = conn.cursor()
        crear_tabla_bd_categorias_mes(cursor)
        conn.commit()

        # Preguntar si truncar
        truncate = input("¿Desea truncar la tabla bd_categorias_mes antes de cargar los datos? (s/n): ").lower() == "s"
        if truncate:
            cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
            conn.commit()
            logging.info("✅ Tabla truncada")

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
            logging.info(f"✅ Lote {i//batch_size+1}: {len(batch)} filas insertadas ({(i+len(batch))/total:.1%})")

        cursor.close()
        conn.close()
        logging.info("=== PROCESO COMPLETADO EXITOSAMENTE ===")

    except Exception as e:
        logging.error(f"❌ Error durante la carga: {e}")
        raise

if __name__ == "__main__":
    print("Script de carga de BaseCat a bd_categorias_mes con JOIN")
    print("=" * 60)
    confirm = input("¿Desea proceder con la carga de datos? (s/n): ").lower()
    if confirm == "s":
        start = datetime.now()
        cargar_excel_a_postgresql()
        end = datetime.now()
        print(f"\n✅ Proceso completado en: {end - start}")
    else:
        print("❌ Proceso cancelado")
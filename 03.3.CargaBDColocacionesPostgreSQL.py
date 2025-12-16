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
        logging.FileHandler("carga_datos_postgresql.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def crear_tabla_bd_colocaciones(cursor):
    """Crea la tabla bd_colocaciones si no existe"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS bd_colocaciones (
        f_registro TIMESTAMP,
        f_entrega TIMESTAMP,
        cuenta_contrato BIGINT,
        doc_identidad VARCHAR(50),
        nombre_apellido_cliente VARCHAR(255),
        telefono VARCHAR(50),
        correo_electronico VARCHAR(255),
        distrito VARCHAR(100),
        nse INT,
        nro_contrato VARCHAR(100),
        nro_boleta VARCHAR(100),
        pedido_venta BIGINT,
        colocacion_sol NUMERIC(18, 2),
        financiamiento_sol NUMERIC(18, 2),
        cuotas INT,
        responsable_de_venta VARCHAR(255),
        proveedor VARCHAR(255),
        sede VARCHAR(100),
        modalidad_de_entrega VARCHAR(100),
        estado_entrega VARCHAR(100),
        anio_fe INT,
        ytd INT,
        producto_1 VARCHAR(255),
        sku_1 VARCHAR(100),
        producto_2 VARCHAR(255),
        sku_2 VARCHAR(100),
        producto_3 VARCHAR(255),
        sku_3 VARCHAR(100),
        producto_4 VARCHAR(255),
        sku_4 VARCHAR(100),
        concatenar VARCHAR(500),
        asesor VARCHAR(255),
        adicional VARCHAR(255),
        b_enero VARCHAR(100),
        tiempo_de_entrega INT,
        rangos VARCHAR(100),
        zona_de_venta VARCHAR(100),
        marca VARCHAR(100),
        modelo VARCHAR(100),
        canal VARCHAR(100),
        tipo_de_producto VARCHAR(100),
        tipo_instalacion VARCHAR(100),
        tipo_validacion_identidad VARCHAR(100),
        categoria_principal VARCHAR(255),
        nro_transacciones INT,
        fee_porcentaje NUMERIC(10, 4),
        fee_sol NUMERIC(18, 2),
        tea NUMERIC(10, 4),
        tem NUMERIC(10, 4),
        tc NUMERIC(10, 4),
        valor_cuota_mes_usd NUMERIC(18, 2),
        valor_cuota_mes_sol NUMERIC(18, 2),
        colocacion_usd NUMERIC(18, 2),
        financiamiento_usd NUMERIC(18, 2),
        fee_usd NUMERIC(18, 2),
        fee_sin_igv_usd NUMERIC(18, 2)
    )
    """
    cursor.execute(create_table_sql)
    logging.info("✅ Tabla bd_colocaciones verificada/creada")

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
    sheet_name = "Acumulado"
    table_name = "bd_colocaciones"  # PostgreSQL usa minúsculas por convención

    try:
        logging.info("=== INICIO DEL PROCESO DE CARGA DE DATOS A POSTGRESQL ===")

        # Definir columnas exactas (minúsculas para PostgreSQL)
        sql_columns = [
            'f_registro', 'f_entrega', 'cuenta_contrato', 'doc_identidad',
            'nombre_apellido_cliente', 'telefono', 'correo_electronico', 'distrito',
            'nse', 'nro_contrato', 'nro_boleta', 'pedido_venta', 'colocacion_sol',
            'financiamiento_sol', 'cuotas', 'responsable_de_venta', 'proveedor',
            'sede', 'modalidad_de_entrega', 'estado_entrega', 'anio_fe', 'ytd',
            'producto_1', 'sku_1', 'producto_2', 'sku_2', 'producto_3', 'sku_3',
            'producto_4', 'sku_4', 'concatenar', 'asesor', 'adicional', 'b_enero',
            'tiempo_de_entrega', 'rangos', 'zona_de_venta', 'marca', 'modelo',
            'canal', 'tipo_de_producto', 'tipo_instalacion', 'tipo_validacion_identidad',
            'categoria_principal', 'nro_transacciones', 'fee_porcentaje', 'fee_sol',
            'tea', 'tem', 'tc', 'valor_cuota_mes_usd', 'valor_cuota_mes_sol',
            'colocacion_usd', 'financiamiento_usd', 'fee_usd', 'fee_sin_igv_usd'
        ]

        # Mapeo de nombres Excel a PostgreSQL
        column_mapping = {
            'F_REGISTRO': 'f_registro', 'F_ENTREGA': 'f_entrega', 
            'CUENTA_CONTRATO': 'cuenta_contrato', 'DOC_IDENTIDAD': 'doc_identidad',
            'NOMBRE_APELLIDO_CLIENTE': 'nombre_apellido_cliente', 'TELEFONO': 'telefono',
            'CORREO_ELECTRONICO': 'correo_electronico', 'DISTRITO': 'distrito',
            'NSE': 'nse', 'NRO_CONTRATO': 'nro_contrato', 'NRO_BOLETA': 'nro_boleta',
            'PEDIDO_VENTA': 'pedido_venta', 'COLOCACION_SOL': 'colocacion_sol',
            'FINANCIAMIENTO_SOL': 'financiamiento_sol', 'CUOTAS': 'cuotas',
            'RESPONSABLE_DE_VENTA': 'responsable_de_venta', 'PROVEEDOR': 'proveedor',
            'SEDE': 'sede', 'MODALIDAD_DE_ENTREGA': 'modalidad_de_entrega',
            'ESTADO_ENTREGA': 'estado_entrega', 'AÑO_FE': 'anio_fe', 'YTD': 'ytd',
            'PRODUCTO_1': 'producto_1', 'SKU_1': 'sku_1', 'PRODUCTO_2': 'producto_2',
            'SKU_2': 'sku_2', 'PRODUCTO_3': 'producto_3', 'SKU_3': 'sku_3',
            'PRODUCTO_4': 'producto_4', 'SKU_4': 'sku_4', 'CONCATENAR': 'concatenar',
            'ASESOR': 'asesor', 'ADICIONAL': 'adicional', 'B_ENERO': 'b_enero',
            'TIEMPO_DE_ENTREGA': 'tiempo_de_entrega', 'RANGOS': 'rangos',
            'ZONA_DE_VENTA': 'zona_de_venta', 'MARCA': 'marca', 'MODELO': 'modelo',
            'CANAL': 'canal', 'TIPO_DE_PRODUCTO': 'tipo_de_producto',
            'TIPO_INSTALACION': 'tipo_instalacion', 
            'TIPO_VALIDACION_IDENTIDAD': 'tipo_validacion_identidad',
            'CATEGORIA_PRINCIPAL': 'categoria_principal', 
            'NRO_TRANSACCIONES': 'nro_transacciones',
            'FEE_PORCENTAJE': 'fee_porcentaje', 'FEE_SOL': 'fee_sol',
            'TEA': 'tea', 'TEM': 'tem', 'TC': 'tc',
            'VALOR_CUOTA_MES_USD': 'valor_cuota_mes_usd',
            'VALOR_CUOTA_MES_SOL': 'valor_cuota_mes_sol',
            'COLOCACION_USD': 'colocacion_usd', 'FINANCIAMIENTO_USD': 'financiamiento_usd',
            'FEE_USD': 'fee_usd', 'FEE_SIN_IGV_USD': 'fee_sin_igv_usd'
        }

        # Leer Excel
        logging.info("Leyendo archivo Excel...")
        df = pd.read_excel(excel_file, sheet_name=sheet_name)

        # Renombrar columnas específicas
        df.rename(columns={'AÑO FE': 'AÑO_FE', 'B.ENERO': 'B_ENERO'}, inplace=True)

        # Verificar columnas necesarias
        excel_columns = [k for k in column_mapping.keys()]
        missing = [c for c in excel_columns if c not in df.columns]
        if missing:
            raise ValueError(f"Faltan columnas en Excel: {missing}")

        # Renombrar columnas a formato PostgreSQL
        df = df[excel_columns].rename(columns=column_mapping)

        # Conversión de tipos
        date_columns = ['f_registro', 'f_entrega']
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        int_columns = ['cuenta_contrato', 'nse', 'pedido_venta', 'cuotas',
                      'anio_fe', 'ytd', 'tiempo_de_entrega', 'nro_transacciones']
        for col in int_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype("Int64")

        decimal_columns = ['colocacion_sol', 'financiamiento_sol', 'fee_porcentaje',
                          'fee_sol', 'tea', 'tem', 'tc', 'valor_cuota_mes_usd',
                          'valor_cuota_mes_sol', 'colocacion_usd', 'financiamiento_usd',
                          'fee_usd', 'fee_sin_igv_usd']
        for col in decimal_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').replace([np.inf, -np.inf], np.nan)

        # Forzar DOC_IDENTIDAD y TELEFONO a texto siempre
        for col in ["doc_identidad", "telefono"]:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: str(int(x)) if pd.notnull(x) and isinstance(x, (int, float)) and x == int(x)
                    else (str(x) if pd.notnull(x) else None)
                )

        # Limpiar strings
        def limpiar_texto(x):
            if pd.isna(x):
                return None
            s = str(x)
            # Reemplazar punto y coma por espacio y normalizar espacios múltiples
            s = s.replace(";", " ")
            s = " ".join(s.split())
            s = s.strip()
            # Tratar valores tipo 'nan', 'none', 'null' como nulos
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
        crear_tabla_bd_colocaciones(cursor)
        conn.commit()

        # Preguntar si truncar
        truncate = input("¿Desea truncar la tabla antes de cargar los datos? (s/n): ").lower() == "s"
        if truncate:
            cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
            conn.commit()
            logging.info("Tabla truncada")

        # Insertar en lotes usando execute_values (más rápido en PostgreSQL)
        placeholders = ", ".join([f"%s" for _ in sql_columns])
        insert_sql = f"INSERT INTO {table_name} ({', '.join(sql_columns)}) VALUES %s"

        batch_size = 5000
        total = len(df)
        logging.info(f"Insertando {total:,} filas en lotes de {batch_size}...")

        for i in range(0, total, batch_size):
            batch_df = df.iloc[i:i+batch_size].copy()
            batch = [tuple(row) for row in batch_df.values]
            
            # execute_values es mucho más rápido que executemany en PostgreSQL
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
    print("Script de carga de datos de Excel a PostgreSQL")
    print("=" * 50)
    confirm = input("¿Desea proceder con la carga de datos? (s/n): ").lower()
    if confirm == "s":
        start = datetime.now()
        cargar_excel_a_postgresql()
        end = datetime.now()
        print(f"\nProceso completado en: {end - start}")
    else:
        print("Proceso cancelado")

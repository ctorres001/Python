import pandas as pd
import pyodbc
import numpy as np
from datetime import datetime
import logging

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("carga_datos.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def cargar_excel_a_sqlserver():
    # Parámetros de conexión
    server = "192.168.64.250"
    database = "BD_CALIDDA_FNB"
    username = "ctorres"
    password = "ibr2025"

    excel_file = r"D:\FNB\Reportes\01. Reporte Diario\Reporte Diario FNB.xlsx"
    sheet_name = "Acumulado"
    table_name = "BD_Colocaciones"

    try:
        logging.info("=== INICIO DEL PROCESO DE CARGA DE DATOS ===")

        # Definir columnas exactas
        sql_columns = [
            'F_REGISTRO', 'F_ENTREGA', 'CUENTA_CONTRATO', 'DOC_IDENTIDAD',
            'NOMBRE_APELLIDO_CLIENTE', 'TELEFONO', 'CORREO_ELECTRONICO', 'DISTRITO',
            'NSE', 'NRO_CONTRATO', 'NRO_BOLETA', 'PEDIDO_VENTA', 'COLOCACION_SOL',
            'FINANCIAMIENTO_SOL', 'CUOTAS', 'RESPONSABLE_DE_VENTA', 'PROVEEDOR',
            'SEDE', 'MODALIDAD_DE_ENTREGA', 'ESTADO_ENTREGA', 'AÑO_FE', 'YTD',
            'PRODUCTO_1', 'SKU_1', 'PRODUCTO_2', 'SKU_2', 'PRODUCTO_3', 'SKU_3',
            'PRODUCTO_4', 'SKU_4', 'CONCATENAR', 'ASESOR', 'ADICIONAL', 'B_ENERO',
            'TIEMPO_DE_ENTREGA', 'RANGOS', 'ZONA_DE_VENTA', 'MARCA', 'MODELO',
            'CANAL', 'TIPO_DE_PRODUCTO', 'TIPO_INSTALACION', 'TIPO_VALIDACION_IDENTIDAD',
            'CATEGORIA_PRINCIPAL', 'NRO_TRANSACCIONES', 'FEE_PORCENTAJE', 'FEE_SOL',
            'TEA', 'TEM', 'TC', 'VALOR_CUOTA_MES_USD', 'VALOR_CUOTA_MES_SOL',
            'COLOCACION_USD', 'FINANCIAMIENTO_USD', 'FEE_USD', 'FEE_SIN_IGV_USD'
        ]

        # Leer Excel
        logging.info("Leyendo archivo Excel...")
        df = pd.read_excel(excel_file, sheet_name=sheet_name)

        # Renombrar columnas específicas
        df.rename(columns={'AÑO FE': 'AÑO_FE', 'B.ENERO': 'B_ENERO'}, inplace=True)

        # Verificar columnas
        missing = [c for c in sql_columns if c not in df.columns]
        if missing:
            raise ValueError(f"Faltan columnas en Excel: {missing}")

        # Seleccionar solo columnas requeridas
        df = df[sql_columns]

        # Conversión de tipos
        date_columns = ['F_REGISTRO', 'F_ENTREGA']
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        int_columns = ['CUENTA_CONTRATO', 'NSE', 'PEDIDO_VENTA', 'CUOTAS',
                    'AÑO_FE', 'YTD', 'TIEMPO_DE_ENTREGA', 'NRO_TRANSACCIONES']
        for col in int_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype("Int64")

        decimal_columns = ['COLOCACION_SOL', 'FINANCIAMIENTO_SOL', 'FEE_PORCENTAJE',
                        'FEE_SOL', 'TEA', 'TEM', 'TC', 'VALOR_CUOTA_MES_USD',
                        'VALOR_CUOTA_MES_SOL', 'COLOCACION_USD', 'FINANCIAMIENTO_USD',
                        'FEE_USD', 'FEE_SIN_IGV_USD']
        for col in decimal_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').replace([np.inf, -np.inf], np.nan)

        # 🔑 Forzar DOC_IDENTIDAD y TELEFONO a texto siempre
        for col in ["DOC_IDENTIDAD", "TELEFONO"]:
            if col in df.columns:
                df[col] = df[col].apply(
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

        # 🔑 Conversión final: evitar NAType
        df = df.astype(object).where(pd.notnull(df), None)


        # Conectar a SQL Server
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};DATABASE={database};UID={username};PWD={password}"
        )
        conn = pyodbc.connect(conn_str, autocommit=False)
        cursor = conn.cursor()
        cursor.fast_executemany = True

        # Preguntar si truncar
        truncate = input("¿Desea truncar la tabla antes de cargar los datos? (s/n): ").lower() == "s"
        if truncate:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            conn.commit()
            logging.info("Tabla truncada")

        # Insertar en lotes rápidos
        placeholders = ", ".join(["?" for _ in sql_columns])
        insert_sql = f"INSERT INTO {table_name} ({', '.join(sql_columns)}) VALUES ({placeholders})"

        batch_size = 5000
        total = len(df)
        logging.info(f"Insertando {total:,} filas en lotes de {batch_size}...")

        for i in range(0, total, batch_size):
            batch_df = df.iloc[i:i+batch_size].copy()
            batch = batch_df.values.tolist()
            cursor.executemany(insert_sql, batch)
            conn.commit()
            logging.info(f"Lote {i//batch_size+1}: {len(batch)} filas insertadas ({(i+len(batch))/total:.1%})")

        cursor.close()
        conn.close()
        logging.info("=== PROCESO COMPLETADO ===")

    except Exception as e:
        logging.error(f"Error durante la carga: {e}")
        raise

if __name__ == "__main__":
    print("Script de carga de datos de Excel a SQL Server")
    print("=" * 50)
    confirm = input("¿Desea proceder con la carga de datos? (s/n): ").lower()
    if confirm == "s":
        start = datetime.now()
        cargar_excel_a_sqlserver()
        end = datetime.now()
        print(f"\nProceso completado en: {end - start}")
    else:
        print("Proceso cancelado")


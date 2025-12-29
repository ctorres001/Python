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
        logging.FileHandler("carga_datos_mes_postgresql.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def crear_tabla_bd_colocaciones_mes(cursor):
    """Crea la tabla bd_colocaciones_mes si no existe"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS bd_colocaciones_mes (
        f_registro DATE,
        f_entrega DATE,
        cuenta_contrato BIGINT,
        doc_identidad VARCHAR(50),
        nombre_apellido_cliente VARCHAR(255),
        telefono VARCHAR(50),
        correo_electronico VARCHAR(255),
        distrito VARCHAR(100),
        nse INT,
        nro_contrato TEXT,
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
    logging.info("✅ Tabla bd_colocaciones_mes verificada/creada")

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
    sheet_name = "Base"  # Hoja origen cambiada
    table_name = "bd_colocaciones_mes"  # Tabla destino cambiada

    try:
        logging.info("=== INICIO DEL PROCESO DE CARGA DE DATOS A POSTGRESQL (Mensual) ===")

        # Leer Excel
        logging.info("Leyendo archivo Excel (Hoja: Base)...")
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        
        logging.info(f"Registros leídos: {len(df):,}")

        # PASO 1: Eliminar columnas no necesarias (equivalente a RemoveColumns)
        columnas_a_quitar = [
            "CruceFecha", "TelefonoOriginal", "CruceBoleta", "RESPONSABLE ORIGINAL", 
            "PROVEEDOR ORIGINAL", "SEDE ORIGINAL", "ESTADO ORIGINAL", "CruceEstado", 
            "Marca Rep", "Auditoria", "Duplicado", "FFVV_EDU_PROV", "CALL_CSC", 
            "PROV_MOTOS", "CATECRUC", "Nro PV Cardif"
        ]
        
        # Eliminar solo las columnas que existan
        columnas_existentes_a_quitar = [col for col in columnas_a_quitar if col in df.columns]
        if columnas_existentes_a_quitar:
            df = df.drop(columns=columnas_existentes_a_quitar)
            logging.info(f"Columnas eliminadas: {len(columnas_existentes_a_quitar)}")

        # PASO 2: Convertir fechas a tipo date
        df['F. Registro'] = pd.to_datetime(df['F. Registro'], errors='coerce').dt.date
        df['F. Entrega'] = pd.to_datetime(df['F. Entrega'], errors='coerce').dt.date

        # PASO 3: Renombrar columnas (equivalente a RenameColumns)
        column_mapping = {
            'F. Registro': 'f_registro',
            'F. Entrega': 'f_entrega',
            'Cuenta Contrato': 'cuenta_contrato',
            'DNI': 'doc_identidad',
            'Nombre y Apellido de Cliente': 'nombre_apellido_cliente',
            'Telefono': 'telefono',
            'Correo Electronico': 'correo_electronico',
            'Distrito': 'distrito',
            'NSE': 'nse',
            'N° de Contrato': 'nro_contrato',
            'N° de Boleta': 'nro_boleta',
            'Pedido Venta': 'pedido_venta',
            'Importe\nColocación  S/': 'colocacion_sol',
            'Importe\nFinanciamiento  S/': 'financiamiento_sol',
            'N° de Cuotas': 'cuotas',
            'Nombre Responsable de Venta': 'responsable_de_venta',
            'Nombre de Proveedor': 'proveedor',
            'Nombre Tienda de Venta': 'sede',
            'Modalidad de Entrega': 'modalidad_de_entrega',
            'Estado de Entrega': 'estado_entrega',
            'Año FE': 'anio_fe',
            'YTD': 'ytd',
            'PRODUCTO 1': 'producto_1',
            'SKU 1': 'sku_1',
            'PRODUCTO 2': 'producto_2',
            'SKU 2': 'sku_2',
            'PRODUCTO 3': 'producto_3',
            'SKU 3': 'sku_3',
            'PRODUCTO 4': 'producto_4',
            'SKU 4': 'sku_4',
            'Concatenar': 'concatenar',
            'Asesor': 'asesor',
            'Adicional': 'adicional',
            'B.Enero': 'b_enero',
            'Tiempo de Entrega': 'tiempo_de_entrega',
            'Rangos': 'rangos',
            'Zona de Venta': 'zona_de_venta',
            'Marca': 'marca',
            'Modelo': 'modelo',
            'Canal': 'canal',
            'Tipo de Producto': 'tipo_de_producto',
            'TIPO INST': 'tipo_instalacion',
            'Tipo Validación Identidad': 'tipo_validacion_identidad',
            'CATEGORIA PRINCIPAL': 'categoria_principal',
            '# Transacciones': 'nro_transacciones',
            'FEE  %': 'fee_porcentaje',
            'FEE_SOL': 'fee_sol',
            'TEA': 'tea',
            'TEM': 'tem',
            'TC': 'tc',
            'VALOR_CUOTA_MES_USD': 'valor_cuota_mes_usd',
            'VALOR_CUOTA_MES_SOL': 'valor_cuota_mes_sol',
            'COLOCACION_USD': 'colocacion_usd',
            'FINANCIAMIENTO_USD': 'financiamiento_usd',
            'FEE_USD': 'fee_usd',
            'FEE_SIN_IGV_USD': 'fee_sin_igv_usd'
        }

        # Renombrar solo las columnas que existan
        columnas_a_renombrar = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=columnas_a_renombrar)
        
        logging.info(f"Columnas renombradas: {len(columnas_a_renombrar)}")

        # PASO 4: Conversión de tipos de datos
        
        # Columnas enteras
        int_columns = ['cuenta_contrato', 'nse', 'pedido_venta', 'cuotas',
                      'anio_fe', 'ytd', 'tiempo_de_entrega', 'nro_transacciones']
        for col in int_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype("Int64")

        # Columnas decimales
        decimal_columns = ['colocacion_sol', 'financiamiento_sol', 'fee_porcentaje',
                          'fee_sol', 'tea', 'tem', 'tc', 'valor_cuota_mes_usd',
                          'valor_cuota_mes_sol', 'colocacion_usd', 'financiamiento_usd',
                          'fee_usd', 'fee_sin_igv_usd']
        for col in decimal_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').replace([np.inf, -np.inf], np.nan)

        # NRO_CONTRATO como texto (según el paso TransformColumnTypes)
        if 'nro_contrato' in df.columns:
            df['nro_contrato'] = df['nro_contrato'].astype(str)
            df.loc[df['nro_contrato'] == 'nan', 'nro_contrato'] = None

        # Forzar DOC_IDENTIDAD y TELEFONO a texto
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
            s = s.replace(";", " ")
            s = " ".join(s.split())
            s = s.strip()
            return s if s and s.lower() not in ("nan", "none", "null") else None

        # Definir columnas finales esperadas
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

        string_columns = [c for c in sql_columns if c not in ['f_registro', 'f_entrega'] + int_columns + decimal_columns]
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].apply(limpiar_texto)

        # Asegurar que solo tenemos las columnas necesarias
        df = df[[col for col in sql_columns if col in df.columns]]
        
        # Conversión final: evitar NAType
        df = df.astype(object).where(pd.notnull(df), None)

        logging.info(f"Registros después de limpieza: {len(df):,}")

        # Conectar a PostgreSQL
        logging.info("Conectando a PostgreSQL...")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Crear tabla si no existe
        crear_tabla_bd_colocaciones_mes(cursor)
        conn.commit()

        # Preguntar si truncar
        truncate = input("¿Desea truncar la tabla antes de cargar los datos? (s/n): ").lower() == "s"
        if truncate:
            cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
            conn.commit()
            logging.info("Tabla truncada")

        # Insertar en lotes
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
    print("Script de carga de datos mensuales de Excel a PostgreSQL")
    print("=" * 60)
    confirm = input("¿Desea proceder con la carga de datos? (s/n): ").lower()
    if confirm == "s":
        start = datetime.now()
        cargar_excel_a_postgresql()
        end = datetime.now()
        print(f"\nProceso completado en: {end - start}")
    else:
        print("Proceso cancelado")
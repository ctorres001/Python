import pandas as pd
import psycopg2
from psycopg2 import extras
import numpy as np
from datetime import datetime
import logging
import tkinter as tk
from tkinter import filedialog

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("carga_anulaciones_postgresql.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def crear_tabla_bd_anulaciones(cursor):
    """Crea la tabla bd_anulaciones si no existe"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS bd_anulaciones (
        id SERIAL PRIMARY KEY,
        responsable_de_venta VARCHAR(255),
        sede VARCHAR(100),
        aliado_comercial VARCHAR(255),
        cuenta_contrato BIGINT,
        cliente VARCHAR(255),
        dni VARCHAR(50),
        pedido_venta BIGINT,
        pedido_venta_seguro BIGINT,
        pedido_venta_ingreso_seguro BIGINT,
        importe NUMERIC(18, 2),
        credito_utilizado NUMERIC(18, 2),
        nro_de_cuotas INT,
        fecha_venta DATE,
        fecha_entrega DATE,
        tipo_despacho VARCHAR(100),
        estado VARCHAR(100),
        asesor_de_ventas VARCHAR(255),
        boleta VARCHAR(100),
        usuario_solicitante VARCHAR(255),
        fecha_solicitud DATE,
        motivo TEXT,
        comentarios TEXT,
        estado_anulacion VARCHAR(100),
        fecha_estado DATE,
        usuario_aprobador VARCHAR(255),
        validado_proveedor VARCHAR(100),
        usuario_asignado VARCHAR(255),
        fecha_usuario_asignacion DATE,
        responsable_asignado VARCHAR(255),
        fecha_responsable_asignacion DATE,
        fecha_respuesta_responsable DATE,
        fecha_registro_1ra_instancia DATE,
        fecha_registro_2da_instancia DATE,
        usuario_rechazo_anulacion VARCHAR(255),
        fecha_rechazo_anulacion DATE,
        derivacion_interna TEXT,
        fecha_anulacion DATE,
        usuario_anulacion VARCHAR(255),
        nro_documento_de_deuda_dd VARCHAR(100),
        corresponde_descuento VARCHAR(50),
        cliente_pago_alguna_cuota VARCHAR(50),
        nro_de_aviso_generado VARCHAR(100),
        tipo_anulacion VARCHAR(100),
        estado_del_aviso VARCHAR(100),
        estado_de_la_medida VARCHAR(100),
        fecha_de_aplicacion_de_la_medida DATE,
        proceso_de_retencion VARCHAR(100),
        fecha_de_inicio_de_retencion DATE,
        medida_de_retencion VARCHAR(100),
        detalle_de_la_medida_de_retencion TEXT,
        se_retuvo_al_cliente VARCHAR(50),
        se_realizo_descuento_al_proveedor VARCHAR(50),
        nro_pv_descuento VARCHAR(100),
        comentarios_del_descuento_al_proveedor TEXT
    )
    """
    cursor.execute(create_table_sql)
    logging.info("✅ Tabla bd_anulaciones verificada/creada")

def seleccionar_archivo():
    """Abre un diálogo para seleccionar el archivo Excel"""
    root = tk.Tk()
    root.withdraw()  # Ocultar la ventana principal
    root.attributes('-topmost', True)  # Ventana siempre al frente
    
    archivo = filedialog.askopenfilename(
        title="Seleccionar archivo Excel de Anulaciones",
        filetypes=[
            ("Archivos Excel", "*.xlsx *.xls"),
            ("Todos los archivos", "*.*")
        ]
    )
    
    root.destroy()
    return archivo

def limpiar_datos(df):
    """Limpia y prepara los datos del DataFrame"""
    # Mapeo de columnas del Excel a los nombres de la base de datos
    columnas_mapeo = {
        'ID': 'id',
        'RESPONSABLE DE VENTA': 'responsable_de_venta',
        'SEDE': 'sede',
        'ALIADO COMERCIAL': 'aliado_comercial',
        'CUENTA CONTRATO': 'cuenta_contrato',
        'CLIENTE': 'cliente',
        'DNI': 'dni',
        'N° PEDIDO VENTA': 'pedido_venta',
        'N° PEDIDO VENTA SEGURO': 'pedido_venta_seguro',
        'N° PEDIDO VENTA INGRESO SEGURO': 'pedido_venta_ingreso_seguro',
        'IMPORTE (S./)': 'importe',
        'CRÉDITO UTILIZADO': 'credito_utilizado',
        'NRO. DE CUOTAS': 'nro_de_cuotas',
        'FECHA VENTA': 'fecha_venta',
        'FECHA ENTREGA': 'fecha_entrega',
        'TIPO DESPACHO': 'tipo_despacho',
        'ESTADO': 'estado',
        'ASESOR DE VENTAS': 'asesor_de_ventas',
        'BOLETA': 'boleta',
        'USUARIO SOLICITANTE': 'usuario_solicitante',
        'FECHA SOLICITUD': 'fecha_solicitud',
        'MOTIVO': 'motivo',
        'COMENTARIOS': 'comentarios',
        'ESTADO ANULACION': 'estado_anulacion',
        'FECHA ESTADO': 'fecha_estado',
        'USUARIO APROBADOR': 'usuario_aprobador',
        'VALIDADO PROVEEDOR': 'validado_proveedor',
        'USUARIO ASIGNADO': 'usuario_asignado',
        'FECHA USUARIO ASIGNACION': 'fecha_usuario_asignacion',
        'RESPONSABLE ASIGNADO': 'responsable_asignado',
        'FECHA RESPONSABLE ASIGNACION': 'fecha_responsable_asignacion',
        'FECHA RESPUESTA RESPONSABLE': 'fecha_respuesta_responsable',
        'FECHA REGISTRO 1RA INSTANCIA': 'fecha_registro_1ra_instancia',
        'FECHA REGISTRO 2DA INSTANCIA': 'fecha_registro_2da_instancia',
        'USUARIO RECHAZO ANULACION': 'usuario_rechazo_anulacion',
        'FECHA RECHAZO ANULACION': 'fecha_rechazo_anulacion',
        'DERIVACION INTERNA': 'derivacion_interna',
        'FECHA ANULACION': 'fecha_anulacion',
        'USUARIO ANULACION': 'usuario_anulacion',
        'NRO. DOCUMENTO DE DEUDA (DD)': 'nro_documento_de_deuda_dd',
        '¿CORRESPONDE DESCUENTO?': 'corresponde_descuento',
        '¿CLIENTE PAGO ALGUNA CUOTA?': 'cliente_pago_alguna_cuota',
        'NRO. DE AVISO GENERADO': 'nro_de_aviso_generado',
        'TIPO ANULACION': 'tipo_anulacion',
        'ESTADO DEL AVISO': 'estado_del_aviso',
        'ESTADO DE LA MEDIDA': 'estado_de_la_medida',
        'FECHA DE APLICACIÓN DE LA MEDIDA': 'fecha_de_aplicacion_de_la_medida',
        'PROCESO DE RETENCION': 'proceso_de_retencion',
        'FECHA DE INICIO DE RETENCION': 'fecha_de_inicio_de_retencion',
        'MEDIDA DE RETENCION': 'medida_de_retencion',
        'DETALLE DE LA MEDIDA DE RETENCION': 'detalle_de_la_medida_de_retencion',
        'SE RETUVO AL CLIENTE': 'se_retuvo_al_cliente',
        'SE REALIZO DESCUENTO AL PROVEEDOR': 'se_realizo_descuento_al_proveedor',
        'NRO. PV DESCUENTO': 'nro_pv_descuento',
        'COMENTARIOS DEL DESCUENTO AL PROVEEDOR': 'comentarios_del_descuento_al_proveedor'
    }
    
    # Renombrar columnas
    df = df.rename(columns=columnas_mapeo)
    
    # Convertir columnas de fecha con dayfirst=True para evitar warnings
    columnas_fecha = [
        'fecha_venta', 'fecha_entrega', 'fecha_solicitud', 'fecha_estado',
        'fecha_usuario_asignacion', 'fecha_responsable_asignacion', 
        'fecha_respuesta_responsable', 'fecha_registro_1ra_instancia',
        'fecha_registro_2da_instancia', 'fecha_rechazo_anulacion',
        'fecha_anulacion', 'fecha_de_aplicacion_de_la_medida',
        'fecha_de_inicio_de_retencion'
    ]
    
    for col in columnas_fecha:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
            # Convertir NaT a None inmediatamente después de conversión
            df[col] = df[col].where(pd.notna(df[col]), None)
    
    # Convertir columnas numéricas
    if 'importe' in df.columns:
        df['importe'] = pd.to_numeric(df['importe'], errors='coerce')
    if 'credito_utilizado' in df.columns:
        df['credito_utilizado'] = pd.to_numeric(df['credito_utilizado'], errors='coerce')
    
    # Convertir columnas de texto a string y limpiar espacios
    # IMPORTANTE: No hacer astype(str) en columnas que NO son de texto
    columnas_texto = [
        'responsable_de_venta', 'sede', 'aliado_comercial', 'cliente', 'dni',
        'tipo_despacho', 'estado', 'asesor_de_ventas', 'boleta',
        'usuario_solicitante', 'motivo', 'comentarios', 'estado_anulacion',
        'usuario_aprobador', 'validado_proveedor', 'usuario_asignado',
        'responsable_asignado', 'usuario_rechazo_anulacion', 'derivacion_interna',
        'usuario_anulacion', 'nro_documento_de_deuda_dd', 'corresponde_descuento',
        'cliente_pago_alguna_cuota', 'nro_de_aviso_generado', 'tipo_anulacion',
        'estado_del_aviso', 'estado_de_la_medida', 'proceso_de_retencion',
        'medida_de_retencion', 'detalle_de_la_medida_de_retencion',
        'se_retuvo_al_cliente', 'se_realizo_descuento_al_proveedor',
        'nro_pv_descuento', 'comentarios_del_descuento_al_proveedor'
    ]
    
    for col in columnas_texto:
        if col in df.columns:
            # Solo procesar valores no nulos
            df[col] = df[col].apply(lambda x: str(x).strip() if pd.notna(x) and x != '' else None)
    
    # Reemplazar valores NaN con None en todo el DataFrame
    df = df.where(pd.notna(df), None)
    
    logging.info(f"✅ Datos limpiados. Registros totales: {len(df)}")
    return df

def cargar_excel_a_postgresql():
    # Parámetros de conexión PostgreSQL
    db_config = {
        "host": "localhost",
        "port": 5432,
        "database": "bd_calidda_fnb",
        "user": "postgres",
        "password": "ibr2025"
    }

    table_name = "bd_anulaciones"

    try:
        logging.info("=== INICIO DEL PROCESO DE CARGA DE DATOS A POSTGRESQL ===")
        
        # Seleccionar archivo
        excel_file = seleccionar_archivo()
        
        if not excel_file:
            logging.warning("⚠️ No se seleccionó ningún archivo. Proceso cancelado.")
            return
        
        logging.info(f"📂 Archivo seleccionado: {excel_file}")
        
        # Leer el archivo Excel
        logging.info("📖 Leyendo archivo Excel...")
        df = pd.read_excel(excel_file)
        logging.info(f"✅ Archivo Excel leído correctamente. Filas: {len(df)}, Columnas: {len(df.columns)}")
        
        # Limpiar y preparar datos
        df = limpiar_datos(df)
        
        # Conectar a PostgreSQL
        logging.info("🔌 Conectando a PostgreSQL...")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        logging.info("✅ Conexión a PostgreSQL establecida")
        
        # Crear tabla si no existe
        crear_tabla_bd_anulaciones(cursor)
        
        # Eliminar datos existentes en la tabla
        logging.info(f"🗑️ Eliminando datos existentes en la tabla {table_name}...")
        cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
        conn.commit()
        logging.info("✅ Tabla limpiada correctamente")
        
        # Preparar datos para inserción
        logging.info("📝 Preparando datos para inserción...")
        
        # Excluir la columna 'id' si existe en el DataFrame (se autogenera en la BD)
        if 'id' in df.columns:
            df = df.drop('id', axis=1)
        
        # Convertir DataFrame a lista de diccionarios
        registros = df.to_dict('records')
        
        # Obtener nombres de columnas
        columnas = list(df.columns)
        columnas_str = ", ".join(columnas)
        placeholders = ", ".join(["%s"] * len(columnas))
        insert_query = f"INSERT INTO {table_name} ({columnas_str}) VALUES ({placeholders})"
        
        # Insertar datos en lotes
        logging.info("⬆️ Insertando datos en PostgreSQL...")
        batch_size = 1000
        total_registros = len(registros)
        
        for i in range(0, total_registros, batch_size):
            batch_dicts = registros[i:i + batch_size]
            # Convertir cada diccionario a tupla, asegurando que None se mantenga como None
            batch_tuplas = []
            for registro in batch_dicts:
                tupla = []
                for col in columnas:
                    valor = registro[col]
                    # Asegurar que NaT, nan, y valores inválidos se conviertan a None
                    if pd.isna(valor) or valor == 'NaT' or valor == 'nan':
                        tupla.append(None)
                    else:
                        tupla.append(valor)
                batch_tuplas.append(tuple(tupla))
            
            extras.execute_batch(cursor, insert_query, batch_tuplas)
            conn.commit()
            logging.info(f"   ➤ Insertados {min(i + batch_size, total_registros)}/{total_registros} registros")
        
        logging.info(f"✅ Datos cargados exitosamente en la tabla {table_name}")
        
        # Verificar registros insertados
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        logging.info(f"📊 Total de registros en la tabla: {count}")
        
        # Cerrar conexiones
        cursor.close()
        conn.close()
        logging.info("🔒 Conexión cerrada")
        
        logging.info("=== PROCESO COMPLETADO EXITOSAMENTE ===")
        print(f"\n✅ ¡PROCESO COMPLETADO! Se cargaron {count} registros en la tabla {table_name}")
        
    except FileNotFoundError:
        logging.error(f"❌ ERROR: No se encontró el archivo")
    except pd.errors.EmptyDataError:
        logging.error("❌ ERROR: El archivo Excel está vacío")
    except psycopg2.Error as e:
        logging.error(f"❌ ERROR de PostgreSQL: {e}")
        if 'conn' in locals():
            conn.rollback()
    except Exception as e:
        logging.error(f"❌ ERROR inesperado: {e}")
        import traceback
        logging.error(traceback.format_exc())
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
        logging.info("🔒 Recursos liberados")

if __name__ == "__main__":
    cargar_excel_a_postgresql()
    input("\nPresione Enter para salir...")
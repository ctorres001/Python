import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text
import urllib

# Configuración de conexión
server = "192.168.64.250"
database = "BD_CALIDDA_FNB"
username = "ctorres"
password = "ibr2025"

# Ruta del archivo Excel
excel_path = r"D:\FNB\Reportes\04 Reporte Clientes Potenciales\Estado Rechazo.xlsx"
sheet_name = "Hoja1"
table_name = "BD_Potenciales_EstadosRechazo"

def crear_conexion():
    """Crea y retorna la conexión a SQL Server usando SQLAlchemy"""
    try:
        # Crear string de conexión para SQLAlchemy
        params = urllib.parse.quote_plus(
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'UID={username};'
            f'PWD={password}'
        )
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
        print("✓ Conexión establecida exitosamente")
        return engine
    except Exception as e:
        print(f"✗ Error al conectar a la base de datos: {e}")
        return None

def leer_excel():
    """Lee el archivo Excel y retorna un DataFrame"""
    try:
        df = pd.read_excel(excel_path, sheet_name=sheet_name)
        
        # Verificar que existan las columnas necesarias
        columnas_requeridas = ['Motivo', 'Motivo agrupado']
        if not all(col in df.columns for col in columnas_requeridas):
            print(f"✗ Error: El archivo debe contener las columnas: {columnas_requeridas}")
            return None
        
        # Seleccionar solo las columnas necesarias
        df = df[columnas_requeridas]
        
        # Convertir a tipo texto y limpiar espacios
        df['Motivo'] = df['Motivo'].astype(str).str.strip()
        df['Motivo agrupado'] = df['Motivo agrupado'].astype(str).str.strip()
        
        # Eliminar duplicados en el archivo de origen
        df_sin_duplicados = df.drop_duplicates()
        
        duplicados_removidos = len(df) - len(df_sin_duplicados)
        if duplicados_removidos > 0:
            print(f"⚠ Se eliminaron {duplicados_removidos} registros duplicados del archivo Excel")
        
        print(f"✓ Archivo Excel leído exitosamente: {len(df_sin_duplicados)} registros únicos")
        return df_sin_duplicados
    except FileNotFoundError:
        print(f"✗ Error: No se encontró el archivo en la ruta: {excel_path}")
        return None
    except Exception as e:
        print(f"✗ Error al leer el archivo Excel: {e}")
        return None

def tabla_existe(engine):
    """Verifica si la tabla existe en la base de datos"""
    try:
        with engine.connect() as conn:
            query = text(f"""
                SELECT COUNT(*) as existe
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = '{table_name}'
            """)
            result = conn.execute(query).fetchone()
            return result[0] > 0
    except Exception as e:
        print(f"✗ Error al verificar existencia de tabla: {e}")
        return False

def crear_tabla(engine):
    """Crea la tabla si no existe"""
    try:
        with engine.connect() as conn:
            query = text(f"""
                CREATE TABLE {table_name} (
                    ID INT IDENTITY(1,1) PRIMARY KEY,
                    Motivo NVARCHAR(255) NOT NULL,
                    [Motivo agrupado] NVARCHAR(255) NOT NULL,
                    FechaCreacion DATETIME DEFAULT GETDATE(),
                    CONSTRAINT UK_Motivo UNIQUE (Motivo, [Motivo agrupado])
                )
            """)
            conn.execute(query)
            conn.commit()
            print(f"✓ Tabla '{table_name}' creada exitosamente")
            return True
    except Exception as e:
        print(f"✗ Error al crear la tabla: {e}")
        return False

def obtener_datos_existentes(engine):
    """Obtiene los datos existentes de la tabla"""
    try:
        query = f"SELECT [Motivo], [Motivo agrupado] FROM {table_name}"
        df_existente = pd.read_sql(query, engine)
        print(f"✓ Se encontraron {len(df_existente)} registros en la tabla")
        return df_existente
    except Exception as e:
        print(f"✗ Error al leer datos existentes: {e}")
        return pd.DataFrame()

def cargar_datos_incrementales(engine, df_nuevos):
    """Realiza la carga incremental de datos"""
    try:
        # Obtener datos existentes
        df_existentes = obtener_datos_existentes(engine)
        
        if df_existentes.empty:
            # Si la tabla está vacía, cargar todos los datos
            datos_a_insertar = df_nuevos
            print("→ Tabla vacía: se cargarán todos los registros")
        else:
            # Identificar registros nuevos
            # Crear una columna auxiliar para comparación
            df_existentes['_key'] = df_existentes['Motivo'] + '|' + df_existentes['Motivo agrupado']
            df_nuevos['_key'] = df_nuevos['Motivo'] + '|' + df_nuevos['Motivo agrupado']
            
            # Filtrar solo los registros que no existen
            datos_a_insertar = df_nuevos[~df_nuevos['_key'].isin(df_existentes['_key'])]
            datos_a_insertar = datos_a_insertar.drop(columns=['_key'])
            
            registros_duplicados = len(df_nuevos) - len(datos_a_insertar)
            print(f"→ Registros duplicados (omitidos): {registros_duplicados}")
        
        # Insertar solo datos nuevos
        if len(datos_a_insertar) > 0:
            datos_a_insertar.to_sql(
                table_name,
                engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            print(f"✓ Se insertaron {len(datos_a_insertar)} registros nuevos exitosamente")
            return len(datos_a_insertar)
        else:
            print("✓ No hay registros nuevos para insertar")
            return 0
    except Exception as e:
        print(f"✗ Error al cargar datos: {e}")
        return -1

def main():
    """Función principal"""
    print("="*60)
    print("INICIO DEL PROCESO DE CARGA INCREMENTAL")
    print("="*60)
    
    # Paso 1: Leer archivo Excel
    print("\n[1/4] Leyendo archivo Excel...")
    df = leer_excel()
    if df is None or df.empty:
        print("\n✗ Proceso finalizado con errores")
        return
    
    # Paso 2: Conectar a la base de datos
    print("\n[2/4] Conectando a la base de datos...")
    engine = crear_conexion()
    if engine is None:
        print("\n✗ Proceso finalizado con errores")
        return
    
    # Paso 3: Verificar/Crear tabla
    print("\n[3/4] Verificando tabla...")
    if not tabla_existe(engine):
        print(f"→ La tabla '{table_name}' no existe. Creando...")
        if not crear_tabla(engine):
            print("\n✗ Proceso finalizado con errores")
            return
    else:
        print(f"✓ La tabla '{table_name}' ya existe")
    
    # Paso 4: Carga incremental
    print("\n[4/4] Realizando carga incremental...")
    registros_insertados = cargar_datos_incrementales(engine, df)
    
    # Resumen final
    print("\n" + "="*60)
    print("RESUMEN DEL PROCESO")
    print("="*60)
    print(f"Registros en Excel: {len(df)}")
    print(f"Registros insertados: {registros_insertados if registros_insertados >= 0 else 'Error'}")
    print("="*60)
    
    if registros_insertados >= 0:
        print("\n✓ Proceso completado exitosamente")
    else:
        print("\n✗ Proceso completado con errores")

if __name__ == "__main__":
    main()
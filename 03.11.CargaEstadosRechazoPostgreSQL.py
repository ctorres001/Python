import pandas as pd
import psycopg2
from psycopg2 import extras

# Configuración de conexión PostgreSQL
db_config = {
    "host": "localhost",
    "port": 5432,
    "database": "bd_calidda_fnb",
    "user": "postgres",
    "password": "ibr2025"
}

# Ruta del archivo Excel
excel_path = r"D:\FNB\Reportes\04 Reporte Clientes Potenciales\Estado Rechazo.xlsx"
sheet_name = "Hoja1"
table_name = "bd_potenciales_estados_rechazo"

def crear_tabla_bd_estados_rechazo(cursor):
    """Crea la tabla bd_potenciales_estados_rechazo si no existe"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS bd_potenciales_estados_rechazo (
        motivo VARCHAR(500) NOT NULL,
        motivo_agrupado VARCHAR(500) NOT NULL,
        UNIQUE (motivo, motivo_agrupado)
    )
    """
    cursor.execute(create_table_sql)
    print("✅ Tabla bd_potenciales_estados_rechazo verificada/creada")

def asegurar_indice_unico(conn):
    """Asegura que exista un índice/constraint único para (motivo, motivo_agrupado)."""
    try:
        cursor = conn.cursor()
        # Un índice único permite que ON CONFLICT (motivo, motivo_agrupado) funcione
        cursor.execute(
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_{table_name}_motivo_motivo_agrupado
            ON {table_name} (motivo, motivo_agrupado)
            """
        )
        conn.commit()
        cursor.close()
        print("✓ Índice único verificado/creado")
        return True
    except Exception as e:
        print(f"✗ Error al crear/verificar índice único: {e}")
        conn.rollback()
        return False

def crear_conexion():
    """Crea y retorna la conexión a PostgreSQL"""
    try:
        conn = psycopg2.connect(**db_config)
        print("✓ Conexión establecida exitosamente")
        return conn
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
        
        # Seleccionar solo las columnas necesarias y renombrar para PostgreSQL
        df = df[columnas_requeridas].copy()
        df.columns = ['motivo', 'motivo_agrupado']  # Renombrar a minúsculas
        
        # Convertir a tipo texto y limpiar espacios
        df['motivo'] = df['motivo'].astype(str).str.strip()
        df['motivo_agrupado'] = df['motivo_agrupado'].astype(str).str.strip()
        
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

def tabla_existe(conn):
    """Verifica si la tabla existe en la base de datos"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_name = %s
            )
        """, (table_name,))
        existe = cursor.fetchone()[0]
        cursor.close()
        return existe
    except Exception as e:
        print(f"✗ Error al verificar existencia de tabla: {e}")
        return False

def crear_tabla(conn):
    """Crea la tabla si no existe - ahora usa la función centralizada"""
    try:
        cursor = conn.cursor()
        # Usar la función que crea la tabla con IF NOT EXISTS
        crear_tabla_bd_estados_rechazo(cursor)
        conn.commit()
        cursor.close()
        return True
    except Exception as e:
        print(f"✗ Error al crear la tabla: {e}")
        conn.rollback()
        return False

def obtener_datos_existentes(conn):
    """Obtiene los datos existentes de la tabla"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT motivo, motivo_agrupado FROM {table_name}")
        resultados = cursor.fetchall()
        cursor.close()
        
        if resultados:
            df_existente = pd.DataFrame(resultados, columns=['motivo', 'motivo_agrupado'])
            print(f"✓ Se encontraron {len(df_existente)} registros en la tabla")
            return df_existente
        else:
            print("✓ La tabla está vacía")
            return pd.DataFrame(columns=['motivo', 'motivo_agrupado'])
    except Exception as e:
        print(f"✗ Error al leer datos existentes: {e}")
        return pd.DataFrame()

def cargar_datos_incrementales(conn, df_nuevos):
    """Realiza la carga incremental de datos"""
    try:
        # Obtener datos existentes
        df_existentes = obtener_datos_existentes(conn)
        
        if df_existentes.empty:
            # Si la tabla está vacía, cargar todos los datos
            datos_a_insertar = df_nuevos
            print("→ Tabla vacía: se cargarán todos los registros")
        else:
            # Identificar registros nuevos
            # Crear una columna auxiliar para comparación
            df_existentes['_key'] = df_existentes['motivo'] + '|' + df_existentes['motivo_agrupado']
            df_nuevos['_key'] = df_nuevos['motivo'] + '|' + df_nuevos['motivo_agrupado']
            
            # Filtrar solo los registros que no existen
            datos_a_insertar = df_nuevos[~df_nuevos['_key'].isin(df_existentes['_key'])].copy()
            datos_a_insertar = datos_a_insertar.drop(columns=['_key'])
            
            registros_duplicados = len(df_nuevos) - len(datos_a_insertar)
            print(f"→ Registros duplicados (omitidos): {registros_duplicados}")
        
        # Insertar solo datos nuevos usando execute_values (más eficiente)
        if len(datos_a_insertar) > 0:
            cursor = conn.cursor()
            
            # Preparar valores para inserción
            valores = [tuple(row) for row in datos_a_insertar[['motivo', 'motivo_agrupado']].values]
            
            # Usar execute_values para inserción en lote
            insert_sql = f"""
                INSERT INTO {table_name} (motivo, motivo_agrupado) 
                VALUES %s
                ON CONFLICT (motivo, motivo_agrupado) DO NOTHING
            """
            extras.execute_values(cursor, insert_sql, valores)
            
            conn.commit()
            cursor.close()
            
            print(f"✓ Se insertaron {len(datos_a_insertar)} registros nuevos exitosamente")
            return len(datos_a_insertar)
        else:
            print("✓ No hay registros nuevos para insertar")
            return 0
    except Exception as e:
        print(f"✗ Error al cargar datos: {e}")
        conn.rollback()
        return -1

def main():
    """Función principal"""
    print("="*60)
    print("INICIO DEL PROCESO DE CARGA INCREMENTAL - POSTGRESQL")
    print("="*60)
    
    # Paso 1: Leer archivo Excel
    print("\n[1/4] Leyendo archivo Excel...")
    df = leer_excel()
    if df is None or df.empty:
        print("\n✗ Proceso finalizado con errores")
        return
    
    # Paso 2: Conectar a la base de datos
    print("\n[2/4] Conectando a la base de datos PostgreSQL...")
    conn = crear_conexion()
    if conn is None:
        print("\n✗ Proceso finalizado con errores")
        return
    
    # Paso 3: Verificar/Crear tabla
    print("\n[3/4] Verificando tabla...")
    if not tabla_existe(conn):
        print(f"→ La tabla '{table_name}' no existe. Creando...")
        if not crear_tabla(conn):
            print("\n✗ Proceso finalizado con errores")
            conn.close()
            return
    else:
        print(f"✓ La tabla '{table_name}' ya existe")

    # Asegurar índice único requerido por ON CONFLICT
    if not asegurar_indice_unico(conn):
        print("\n✗ Proceso finalizado con errores (índice único)")
        conn.close()
        return
    
    # Paso 4: Carga incremental
    print("\n[4/4] Realizando carga incremental...")
    registros_insertados = cargar_datos_incrementales(conn, df)
    
    # Cerrar conexión
    conn.close()
    
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

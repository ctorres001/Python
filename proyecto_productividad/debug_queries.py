"""
Script de debug para verificar si los datos se están guardando en BD
Ejecutar desde la raíz del proyecto: python debug_queries.py
"""

import os
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import toml

def get_direct_connection():
    """
    Conecta directamente a la BD sin usar Streamlit.
    Lee credenciales de .streamlit/secrets.toml
    """
    import toml
    
    # Leer secrets.toml
    secrets_path = ".streamlit/secrets.toml"
    
    if not os.path.exists(secrets_path):
        print(f"❌ ERROR: No se encontró {secrets_path}")
        return None
    
    try:
        with open(secrets_path, "r") as f:
            secrets = toml.load(f)
        
        # Debug: mostrar todas las claves disponibles
        print(f"🔍 Claves encontradas en secrets.toml: {list(secrets.keys())}\n")
        
        # Buscar la URL de BD con diferentes nombres posibles
        database_url = None
        
        # Streamlit connections pattern
        if "connections" in secrets and "neon_db" in secrets["connections"]:
            database_url = secrets["connections"]["neon_db"].get("url")
        
        # Otras opciones
        if not database_url:
            database_url = (
                secrets.get("database_url") or 
                secrets.get("DATABASE_URL") or
                secrets.get("db_url") or
                secrets.get("DB_URL") or
                secrets.get("postgres_url") or
                secrets.get("POSTGRES_URL")
            )
        
        if not database_url:
            print("❌ ERROR: No se encontró la URL de BD en secrets.toml")
            print(f"   Claves disponibles: {list(secrets.keys())}")
            print("   Verifica que tengas una clave como: database_url, DATABASE_URL, etc.")
            return None
        
        # Ocultar credenciales en el mensaje
        display_url = database_url.split("@")[1] if "@" in database_url else database_url
        print(f"📌 Conectando a BD: ...@{display_url}")
        
        engine = create_engine(database_url)
        connection = engine.connect()
        print("✅ Conexión a BD exitosa\n")
        return connection
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None

def check_registros():
    """Verifica qué hay en la tabla registro_actividades"""
    conn = get_direct_connection()
    
    if not conn:
        return
    
    print("=" * 60)
    print("VERIFICANDO TABLA registro_actividades")
    print("=" * 60)
    
    try:
        # 1. Contar registros totales
        result = conn.execute(text("SELECT COUNT(*) as total FROM registro_actividades"))
        total = result.fetchone()[0]
        print(f"\n✅ Total de registros: {total}")
        
        # 2. Ver últimos 10 registros
        query = """
            SELECT 
                r.id, 
                r.usuario_id, 
                r.actividad_id, 
                a.nombre_actividad,
                r.fecha, 
                r.hora_inicio, 
                r.hora_fin,
                r.estado,
                r.duracion_seg
            FROM registro_actividades r
            LEFT JOIN actividades a ON r.actividad_id = a.id
            ORDER BY r.id DESC 
            LIMIT 10
        """
        df_recent = pd.read_sql_query(text(query), conn)
        
        print("\n📋 Últimos 10 registros:")
        print(df_recent.to_string())
        
        # 3. Ver registros de hoy
        query_today = """
            SELECT 
                r.id,
                r.usuario_id,
                a.nombre_actividad,
                r.hora_inicio,
                r.hora_fin,
                r.duracion_seg
            FROM registro_actividades r
            LEFT JOIN actividades a ON r.actividad_id = a.id
            WHERE r.fecha = CURRENT_DATE
            ORDER BY r.hora_inicio DESC
        """
        df_today = pd.read_sql_query(text(query_today), conn)
        
        print(f"\n📅 Registros de hoy: {len(df_today)}")
        if not df_today.empty:
            print(df_today.to_string())
        else:
            print("❌ Sin registros para hoy")
        
        # 4. Ver usuarios
        print("\n👥 Verificando usuarios:")
        df_users = pd.read_sql_query(
            text("SELECT id, nombre_usuario, nombre_completo FROM usuarios LIMIT 5"),
            conn
        )
        print(df_users.to_string())
        
        # 5. Ver actividades
        print("\n🎯 Verificando actividades:")
        df_acts = pd.read_sql_query(
            text("SELECT id, nombre_actividad, activo FROM actividades ORDER BY id"),
            conn
        )
        print(df_acts.to_string())
        
        # 6. Verificar zona horaria
        print("\n🕐 Verificando zona horaria de BD:")
        tz_result = pd.read_sql_query(
            text("SELECT CURRENT_DATE, CURRENT_TIMESTAMP, NOW()"),
            conn
        )
        print(tz_result.to_string())
        
    except Exception as e:
        print(f"❌ Error en consulta: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    check_registros()
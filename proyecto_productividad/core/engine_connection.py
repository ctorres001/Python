# core/engine_connection.py
"""
Conexión SQLAlchemy con configuración de timezone para Neon PostgreSQL
IMPORTANTE: Neon usa GMT por defecto, necesitamos forzar America/Lima
"""
from sqlalchemy import create_engine, event, text
from sqlalchemy.pool import NullPool
import streamlit as st

@st.cache_resource
def get_engine():
    """
    Crea una conexión SQLAlchemy directa al pooler Neon.
    Configurado para usar zona horaria America/Lima (Perú - GMT-5)
    """
    url = st.secrets["connections"]["neon_db"]["url"]
    
    # ESTRATEGIA 1: Agregar timezone en los parámetros de conexión
    # Esto funciona incluso si Neon está en región Sao Paulo
    connect_args = {
        "options": "-c timezone=America/Lima"
    }
    
    # ESTRATEGIA 2: También agregarlo a la URL como backup
    if '?' in url:
        # Ya tiene parámetros, agregar con &
        if 'options=' not in url:
            url += '&options=-c%20timezone%3DAmerica/Lima'
    else:
        # No tiene parámetros, agregar con ?
        url += '?options=-c%20timezone%3DAmerica/Lima'
    
    # Crear engine optimizado para Neon
    engine = create_engine(
        url,
        pool_pre_ping=True,      # Verifica que la conexión esté viva
        pool_recycle=3600,       # Recicla conexiones cada hora
        echo=False,              # No mostrar SQL en logs (cambiar a True para debug)
        connect_args=connect_args
    )
    
    # ESTRATEGIA 3: Event listener que se ejecuta en CADA conexión
    @event.listens_for(engine, "connect")
    def set_timezone(dbapi_conn, connection_record):
        """
        Se ejecuta cada vez que se crea una nueva conexión.
        Fuerza el timezone a America/Lima sin importar la configuración de Neon.
        """
        cursor = dbapi_conn.cursor()
        try:
            cursor.execute("SET TIME ZONE 'America/Lima'")
        except Exception as e:
            print(f"⚠️ Advertencia al configurar timezone: {e}")
        finally:
            cursor.close()
    
    # ESTRATEGIA 4: Verificar en la primera conexión
    @event.listens_for(engine, "first_connect")
    def verify_timezone(dbapi_conn, connection_record):
        """
        Verifica el timezone al conectar por primera vez.
        Muestra advertencia si no es America/Lima.
        """
        cursor = dbapi_conn.cursor()
        try:
            cursor.execute("SHOW timezone")
            tz = cursor.fetchone()[0]
            
            if tz == 'America/Lima':
                print(f"✅ Timezone configurado correctamente: {tz}")
            else:
                print(f"⚠️ Timezone actual: {tz} (esperado: America/Lima)")
                # Intentar corregir
                cursor.execute("SET TIME ZONE 'America/Lima'")
                print("✅ Timezone corregido a America/Lima")
                
        except Exception as e:
            print(f"⚠️ Error verificando timezone: {e}")
        finally:
            cursor.close()
    
    return engine


def get_current_timezone():
    """
    Función de utilidad para verificar el timezone actual.
    Úsala para debugging.
    
    Returns:
        str: Timezone actual (ej: 'America/Lima', 'GMT', etc.)
    """
    engine = get_engine()
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SHOW timezone"))
            return result.scalar()
    except Exception as e:
        return f"Error: {e}"


def get_current_db_time():
    """
    Función de utilidad para verificar la hora actual de la base de datos.
    Úsala para debugging.
    
    Returns:
        datetime: Hora actual en la base de datos
    """
    engine = get_engine()
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT NOW()"))
            return result.scalar()
    except Exception as e:
        return f"Error: {e}"


def test_timezone_configuration():
    """
    Función de prueba completa para verificar configuración de timezone.
    Llámala desde app.py durante desarrollo.
    
    Returns:
        dict: Resultados de las pruebas
    """
    results = {
        "timezone": None,
        "db_time": None,
        "utc_time": None,
        "offset": None,
        "status": "error"
    }
    
    engine = get_engine()
    try:
        with engine.connect() as conn:
            # Obtener timezone
            tz_result = conn.execute(text("SHOW timezone"))
            results["timezone"] = tz_result.scalar()
            
            # Obtener hora de la BD
            time_result = conn.execute(text("SELECT NOW()"))
            results["db_time"] = time_result.scalar()
            
            # Obtener hora UTC
            utc_result = conn.execute(text("SELECT NOW() AT TIME ZONE 'UTC'"))
            results["utc_time"] = utc_result.scalar()
            
            # Calcular offset
            offset_result = conn.execute(text("""
                SELECT EXTRACT(TIMEZONE FROM NOW()) / 3600 as offset_hours
            """))
            results["offset"] = offset_result.scalar()
            
            # Verificar si es correcto
            if results["timezone"] == "America/Lima" and results["offset"] == -5:
                results["status"] = "success"
            else:
                results["status"] = "warning"
                
    except Exception as e:
        results["error"] = str(e)
    
    return results


# Para debugging: descomentar estas líneas
if __name__ == "__main__":
    print("🔍 Probando configuración de timezone...")
    results = test_timezone_configuration()
    print(f"Timezone: {results.get('timezone')}")
    print(f"Hora DB: {results.get('db_time')}")
    print(f"Hora UTC: {results.get('utc_time')}")
    print(f"Offset: {results.get('offset')} horas")
    print(f"Status: {results.get('status')}")
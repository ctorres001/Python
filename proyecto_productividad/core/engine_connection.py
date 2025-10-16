from sqlalchemy import create_engine, event
import streamlit as st

@st.cache_resource
def get_engine():
    """Crea una conexión SQLAlchemy directa al pooler Neon con zona horaria de Lima."""
    url = st.secrets["connections"]["neon_db"]["url"]
    engine = create_engine(url, pool_pre_ping=True)
    
    # Configurar zona horaria de Lima para todas las conexiones
    @event.listens_for(engine, "connect")
    def set_timezone(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("SET TIME ZONE 'America/Lima'")
        cursor.close()
    
    return engine
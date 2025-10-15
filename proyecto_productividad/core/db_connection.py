import streamlit as st

@st.cache_resource
def get_db_connection():
    """
    Establece y devuelve una conexión a la base de datos Neon 
    utilizando las credenciales de st.secrets.
    
    Retorna un objeto SQLConnection de Streamlit que tiene:
    - .query() para SELECT
    - .engine para acceso directo a SQLAlchemy
    """
    try:
        # st.connection maneja el pooling y la reconexión automáticamente.
        conn = st.connection("neon_db", type="sql")
        return conn
    except Exception as e:
        st.error(f"Error al conectar a la base de datos: {e}")
        return None
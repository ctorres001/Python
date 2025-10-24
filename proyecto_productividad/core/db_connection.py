import streamlit as st

@st.cache_resource
def get_db_connection():
    """
    Establece conexión a Neon PostgreSQL compatible con Streamlit Cloud.
    Soporta múltiples formatos de secrets.
    """
    try:
        # MÉTODO 1: Intentar con formato connections.neon_db
        if "connections" in st.secrets and "neon_db" in st.secrets["connections"]:
            conn = st.connection("neon_db", type="sql")
            return conn
        
        # MÉTODO 2: Intentar con URL directa
        elif "neon_db" in st.secrets and "url" in st.secrets["neon_db"]:
            url = st.secrets["neon_db"]["url"]
            conn = st.connection(
                "neon_db",
                type="sql",
                url=url
            )
            return conn
        
        # MÉTODO 3: Construir URL desde componentes separados
        elif "neon_db" in st.secrets:
            db_config = st.secrets["neon_db"]
            
            # Construir URL de conexión
            url = (
                f"postgresql://{db_config['user']}:{db_config['password']}"
                f"@{db_config['host']}:{db_config.get('port', '5432')}"
                f"/{db_config['database']}"
                f"?sslmode={db_config.get('sslmode', 'require')}"
            )
            
            conn = st.connection(
                "neon_db",
                type="sql",
                url=url
            )
            return conn
        
        else:
            st.error("❌ No se encontró configuración de base de datos en secrets.")
            st.info("""
            **Configura secrets en Streamlit Cloud:**
            
            Settings → Secrets → Pega esto:
            
            ```toml
            [neon_db]
            host = "tu-host.neon.tech"
            database = "ControldeActividades"
            user = "neondb_owner"
            password = "tu-password"
            port = "5432"
            sslmode = "require"
            ```
            """)
            return None
            
    except Exception as e:
        st.error(f"❌ Error al conectar a la base de datos: {e}")
        st.info("Verifica que los secrets estén correctamente configurados en Streamlit Cloud.")
        return None
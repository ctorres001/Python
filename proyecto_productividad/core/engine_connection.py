from sqlalchemy import create_engine, text
import streamlit as st

@st.cache_resource
def get_engine():
    """Crea una conexión SQLAlchemy directa al pooler Neon."""
    url = st.secrets["connections"]["neon_db"]["url"]
    engine = create_engine(url, pool_pre_ping=True)
    return engine

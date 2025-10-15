import re
import streamlit as st
from core.queries import get_user_by_username

def is_strong_password(password: str) -> bool:
    """
    Verifica si una contraseña cumple los criterios mínimos:
    - Al menos 8 caracteres
    - Contiene mayúsculas, minúsculas, números y símbolos
    """
    if len(password) < 8:
        return False
    if not re.search(r"[A-Z]", password):  # mayúsculas
        return False
    if not re.search(r"[a-z]", password):  # minúsculas
        return False
    if not re.search(r"[0-9]", password):  # números
        return False
    if not re.search(r"[^A-Za-z0-9]", password):  # símbolos
        return False
    return True


def authenticate_user(conn, username: str, password: str):
    """
    Valida usuario y contraseña contra la base de datos.
    Devuelve dict con datos del usuario si es válido, None si no.
    """
    user = get_user_by_username(conn, username)

    if not user:
        return None

    # ✅ Comparación de contraseña en texto plano
    if user['contraseña'] != password:
        return None

    if not user['estado']:
        st.warning("⚠️ Tu usuario está inactivo. Contacta al administrador.")
        return None

    # Validación adicional opcional (solo aviso, no bloquea)
    if not is_strong_password(password):
        st.info("🔐 La contraseña actual no cumple los requisitos de seguridad mínimos.")
        st.info("Por favor, contacta al administrador para actualizarla.")
        
    return user


def login_user(user: dict):
    """Guarda la información del usuario en la sesión de Streamlit."""
    st.session_state['logged_in'] = True
    st.session_state['user_info'] = user


def logout_user():
    """Cierra sesión y limpia el estado."""
    st.session_state['logged_in'] = False
    st.session_state.pop('user_info', None)
import re
import streamlit as st
from sqlalchemy import text
from core.queries import get_user_by_username
from core.engine_connection import get_engine


# ======================================================
# 🔐 VALIDACIÓN DE CONTRASEÑAS
# ======================================================

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


# ======================================================
# 👤 AUTENTICACIÓN DE USUARIO
# ======================================================

def authenticate_user(conn, username: str, password: str):
    """
    Valida usuario y contraseña contra la base de datos.
    Devuelve dict con datos del usuario si es válido, None si no.
    """
    user = get_user_by_username(conn, username)

    if not user:
        return None

    # ✅ Comparación directa (sin hash)
    if user['contraseña'] != password:
        return None

    # ✅ Validación de estado
    if not user['estado']:
        st.warning("⚠️ Tu usuario está inactivo. Contacta al administrador.")
        return None

    # ✅ Advertencia si la contraseña es débil (solo aviso)
    if not is_strong_password(password):
        st.info("🔐 Tu contraseña no cumple los requisitos mínimos de seguridad.")
        st.info("Por favor, contacta al administrador para actualizarla.")

    return user


# ======================================================
# 💾 REGISTRO / CREACIÓN DIRECTA (para admins)
# ======================================================

def register_user(username: str, password: str, nombre_completo: str, rol_id: int, campaña_id: int):
    """
    Crea un nuevo usuario directamente en la base de datos.
    Se usa motor SQLAlchemy para garantizar persistencia en Neon.
    """
    engine = get_engine()
    try:
        with engine.begin() as connection:
            connection.execute(
                text("""
                    INSERT INTO public.usuarios 
                    (nombre_usuario, contraseña, nombre_completo, rol_id, campaña_id, estado)
                    VALUES (:u, :p, :nc, :ri, :ci, TRUE)
                """),
                {"u": username, "p": password, "nc": nombre_completo, "ri": rol_id, "ci": campaña_id}
            )
        return True
    except Exception as e:
        st.error(f"Error al registrar usuario: {str(e)}")
        return False


# ======================================================
# 🧭 GESTIÓN DE SESIÓN
# ======================================================

def login_user(user: dict):
    """Guarda la información del usuario en la sesión de Streamlit."""
    st.session_state['logged_in'] = True
    st.session_state['user_info'] = user


def logout_user():
    """Cierra sesión y limpia el estado."""
    st.session_state['logged_in'] = False
    st.session_state.pop('user_info', None)

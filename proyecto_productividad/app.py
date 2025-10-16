import streamlit as st
from core import auth, queries
from core.db_connection import get_db_connection
from views import login_view, asesor_view, supervisor_view, admin_view
import streamlit as st

# ==============================
# ⚙️ CONFIGURACIÓN INICIAL
# ==============================
st.set_page_config(
    page_title="Control de Productividad",
    page_icon="⏱️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Agregar esto:
hide_streamlit_style = """
    <style>
        /* Ocultar menú principal, footer y encabezado */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}

        /* Ocultar botones de acción genéricos */
        .stActionButton {visibility: hidden;}

        /* Ocultar enlace a GitHub si aparece */
        .st-emotion-cache-1avcm0n {display: none;}

        /* Ocultar botón "Manage app" */
        [data-testid="manage-app-button"] {
            display: none !important;
        }
    </style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

# ==============================
# 🔐 ESTADO DE SESIÓN
# ==============================
def init_session_state():
    """Inicializa las variables de sesión por defecto."""
    defaults = {
        'logged_in': False,
        'user_info': None,
        'current_activity_id': None,
        'current_activity_name': "---",
        'current_start_time': None,
        'current_registro_id': None
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

init_session_state()

# ==============================
# 🗄️ CONEXIÓN A BASE DE DATOS
# ==============================
db_conn = get_db_connection()

if not db_conn:
    st.error("❌ Error crítico de conexión a la base de datos. La aplicación no puede continuar.")
    st.stop()

# ==============================
# 🚪 BOTÓN DE CIERRE DE SESIÓN
# ==============================
if st.session_state['logged_in']:
    with st.sidebar:
        user = st.session_state['user_info']
        st.markdown(f"👤 **{user['nombre_completo']}**")
        st.markdown(f"🔐 Rol: **{user['rol_nombre']}**")
        
        if st.button("Cerrar sesión"):
            auth.logout_user()
            st.success("Sesión cerrada correctamente.")
            st.rerun()

# ==============================
# 🧭 RUTEO PRINCIPAL
# ==============================
if not st.session_state['logged_in']:
    # Mostramos la vista del login
    login_view.show_login_view()
else:
    # Determinamos el rol del usuario
    role = st.session_state['user_info']['rol_nombre']

    if role == 'Asesor':
        asesor_view.show_asesor_dashboard(db_conn)
    elif role == 'Supervisor':
        supervisor_view.show_supervisor_dashboard(db_conn)
    elif role == 'Administrador':
        admin_view.show_admin_dashboard(db_conn)
    else:
        st.error("⚠️ Rol de usuario no reconocido. Contacte al administrador.")
        if st.button("Salir"):
            auth.logout_user()
            st.rerun()
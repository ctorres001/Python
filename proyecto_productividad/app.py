import streamlit as st
from core import auth, queries
from core.db_connection import get_db_connection
from views import login_view, asesor_view, supervisor_view, admin_view

# ==============================
# 🔒 INICIALIZACIÓN TEMPRANA
# ==============================
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False

# ==============================
# ⚙️ CONFIGURACIÓN DE PÁGINA
# ==============================
sidebar_state = "collapsed" if not st.session_state['logged_in'] else "expanded"

st.set_page_config(
    page_title="Control de Productividad",
    page_icon="⏱️",
    layout="wide",
    initial_sidebar_state=sidebar_state
)

# ==============================
# 🎨 CSS GLOBAL - OCULTAR STREAMLIT
# ==============================
hide_streamlit_style = """
    <style>
        /* ============================================ */
        /* OCULTAR ELEMENTOS DE STREAMLIT */
        /* ============================================ */
        
        /* Menú principal */
        #MainMenu {
            visibility: hidden !important;
            display: none !important;
        }
        
        /* Footer */
        footer {
            visibility: hidden !important;
            display: none !important;
        }
        
        /* Header */
        header {
            visibility: hidden !important;
            display: none !important;
        }
        
        /* Botón "Manage app" y toolbar */
        [data-testid="manage-app-button"],
        button[kind="header"],
        [data-testid="stToolbar"],
        [data-testid="stDecoration"],
        [data-testid="stStatusWidget"],
        .stDeployButton,
        [data-testid="stToolbarActions"] {
            display: none !important;
            visibility: hidden !important;
        }
        
        /* Link a GitHub */
        .st-emotion-cache-1avcm0n {
            display: none !important;
        }
        
        /* Spinner de queries */
        [data-testid="stSpinner"] > div {
            display: none !important;
        }
        
        /* Banner "Running..." */
        [data-testid="stNotification"] {
            display: none !important;
        }
        
        /* Mensaje de estado */
        .stAlert[data-baseweb="notification"] {
            display: none !important;
        }
        
        /* Botones de acción */
        .stActionButton {
            visibility: hidden !important;
        }
        
        /* ============================================ */
        /* SIDEBAR FORZADO CUANDO LOGGED IN */
        /* ============================================ */
        ${sidebar_css}
    </style>
"""

# CSS condicional para sidebar
sidebar_css = ""
if st.session_state['logged_in']:
    sidebar_css = """
        [data-testid="stSidebar"] {
            display: block !important;
            visibility: visible !important;
        }
        
        [data-testid="collapsedControl"] {
            display: block !important;
        }
    """

# Inyectar CSS
st.markdown(hide_streamlit_style.replace("${sidebar_css}", sidebar_css), unsafe_allow_html=True)

# ==============================
# 🔄 ESTADO DE SESIÓN
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
        
        # Header del sidebar
        st.markdown("""
        <div style="text-align: center; padding: 1rem; background: linear-gradient(135deg, #667eea, #764ba2); border-radius: 10px; margin-bottom: 1rem;">
            <div style="font-size: 2rem; margin-bottom: 0.5rem;">👤</div>
            <div style="color: white; font-weight: 600; font-size: 1.1rem;">""" + user['nombre_completo'] + """</div>
            <div style="color: rgba(255,255,255,0.8); font-size: 0.9rem;">""" + user['rol_nombre'] + """</div>
        </div>
        """, unsafe_allow_html=True)
        
        # Botón de cierre de sesión
        if st.button("🚪 Cerrar Sesión", use_container_width=True, type="primary"):
            auth.logout_user()
            st.success("Sesión cerrada correctamente.")
            st.rerun()
        
        st.divider()
        
        # Información adicional
        st.markdown("### 📊 Información")
        st.info(f"**Campaña:** {user.get('campaña_nombre', 'N/A')}")

# ==============================
# 🧭 RUTEO PRINCIPAL
# ==============================
if not st.session_state['logged_in']:
    # Vista de login
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
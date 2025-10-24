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
# 🎨 CSS GLOBAL - OCULTAR STREAMLIT Y MEJORAR SIDEBAR
# ==============================
hide_streamlit_style = """
    <style>
        /* ============================================ */
        /* OCULTAR ELEMENTOS DE STREAMLIT Y GITHUB */
        /* ============================================ */
        
        /* Menú principal */
        #MainMenu {
            visibility: hidden !important;
            display: none !important;
        }
        
        /* Footer "Made with Streamlit" */
        footer {
            visibility: hidden !important;
            display: none !important;
        }
        
        /* Footer alternativo */
        footer:after {
            content: none !important;
            display: none !important;
        }
        
        /* Header superior */
        header {
            visibility: hidden !important;
            display: none !important;
        }
        
        /* Todos los botones del header */
        [data-testid="stHeader"] {
            display: none !important;
        }
        
        /* Botón "Deploy" / "Manage app" */
        [data-testid="manage-app-button"],
        button[kind="header"],
        [data-testid="stToolbar"],
        [data-testid="stDecoration"],
        [data-testid="stStatusWidget"],
        .stDeployButton,
        [data-testid="stToolbarActions"],
        [data-testid="stStatusWidget"] > div,
        [data-testid="collapsedControl"] + div {
            display: none !important;
            visibility: hidden !important;
        }
        
        /* Link a GitHub (múltiples selectores) */
        .st-emotion-cache-1avcm0n,
        [href*="github.com"],
        a[href*="github.com"],
        .viewerBadge_container__1QSob,
        .styles_viewerBadge__1yB5_,
        .viewerBadge_link__1S137,
        .viewerBadge_text__1JaDK {
            display: none !important;
            visibility: hidden !important;
        }
        
        /* Botones de acción genéricos */
        .stActionButton {
            visibility: hidden !important;
        }
        
        /* Banner "Running..." y spinners */
        [data-testid="stNotification"],
        [data-testid="stSpinner"] > div,
        .stAlert[data-baseweb="notification"] {
            display: none !important;
        }
        
        /* ============================================ */
        /* MEJORAS DEL SIDEBAR */
        /* ============================================ */
        
        /* Sidebar siempre visible cuando logged in */
        [data-testid="stSidebar"][aria-expanded="true"] {
            min-width: 280px !important;
            max-width: 280px !important;
        }
        
        [data-testid="stSidebar"][aria-expanded="false"] {
            min-width: 0px !important;
            max-width: 0px !important;
            margin-left: -280px;
        }
        
        /* Botón de colapsar/expandir mejorado */
        [data-testid="collapsedControl"] {
            display: block !important;
            position: fixed !important;
            top: 1rem !important;
            left: 1rem !important;
            z-index: 999999 !important;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%) !important;
            color: white !important;
            border-radius: 50% !important;
            width: 3rem !important;
            height: 3rem !important;
            display: flex !important;
            align-items: center !important;
            justify-content: center !important;
            box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4) !important;
            cursor: pointer !important;
            transition: all 0.3s ease !important;
        }
        
        [data-testid="collapsedControl"]:hover {
            transform: scale(1.1) !important;
            box-shadow: 0 6px 16px rgba(102, 126, 234, 0.6) !important;
        }
        
        /* Icono del botón de sidebar */
        [data-testid="collapsedControl"] svg {
            width: 1.5rem !important;
            height: 1.5rem !important;
            color: white !important;
        }
        
        /* Estilos del sidebar */
        [data-testid="stSidebar"] > div:first-child {
            background: linear-gradient(180deg, #f8f9fa 0%, #e9ecef 100%);
            border-right: 1px solid #dee2e6;
        }
        
        /* Ajustar contenido cuando sidebar está colapsado */
        [data-testid="stSidebar"][aria-expanded="false"] ~ [data-testid="stAppViewContainer"] {
            margin-left: 0 !important;
        }
        
        /* ============================================ */
        /* MEJORAS GENERALES */
        /* ============================================ */
        
        /* Mejorar transiciones */
        * {
            transition: all 0.2s ease;
        }
        
        /* Espaciado superior cuando no hay header */
        [data-testid="stAppViewContainer"] {
            padding-top: 1rem;
        }
    </style>
"""

st.markdown(hide_streamlit_style, unsafe_allow_html=True)

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
        
        # Header del sidebar con diseño mejorado
        st.markdown("""
        <div style="text-align: center; padding: 1.5rem; background: linear-gradient(135deg, #667eea, #764ba2); 
                    border-radius: 12px; margin-bottom: 1.5rem; box-shadow: 0 4px 12px rgba(102, 126, 234, 0.3);">
            <div style="font-size: 3rem; margin-bottom: 0.5rem;">👤</div>
            <div style="color: white; font-weight: 700; font-size: 1.2rem; margin-bottom: 0.25rem;">""" + user['nombre_completo'] + """</div>
            <div style="color: rgba(255,255,255,0.9); font-size: 0.95rem; font-weight: 500;">""" + user['rol_nombre'] + """</div>
        </div>
        """, unsafe_allow_html=True)
        
        # Información adicional
        st.markdown("### 📊 Información")
        st.info(f"**Campaña:** {user.get('campaña_nombre', 'N/A')}")
        
        st.divider()
        
        # Botón de cierre de sesión con mejor diseño
        if st.button("🚪 Cerrar Sesión", use_container_width=True, type="primary"):
            auth.logout_user()
            st.success("Sesión cerrada correctamente.")
            st.rerun()
        
        # Footer del sidebar
        st.markdown("""
        <div style="position: absolute; bottom: 1rem; left: 1rem; right: 1rem; 
                    text-align: center; color: #6c757d; font-size: 0.75rem;">
            <p style="margin: 0;">iBR - Control de Actividades</p>
            <p style="margin: 0;">v1.0.0</p>
        </div>
        """, unsafe_allow_html=True)

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
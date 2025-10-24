# ============================================
# views/login_view.py - Versión Corregida
# ============================================
import streamlit as st
import os
import base64
from core import auth
from core.db_connection import get_db_connection

def get_image_as_base64(image_path):
    """Convierte una imagen a base64"""
    try:
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode()
    except:
        return None

def show_login_view():
    """Vista de inicio de sesión con diseño empresarial"""

    # --- CSS Completo para ocultar Streamlit y crear login personalizado ---
    st.markdown("""
    <style>
        /* Reset básico */
        html, body {
            margin: 0;
            padding: 0;
            background: white;
            font-family: 'Segoe UI', sans-serif;
        }

        /* Ocultar elementos nativos de Streamlit */
        header, footer, [data-testid="stSidebar"], [data-testid="stHeader"] {
            display: none !important;
        }

        /* Ocultar toolbar y botones de Streamlit */
        [data-testid="stToolbar"],
        [data-testid="stDecoration"],
        [data-testid="stStatusWidget"],
        .stDeployButton,
        button[kind="header"] {
            display: none !important;
        }

        /* Contenedor principal */
        [data-testid="block-container"] {
            padding: 0 !important;
            max-width: 100% !important;
        }

        /* Ocultar todo el contenido por defecto (evita duplicación) */
        [data-testid="stVerticalBlock"] > [data-testid="element-container"] {
            display: none;
        }

        /* Mostrar solo el contenedor del login */
        .login-wrapper {
            display: flex !important;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            min-height: 100vh;
            padding: 2rem;
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        }

        /* Logo y título */
        .login-header {
            text-align: center;
            margin-bottom: 2rem;
        }

        .logo-img {
            width: 80px;
            height: 80px;
            margin-bottom: 1rem;
        }

        .login-title {
            font-size: 1.8rem;
            font-weight: bold;
            color: #1f2937;
            margin-top: 1rem;
        }

        /* Tarjeta de login */
        .login-card {
            background: white;
            border-radius: 16px;
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.1);
            padding: 2.5rem;
            width: 100%;
            max-width: 400px;
        }

        /* Hacer visibles los inputs del formulario */
        .login-card [data-testid="stVerticalBlock"] > [data-testid="element-container"] {
            display: block !important;
        }

        /* Estilo de inputs */
        .stTextInput > div > div > input {
            border-radius: 8px;
            border: 2px solid #e5e7eb;
            padding: 0.75rem;
            font-size: 1rem;
            transition: all 0.3s ease;
        }

        .stTextInput > div > div > input:focus {
            border-color: #667eea;
            box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
        }

        /* Botón de login */
        .stButton > button {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            border-radius: 10px;
            padding: 0.875rem 2rem;
            font-weight: 600;
            font-size: 1rem;
            cursor: pointer;
            transition: all 0.3s ease;
            width: 100%;
            margin-top: 1rem;
        }

        .stButton > button:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 20px rgba(102, 126, 234, 0.3);
        }

        /* Labels */
        .stTextInput > label {
            font-weight: 600;
            color: #374151;
            margin-bottom: 0.5rem;
        }

        /* Footer */
        .login-footer {
            text-align: center;
            margin-top: 2rem;
            color: #6b7280;
            font-size: 0.875rem;
        }
    </style>
    """, unsafe_allow_html=True)

    # --- Obtener imagen en base64 ---
    logo_path = os.path.join("assets", "ibr_logo.png")
    logo_base64 = get_image_as_base64(logo_path)
    
    if logo_base64:
        logo_html = f'<img src="data:image/png;base64,{logo_base64}" alt="iBR" class="logo-img">'
    else:
        logo_html = '<div style="font-size:3rem;font-weight:700;color:#667eea;">iBR</div>'

    # --- Estructura del login ---
    st.markdown(f"""
    <div class="login-wrapper">
        <div class="login-header">
            {logo_html}
            <div class="login-title">Registro de Actividades</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # --- Formulario en tarjeta ---
    with st.container():
        st.markdown('<div class="login-card">', unsafe_allow_html=True)
        
        with st.form("login_form", clear_on_submit=True):
            username = st.text_input(
                "Usuario",
                placeholder="Ingresa tu usuario",
                key="login_username"
            )

            password = st.text_input(
                "Contraseña", 
                type="password",
                placeholder="Ingresa tu contraseña",
                key="login_password"
            )
            
            submitted = st.form_submit_button("Ingresar", use_container_width=True)

            if submitted:
                if not username or not password:
                    st.warning("⚠️ Por favor, ingresa usuario y contraseña.")
                else:
                    with st.spinner("Verificando credenciales..."):
                        conn = get_db_connection()
                        if not conn:
                            st.error("❌ Error al conectar con la base de datos.")
                        else:
                            user_data = auth.authenticate_user(conn, username, password)

                            if user_data:
                                auth.login_user(user_data)
                                st.success(f"✅ ¡Bienvenido(a), {user_data['nombre_completo']}!")
                                st.rerun()
                            else:
                                st.error("❌ Usuario o contraseña incorrectos.")
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Footer
    st.markdown("""
    <div class="login-footer">
        <p>© 2025 iBR - Sistema de Control de Actividades</p>
    </div>
    """, unsafe_allow_html=True)
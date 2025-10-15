# ============================================
# views/login_view.py - Versión Final
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
            display: none;
        }

        /* Contenedor principal */
        [data-testid="block-container"] {
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: flex-start;
            padding-top: 60px;
            min-height: 100vh;
        }

        /* Tarjeta de login */
        .login-card {
            background: white;
            border-radius: 12px;
            box-shadow: 0 8px 24px rgba(0, 0, 0, 0.1);
            padding: 2rem;
            width: 100%;
            max-width: 320px;
            text-align: center;
        }

        .logo-img {
            width: 80px;
            height: 80px;
            margin-bottom: 1rem;
        }

        .login-title {
            font-size: 1.5rem;
            font-weight: bold;
            color: #1f2937;
            margin-bottom: 1.5rem;
        }

        /* Inputs y botón */
        .stTextInput, .stButton {
            width: 100% !important;
            max-width: 280px !important;
            margin: 0 auto 1rem auto !important;
        }

        .stButton > button {
            background: linear-gradient(135deg, #667eea, #764ba2);
            color: white;
            border: none;
            border-radius: 8px;
            padding: 0.75rem;
            font-weight: 600;
            cursor: pointer;
            transition: 0.3s ease;
        }

        .stButton > button:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 16px rgba(102, 126, 234, 0.3);
        }
    </style>
    """, unsafe_allow_html=True)


    # --- Obtener imagen en base64 ---
    logo_path = os.path.join("assets", "ibr_logo.png")
    logo_base64 = get_image_as_base64(logo_path)
    logo_img = ""
    
    if logo_base64:
        logo_img = f'<img src="data:image/png;base64,{logo_base64}" alt="iBR" class="logo-img">'
    else:
        logo_img = '<div style="font-size:2rem;font-weight:700;color:#667eea;margin-bottom:1.5rem;">iBR</div>'

    # --- HTML de la tarjeta de login COMPACTA Y CENTRADA ---
    st.markdown(
        f"""
        <div style="display: flex; flex-direction: column; align-items: center; padding-top: 80px;">
            {logo_img}
            <div style="font-size: 28px; font-weight: bold; margin-top: 20px;">Registro de Actividades</div>
        </div>
        """,
        unsafe_allow_html=True
    )

    # --- Formulario de login COMPACTO Y CENTRADO ---
    with st.form("login_form", clear_on_submit=True):
        
        # Contenedor compacto con ESPACIO ADICIONAL
        st.markdown('<div class="form-compact-container">', unsafe_allow_html=True)
        
        username = st.text_input(
            "Usuario",
            placeholder="Ingresa tu usuario",
            autocomplete="off"
        )

        password = st.text_input(
            "Contraseña", 
            type="password",
            placeholder="Ingresa tu contraseña", 
            autocomplete="off"
        )
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Botón centrado
        submitted = st.form_submit_button("Ingresar", use_container_width=False)

        if submitted:
            if not username or not password:
                st.warning("⚠️ Por favor, ingresa usuario y contraseña.")
            else:
                conn = get_db_connection()
                if not conn:
                    st.error("❌ Error al conectar con la base de datos.")
                else:
                    user_data = auth.authenticate_user(conn, username, password)

                    if user_data:
                        auth.login_user(user_data)
                        st.success(f"✅ ¡Bienvenido(a), {user_data['nombre_completo']}!")
                        st.balloons()
                        st.rerun()
                    else:
                        st.error("❌ Usuario o contraseña incorrectos.")
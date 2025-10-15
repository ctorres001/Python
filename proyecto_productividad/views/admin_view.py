import streamlit as st
from core import queries, auth
from streamlit_option_menu import option_menu


# ======================================================
# 🧭 PANEL PRINCIPAL DE ADMINISTRADOR
# ======================================================
def show_admin_dashboard(conn):
    st.title(f"Panel de Administración: {st.session_state['user_info']['nombre_completo']}")

    # Menú de navegación
    selected = option_menu(
        menu_title=None,
        options=["Dashboard General", "Gestión de Usuarios", "Gestión de Actividades"],
        icons=["bar-chart-line", "people", "list-task"],
        orientation="horizontal",
    )

    if selected == "Dashboard General":
        st.subheader("Dashboard General (Todas las Campañas)")
        st.info("Aquí puedes construir vistas agregadas, similares a las del supervisor pero con filtros por campaña y/o supervisor.")

    elif selected == "Gestión de Usuarios":
        show_user_management(conn)
        
    elif selected == "Gestión de Actividades":
        st.subheader("Gestión de Actividades")
        st.info("Aquí iría un CRUD (Crear, Leer, Actualizar, Borrar) para la tabla 'actividades'.")


# ======================================================
# 👥 GESTIÓN DE USUARIOS
# ======================================================
def show_user_management(conn):
    st.subheader("Gestión de Usuarios")
    
    # --- Datos base para dropdowns ---
    roles, campañas = queries.get_dropdown_data(conn)
    
    if not roles or not campañas:
        st.error("⚠️ No hay roles o campañas disponibles en la base de datos.")
        return
    
    roles_dict = {r['nombre']: r['id'] for r in roles}
    campañas_dict = {c['nombre']: c['id'] for c in campañas}

    # ==================================================
    # 🆕 SECCIÓN: CREAR NUEVO USUARIO
    # ==================================================
    with st.expander("Crear Nuevo Usuario"):
        with st.form("new_user_form", clear_on_submit=True):
            st.write("Crear un nuevo usuario y asignar rol/campaña.")
            
            c1, c2 = st.columns(2)
            username = c1.text_input("Nombre de Usuario (para login)")
            password = c2.text_input("Contraseña", type="password")
            nombre_completo = st.text_input("Nombre Completo")
            
            c3, c4 = st.columns(2)
            rol_nombre = c3.selectbox("Rol", options=list(roles_dict.keys()))
            campaña_nombre = c4.selectbox("Campaña", options=list(campañas_dict.keys()))
            
            submit_new = st.form_submit_button("Crear Usuario")

            if submit_new:
                if not all([username, password, nombre_completo, rol_nombre, campaña_nombre]):
                    st.error("Todos los campos son requeridos.")
                elif not auth.is_strong_password(password):
                    st.error("❌ La contraseña debe tener: 8+ caracteres, mayúsculas, minúsculas, números y símbolos.")
                elif queries.check_username_exists(conn, username):
                    st.error(f"❌ El usuario '{username}' ya existe.")
                else:
                    try:
                        # ✅ Usa la nueva función con engine real
                        success = auth.register_user(
                            username, password, nombre_completo,
                            roles_dict[rol_nombre], campañas_dict[campaña_nombre]
                        )
                        if success:
                            st.success(f"✅ Usuario '{username}' creado exitosamente.")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("No se pudo crear el usuario. Revisa el log o la conexión a la base de datos.")
                    except Exception as e:
                        st.error(f"Error al crear usuario: {e}")

    st.divider()

    # ==================================================
    # 🧾 SECCIÓN: EDITAR USUARIOS EXISTENTES
    # ==================================================
    st.write("Editar usuarios existentes (doble clic en una celda para editar).")

    users_df = queries.get_all_users_admin(conn)

    if users_df.empty:
        st.info("No hay usuarios registrados aún.")
        return

    edited_df = st.data_editor(
        users_df,
        column_config={
            "id": st.column_config.NumberColumn("ID", disabled=True),
            "nombre_usuario": st.column_config.TextColumn("Usuario (Login)", disabled=True),
            "nombre_completo": st.column_config.TextColumn("Nombre Completo", required=True),
            "rol": st.column_config.SelectboxColumn("Rol", options=[r['nombre'] for r in roles], required=True),
            "campaña": st.column_config.SelectboxColumn("Campaña", options=[c['nombre'] for c in campañas], required=True),
            "estado": st.column_config.CheckboxColumn("Activo?", required=True),
        },
        hide_index=True,
        width="stretch",  # reemplaza use_container_width
        num_rows="fixed"
    )

    # --- Botón para guardar cambios ---
    if st.button("Guardar Cambios"):
        try:
            progress_bar = st.progress(0, "Guardando cambios...")
            cambios_realizados = False

            for i, row in edited_df.iterrows():
                original_row = users_df[users_df['id'] == row['id']].iloc[0]
                
                # Detectar cambios
                if (original_row != row).any():
                    queries.update_user_admin(
                        conn,
                        row['id'],
                        row['nombre_completo'],
                        roles_dict[row['rol']],
                        campañas_dict[row['campaña']],
                        row['estado']
                    )
                    cambios_realizados = True
                
                progress_bar.progress((i + 1) / len(edited_df), f"Actualizando usuario {row['id']}...")

            progress_bar.empty()

            if cambios_realizados:
                st.success("✅ ¡Cambios guardados con éxito!")
                st.cache_data.clear()
                st.rerun()
            else:
                st.info("ℹ️ No hay cambios para guardar.")
        except Exception as e:
            st.error(f"Error al guardar cambios: {e}")

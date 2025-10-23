# views/admin_view.py - Versión Completa y Mejorada
import streamlit as st
from core import queries, auth
from streamlit_option_menu import option_menu
from datetime import datetime, timedelta
import pandas as pd
import io

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
        show_dashboard_general(conn)
    elif selected == "Gestión de Usuarios":
        show_user_management(conn)
    elif selected == "Gestión de Actividades":
        show_activity_management(conn)


# ======================================================
# 📊 DASHBOARD GENERAL
# ======================================================
def show_dashboard_general(conn):
    st.subheader("📊 Dashboard General")
    
    # Filtros
    st.markdown("### 🔍 Filtros")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Rango de fechas
        fecha_inicio = st.date_input(
            "Fecha Inicio",
            value=datetime.now().date() - timedelta(days=7),
            max_value=datetime.now().date()
        )
    
    with col2:
        fecha_fin = st.date_input(
            "Fecha Fin",
            value=datetime.now().date(),
            max_value=datetime.now().date()
        )
    
    with col3:
        # Filtro por campaña
        try:
            campañas = queries.get_all_campaigns(conn)
            campaña_options = ["Todas"] + campañas['nombre'].tolist()
            campaña_selected = st.selectbox("Campaña", campaña_options)
            campaña_id = None if campaña_selected == "Todas" else campañas[campañas['nombre'] == campaña_selected]['id'].iloc[0]
        except:
            st.warning("No hay campañas configuradas")
            campaña_id = None
    
    st.divider()
    
    # KPIs Principales
    try:
        kpis = queries.get_admin_kpis(conn, fecha_inicio, fecha_fin, campaña_id)
        
        if not kpis.empty:
            kpi_data = kpis.iloc[0]
            
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("👥 Asesores", int(kpi_data['total_asesores']) if pd.notna(kpi_data['total_asesores']) else 0)
            col2.metric("📋 Registros", int(kpi_data['total_registros']) if pd.notna(kpi_data['total_registros']) else 0)
            col3.metric("⏱️ Horas Totales", f"{float(kpi_data['horas_totales']):.1f}" if pd.notna(kpi_data['horas_totales']) else "0.0")
            col4.metric("📊 Promedio/Actividad", f"{float(kpi_data['promedio_minutos']):.1f} min" if pd.notna(kpi_data['promedio_minutos']) else "0.0 min")
    except Exception as e:
        st.error(f"Error al cargar KPIs: {e}")
    
    st.divider()
    
    # Tabs para diferentes vistas
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Por Campaña", "👤 Por Asesor", "📋 Por Actividad", "📈 Gráficos"])
    
    with tab1:
        show_campaign_report(conn, fecha_inicio, fecha_fin)
    
    with tab2:
        show_asesor_report(conn, fecha_inicio, fecha_fin, campaña_id)
    
    with tab3:
        show_activity_report(conn, fecha_inicio, fecha_fin, campaña_id)
    
    with tab4:
        show_charts(conn, fecha_inicio, fecha_fin, campaña_id)


def show_campaign_report(conn, fecha_inicio, fecha_fin):
    """Reporte por campaña"""
    st.markdown("### 📊 Resumen por Campaña")
    
    try:
        df = queries.get_admin_dashboard_by_campaign(conn, fecha_inicio, fecha_fin)
        
        if not df.empty:
            # Formatear
            df['horas_totales'] = df['horas_totales'].apply(lambda x: f"{float(x):.2f}" if pd.notna(x) else "0.00")
            df['promedio_actividad_min'] = df['promedio_actividad_min'].apply(lambda x: f"{float(x):.1f}" if pd.notna(x) else "0.0")
            
            st.dataframe(df, use_container_width=True, hide_index=True)
            
            # Botón de exportación
            if st.button("📥 Exportar a Excel", key="export_campaign"):
                export_to_excel(df, "reporte_por_campaña")
        else:
            st.info("No hay datos para el rango seleccionado")
    except Exception as e:
        st.error(f"Error: {e}")


def show_asesor_report(conn, fecha_inicio, fecha_fin, campaña_id):
    """Reporte por asesor"""
    st.markdown("### 👤 Resumen por Asesor")
    
    try:
        df = queries.get_admin_dashboard_by_asesor(conn, fecha_inicio, fecha_fin, campaña_id)
        
        if not df.empty:
            # Formatear
            df['horas_totales'] = df['horas_totales'].apply(lambda x: f"{float(x):.2f}" if pd.notna(x) else "0.00")
            
            st.dataframe(df, use_container_width=True, hide_index=True)
            
            # Botón de exportación
            if st.button("📥 Exportar a Excel", key="export_asesor"):
                export_to_excel(df, "reporte_por_asesor")
        else:
            st.info("No hay datos para el rango seleccionado")
    except Exception as e:
        st.error(f"Error: {e}")


def show_activity_report(conn, fecha_inicio, fecha_fin, campaña_id):
    """Reporte por actividad"""
    st.markdown("### 📋 Resumen por Actividad")
    
    try:
        df = queries.get_admin_activity_breakdown(conn, fecha_inicio, fecha_fin, campaña_id)
        
        if not df.empty:
            # Formatear
            df['horas_totales'] = df['horas_totales'].apply(lambda x: f"{float(x):.2f}" if pd.notna(x) else "0.00")
            df['promedio_minutos'] = df['promedio_minutos'].apply(lambda x: f"{float(x):.1f}" if pd.notna(x) else "0.0")
            
            st.dataframe(df, use_container_width=True, hide_index=True)
            
            # Botón de exportación
            if st.button("📥 Exportar a Excel", key="export_activity"):
                export_to_excel(df, "reporte_por_actividad")
        else:
            st.info("No hay datos para el rango seleccionado")
    except Exception as e:
        st.error(f"Error: {e}")


def show_charts(conn, fecha_inicio, fecha_fin, campaña_id):
    """Gráficos visuales"""
    st.markdown("### 📈 Visualizaciones")
    
    try:
        # Gráfico por campaña
        df_campaign = queries.get_admin_dashboard_by_campaign(conn, fecha_inicio, fecha_fin)
        if not df_campaign.empty:
            st.markdown("#### Horas por Campaña")
            chart_data = df_campaign.set_index('campaña')['horas_totales']
            st.bar_chart(chart_data, height=300)
        
        st.divider()
        
        # Gráfico por actividad
        df_activity = queries.get_admin_activity_breakdown(conn, fecha_inicio, fecha_fin, campaña_id)
        if not df_activity.empty:
            st.markdown("#### Distribución de Actividades")
            chart_data2 = df_activity.set_index('nombre_actividad')['horas_totales']
            st.bar_chart(chart_data2, height=300)
    except Exception as e:
        st.error(f"Error al generar gráficos: {e}")


def export_to_excel(df, filename):
    """Exporta DataFrame a Excel"""
    try:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Reporte')
        
        output.seek(0)
        
        st.download_button(
            label="⬇️ Descargar Excel",
            data=output,
            file_name=f"{filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception as e:
        st.error(f"Error al exportar: {e}")


# ======================================================
# 👥 GESTIÓN DE USUARIOS
# ======================================================
def show_user_management(conn):
    st.subheader("👥 Gestión de Usuarios")
    
    # Datos base para dropdowns
    roles, campañas = queries.get_dropdown_data(conn)
    
    if not roles or not campañas:
        st.error("⚠️ No hay roles o campañas disponibles en la base de datos.")
        return
    
    roles_dict = {r['nombre']: r['id'] for r in roles}
    campañas_dict = {c['nombre']: c['id'] for c in campañas}

    # Crear nuevo usuario
    with st.expander("➕ Crear Nuevo Usuario"):
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
                        success = auth.register_user(
                            username, password, nombre_completo,
                            roles_dict[rol_nombre], campañas_dict[campaña_nombre]
                        )
                        if success:
                            st.success(f"✅ Usuario '{username}' creado exitosamente.")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("No se pudo crear el usuario.")
                    except Exception as e:
                        st.error(f"Error al crear usuario: {e}")

    st.divider()

    # Editar usuarios existentes
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
        use_container_width=True,
        num_rows="fixed"
    )

    col1, col2 = st.columns([3, 1])
    
    with col1:
        if st.button("💾 Guardar Cambios", use_container_width=True):
            try:
                cambios_realizados = False
                for i, row in edited_df.iterrows():
                    original_row = users_df[users_df['id'] == row['id']].iloc[0]
                    
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

                if cambios_realizados:
                    st.success("✅ ¡Cambios guardados con éxito!")
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.info("ℹ️ No hay cambios para guardar.")
            except Exception as e:
                st.error(f"Error al guardar cambios: {e}")
    
    with col2:
        if st.button("📥 Exportar a Excel", use_container_width=True):
            export_to_excel(users_df, "usuarios")


# ======================================================
# 📋 GESTIÓN DE ACTIVIDADES
# ======================================================
def show_activity_management(conn):
    st.subheader("📋 Gestión de Actividades")
    
    tab1, tab2, tab3, tab4 = st.tabs(["🏢 Campañas", "📌 Actividades", "🔖 Subactividades", "👤 Roles"])
    
    with tab1:
        manage_campaigns(conn)
    
    with tab2:
        manage_activities(conn)
    
    with tab3:
        manage_subactivities(conn)
    
    with tab4:
        manage_roles(conn)


def manage_campaigns(conn):
    """CRUD de Campañas"""
    st.markdown("### 🏢 Gestión de Campañas")
    
    # Crear
    with st.expander("➕ Nueva Campaña"):
        with st.form("new_campaign"):
            nombre = st.text_input("Nombre de la Campaña")
            if st.form_submit_button("Crear"):
                if nombre:
                    try:
                        queries.create_campaign(conn, nombre)
                        st.success("✅ Campaña creada")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))
                else:
                    st.error("El nombre es requerido")
    
    # Listar y editar
    try:
        df = queries.get_all_campaigns(conn)
        if not df.empty:
            edited_df = st.data_editor(
                df,
                column_config={
                    "id": st.column_config.NumberColumn("ID", disabled=True),
                    "nombre": st.column_config.TextColumn("Nombre", required=True)
                },
                hide_index=True,
                use_container_width=True,
                num_rows="fixed",
                key="campaigns_editor"
            )
            
            col1, col2 = st.columns([3, 1])
            with col1:
                if st.button("💾 Guardar Cambios", key="save_campaigns"):
                    for i, row in edited_df.iterrows():
                        original = df[df['id'] == row['id']].iloc[0]
                        if original['nombre'] != row['nombre']:
                            try:
                                queries.update_campaign(conn, row['id'], row['nombre'])
                            except Exception as e:
                                st.error(str(e))
                    st.success("✅ Cambios guardados")
                    st.cache_data.clear()
                    st.rerun()
        else:
            st.info("No hay campañas registradas")
    except Exception as e:
        st.error(f"Error: {e}")


def manage_activities(conn):
    """CRUD de Actividades"""
    st.markdown("### 📌 Gestión de Actividades")
    
    # Crear
    with st.expander("➕ Nueva Actividad"):
        with st.form("new_activity"):
            nombre = st.text_input("Nombre de la Actividad")
            descripcion = st.text_area("Descripción")
            orden = st.number_input("Orden", min_value=0, value=0)
            if st.form_submit_button("Crear"):
                if nombre:
                    try:
                        queries.create_activity(conn, nombre, descripcion, orden)
                        st.success("✅ Actividad creada")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))
                else:
                    st.error("El nombre es requerido")
    
    # Listar y editar
    try:
        df = queries.get_all_activities_admin(conn)
        if not df.empty:
            edited_df = st.data_editor(
                df,
                column_config={
                    "id": st.column_config.NumberColumn("ID", disabled=True),
                    "nombre_actividad": st.column_config.TextColumn("Nombre", required=True),
                    "descripcion": st.column_config.TextColumn("Descripción"),
                    "orden": st.column_config.NumberColumn("Orden", required=True),
                    "activo": st.column_config.CheckboxColumn("Activo?", required=True)
                },
                hide_index=True,
                use_container_width=True,
                num_rows="fixed",
                key="activities_editor"
            )
            
            if st.button("💾 Guardar Cambios", key="save_activities"):
                for i, row in edited_df.iterrows():
                    original = df[df['id'] == row['id']].iloc[0]
                    if (original != row).any():
                        try:
                            queries.update_activity(
                                conn, 
                                row['id'], 
                                row['nombre_actividad'], 
                                row['descripcion'],
                                row['orden'],
                                row['activo']
                            )
                        except Exception as e:
                            st.error(str(e))
                st.success("✅ Cambios guardados")
                st.cache_data.clear()
                st.rerun()
        else:
            st.info("No hay actividades registradas")
    except Exception as e:
        st.error(f"Error: {e}")


def manage_subactivities(conn):
    """CRUD de Subactividades"""
    st.markdown("### 🔖 Gestión de Subactividades")
    
    # Crear
    with st.expander("➕ Nueva Subactividad"):
        with st.form("new_subactivity"):
            try:
                activities = queries.get_all_activities_admin(conn)
                if not activities.empty:
                    activity_options = dict(zip(activities['nombre_actividad'], activities['id']))
                    
                    activity_name = st.selectbox("Actividad Padre", options=list(activity_options.keys()))
                    nombre = st.text_input("Nombre de la Subactividad")
                    orden = st.number_input("Orden", min_value=0, value=0)
                    
                    if st.form_submit_button("Crear"):
                        if nombre:
                            try:
                                queries.create_subactivity(conn, activity_options[activity_name], nombre, orden)
                                st.success("✅ Subactividad creada")
                                st.cache_data.clear()
                                st.rerun()
                            except Exception as e:
                                st.error(str(e))
                        else:
                            st.error("El nombre es requerido")
                else:
                    st.warning("Primero debes crear actividades")
            except Exception as e:
                st.error(f"Error: {e}")
    
    # Listar y editar
    try:
        df = queries.get_all_subactivities(conn)
        if not df.empty:
            edited_df = st.data_editor(
                df,
                column_config={
                    "id": st.column_config.NumberColumn("ID", disabled=True),
                    "nombre_subactividad": st.column_config.TextColumn("Nombre", required=True),
                    "nombre_actividad": st.column_config.TextColumn("Actividad Padre", disabled=True),
                    "orden": st.column_config.NumberColumn("Orden", required=True),
                    "activo": st.column_config.CheckboxColumn("Activo?", required=True)
                },
                hide_index=True,
                use_container_width=True,
                num_rows="fixed",
                key="subactivities_editor"
            )
            
            if st.button("💾 Guardar Cambios", key="save_subactivities"):
                for i, row in edited_df.iterrows():
                    original = df[df['id'] == row['id']].iloc[0]
                    if (original != row).any():
                        try:
                            queries.update_subactivity(
                                conn,
                                row['id'],
                                row['nombre_subactividad'],
                                row['activo'],
                                row['orden']
                            )
                        except Exception as e:
                            st.error(str(e))
                st.success("✅ Cambios guardados")
                st.cache_data.clear()
                st.rerun()
        else:
            st.info("No hay subactividades registradas")
    except Exception as e:
        st.error(f"Error: {e}")


def manage_roles(conn):
    """CRUD de Roles"""
    st.markdown("### 👤 Gestión de Roles")
    
    # Crear
    with st.expander("➕ Nuevo Rol"):
        with st.form("new_role"):
            nombre = st.text_input("Nombre del Rol")
            if st.form_submit_button("Crear"):
                if nombre:
                    try:
                        queries.create_role(conn, nombre)
                        st.success("✅ Rol creado")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))
                else:
                    st.error("El nombre es requerido")
    
    # Listar y editar
    try:
        df = queries.get_all_roles(conn)
        if not df.empty:
            edited_df = st.data_editor(
                df,
                column_config={
                    "id": st.column_config.NumberColumn("ID", disabled=True),
                    "nombre": st.column_config.TextColumn("Nombre", required=True)
                },
                hide_index=True,
                use_container_width=True,
                num_rows="fixed",
                key="roles_editor"
            )
            
            col1, col2 = st.columns([3, 1])
            with col1:
                if st.button("💾 Guardar Cambios", key="save_roles"):
                    for i, row in edited_df.iterrows():
                        original = df[df['id'] == row['id']].iloc[0]
                        if original['nombre'] != row['nombre']:
                            try:
                                queries.update_role(conn, row['id'], row['nombre'])
                            except Exception as e:
                                st.error(str(e))
                    st.success("✅ Cambios guardados")
                    st.cache_data.clear()
                    st.rerun()
            
            with col2:
                st.info("ℹ️ Los roles por defecto no deben eliminarse")
        else:
            st.info("No hay roles registrados")
    except Exception as e:
        st.error(f"Error: {e}")
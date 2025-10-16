import streamlit as st
from core import queries
from datetime import datetime, time as dt_time
import pandas as pd
from streamlit.components.v1 import html
from html import escape

# --- Utilidades ---
def format_timedelta(td):
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"

def inject_timer_script(start_timestamp):
    timer_html = f"""
    <div id="timer-container" style="font-size: 2rem; font-weight: 600; color: white; text-align: center; padding: 1.5rem; background: linear-gradient(135deg, #4A90E2 0%, #357ABD 100%); border-radius: 16px; box-shadow: 0 8px 16px rgba(74, 144, 226, 0.3);">
        <div style="font-size: 0.85rem; font-weight: 400; opacity: 0.95; margin-bottom: 0.5rem; letter-spacing: 0.5px;">TIEMPO TRANSCURRIDO</div>
        <div id="timer-display" style="font-size: 2.5rem; font-weight: 700; letter-spacing: 2px;">00:00:00</div>
    </div>
    <script>
        const startTime = {start_timestamp};
        function updateTimer() {{
            const now = Date.now();
            const elapsed = Math.floor((now - startTime) / 1000);
            const hours = Math.floor(elapsed / 3600);
            const minutes = Math.floor((elapsed % 3600) / 60);
            const seconds = elapsed % 60;
            const display = String(hours).padStart(2, '0') + ':' + 
                            String(minutes).padStart(2, '0') + ':' + 
                            String(seconds).padStart(2, '0');
            document.getElementById('timer-display').textContent = display;
        }}
        updateTimer();
        setInterval(updateTimer, 1000);
    </script>
    """
    return timer_html

def check_and_close_day(conn, user_id):
    now = datetime.now()
    if st.session_state.get('current_registro_id'):
        current_start = st.session_state.get('current_start_time')
        if current_start and current_start.date() < now.date():
            end_of_previous_day = datetime.combine(current_start.date(), dt_time(23, 59, 59))
            try:
                queries.stop_activity(conn, st.session_state['current_registro_id'], end_of_previous_day)
                st.session_state['current_activity_id'] = None
                st.session_state['current_activity_name'] = '---'
                st.session_state['current_start_time'] = None
                st.session_state['current_registro_id'] = None
                st.session_state['current_subactivity'] = None
                st.session_state['current_comment'] = None
                st.info("✅ Jornada anterior cerrada automáticamente a las 23:59:59")
            except Exception as e:
                st.error(f"Error al cerrar jornada anterior: {e}")

def restore_open_activity(conn, user_id, date):
    if 'activity_restored' not in st.session_state:
        try:
            open_activity = queries.get_open_activity(conn, user_id, date)
            if open_activity is not None and not open_activity.empty:
                row = open_activity.iloc[0]
                st.session_state['current_registro_id'] = row['id']
                st.session_state['current_activity_id'] = row['actividad_id']
                st.session_state['current_activity_name'] = row['nombre_actividad']
                st.session_state['current_start_time'] = row['hora_inicio']
                st.session_state['current_subactivity'] = row.get('subactividad', None)
                st.session_state['current_comment'] = row.get('comentario', None)
                st.success(f"🔄 Actividad restaurada: {row['nombre_actividad']}")
            st.session_state['activity_restored'] = True
        except Exception as e:
            st.warning(f"No se pudo restaurar actividad: {e}")
            st.session_state['activity_restored'] = True

def handle_activity_click(conn, user_id, new_activity_id, new_activity_name, subactivity=None, comment=None):
    now = datetime.now()
    if st.session_state.get('current_registro_id'):
        try:
            queries.stop_activity(conn, st.session_state['current_registro_id'], now)
        except Exception as e:
            st.session_state['last_error'] = f"Error al detener actividad: {str(e)}"
            return
    if new_activity_name == 'Salida':
        try:
            reg_id = queries.start_activity(conn, user_id, new_activity_id, now, subactivity, comment)
            queries.stop_activity(conn, reg_id, now)
            st.session_state['current_activity_id'] = None
            st.session_state['current_activity_name'] = "Jornada Finalizada"
            st.session_state['current_start_time'] = None
            st.session_state['current_registro_id'] = None
            st.session_state['current_subactivity'] = None
            st.session_state['current_comment'] = None
            st.session_state['last_success'] = "✅ Has marcado tu Salida. ¡Jornada finalizada!"
            st.session_state.pop('last_error', None)
            st.session_state.pop('show_subactivity_modal', None)
        except Exception as e:
            st.session_state['last_error'] = f"Error al marcar salida: {str(e)}"
            return
    else:
        try:
            new_reg_id = queries.start_activity(conn, user_id, new_activity_id, now, subactivity, comment)
            st.session_state['current_activity_id'] = new_activity_id
            st.session_state['current_activity_name'] = new_activity_name
            st.session_state['current_start_time'] = now
            st.session_state['current_registro_id'] = new_reg_id
            st.session_state['current_subactivity'] = subactivity
            st.session_state['current_comment'] = comment
            subact_text = f" - {subactivity}" if subactivity else ""
            st.session_state['last_success'] = f"✅ Actividad iniciada: {new_activity_name}{subact_text}"
            st.session_state.pop('last_error', None)
            st.session_state.pop('show_subactivity_modal', None)
        except Exception as e:
            st.session_state['last_error'] = f"Error al iniciar actividad: {str(e)}"
            return
    st.cache_data.clear()

def get_activity_color(activity_name):
    colors = {
        'Ingreso': '#E0E0E0',
        'Seguimiento': '#C8E6C9',
        'Bandeja de correo': '#BBDEFB',
        'Reportes': '#FFE0B2',
        'Break Salida': '#B2EBF2',
        'Regreso Break': '#B2EBF2',
        'Auxiliares': '#F8BBD0',
        'Reunión': '#E1BEE7',
        'Incidencia': '#FFCDD2',
        'Salida': '#CFD8DC'
    }
    return colors.get(activity_name, '#F5F5F5')

def show_subactivity_modal(conn, activity_id, activity_name):
    """Muestra modal con selector de subactividad desde BD y campo de comentario"""
    st.markdown("---")
    st.markdown(f"### 📋 Detalles de {activity_name}")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.markdown("**Subactividad**")
        
        # Obtener subactividades desde la base de datos
        try:
            subact_df = queries.get_subactivities(conn, activity_id)
            
            if not subact_df.empty:
                # Crear diccionario {nombre: id}
                options_dict = dict(zip(subact_df['nombre_subactividad'], subact_df['id']))
                
                selected_name = st.selectbox(
                    "Tipo de gestión:",
                    list(options_dict.keys()),
                    key=f"subact_{activity_name}_{activity_id}",
                    label_visibility="collapsed"
                )
                selected_subactivity_id = options_dict[selected_name]
            else:
                st.warning("⚠️ No hay subactividades configuradas para esta actividad")
                selected_subactivity_id = None
        except Exception as e:
            st.error(f"Error al cargar subactividades: {e}")
            selected_subactivity_id = None
    
    with col2:
        st.markdown("**ID Cliente / Referencia**")
        client_ref = st.text_input(
            "ID Cliente:",
            placeholder="Ej: CLI-0003",
            key=f"client_{activity_name}_{activity_id}",
            label_visibility="collapsed",
            max_chars=50
        )
    
    st.markdown("**Resumen breve**")
    comment = st.text_area(
        "Describe brevemente la actividad:",
        placeholder="Ej: Atención de reclamo por facturación incorrecta del mes anterior...",
        key=f"comment_{activity_name}_{activity_id}",
        label_visibility="collapsed",
        max_chars=250,
        height=100
    )
    
    # Combinar referencia y comentario
    full_comment = ""
    if client_ref:
        full_comment += f"[{client_ref}] "
    if comment:
        full_comment += comment
    
    st.markdown("---")
    
    return selected_subactivity_id, full_comment if full_comment else None

def show_asesor_dashboard(conn):
    user = st.session_state['user_info']
    user_id = user['id']
    
    # Definir la fecha "de hoy" (según el servidor de Streamlit)
    today_date = datetime.now().date()

    # Restaurar actividad abierta al cargar
    restore_open_activity(conn, user_id, today_date)
    
    # Verificar y cerrar día si cambió
    check_and_close_day(conn, user_id)
    
    # CSS personalizado con azul corporativo
    st.markdown("""
    <style>
        .block-container {
            padding-top: 2rem;
        }
        
        .main-header {
            font-size: 1.8rem;
            font-weight: 700;
            color: #1E3A5F;
            margin-bottom: 0.3rem;
        }
        
        .sub-header {
            color: #6B7280;
            font-size: 0.95rem;
            margin-bottom: 1.5rem;
        }
        
        .section-title {
            font-size: 1.1rem;
            font-weight: 600;
            color: #1E3A5F;
            margin: 1.5rem 0 1rem 0;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }
        
        .current-activity-badge {
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
            background: linear-gradient(135deg, #4A90E2 0%, #357ABD 100%);
            color: white;
            padding: 0.75rem 1.5rem;
            border-radius: 24px;
            font-weight: 600;
            font-size: 1rem;
            box-shadow: 0 4px 12px rgba(74, 144, 226, 0.25);
        }
        
        .timeline-item {
            padding: 1rem 1.25rem;
            border-radius: 12px;
            margin-bottom: 0.75rem;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }
        
        .timeline-item:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.12);
        }
        
        .timeline-activity {
            font-weight: 600;
            font-size: 0.95rem;
            color: #1E3A5F;
            margin-bottom: 0.25rem;
        }
        
        .timeline-subactivity {
            font-size: 0.85rem;
            color: #6B7280;
            font-style: italic;
            margin-bottom: 0.25rem;
        }
        
        .timeline-comment {
            font-size: 0.8rem;
            color: #9CA3AF;
            margin-top: 0.25rem;
            padding: 0.25rem 0.5rem;
            background: rgba(255,255,255,0.5);
            border-radius: 6px;
        }
        
        .timeline-time {
            font-weight: 700;
            font-size: 1rem;
            color: #4A90E2;
            margin-top: 0.5rem;
        }
        
        .stButton > button {
            border-radius: 12px !important;
            font-weight: 600 !important;
            transition: all 0.3s ease !important;
            border: 2px solid transparent !important;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08) !important;
        }
        
        .stButton > button:hover:not(:disabled) {
            transform: translateY(-2px) !important;
            box-shadow: 0 4px 12px rgba(74, 144, 226, 0.3) !important;
            border-color: #4A90E2 !important;
        }
        
        .stButton > button[kind="primary"] {
            background: linear-gradient(135deg, #4A90E2 0%, #357ABD 100%) !important;
            color: white !important;
        }
        
        .stButton > button:disabled {
            opacity: 0.5 !important;
            cursor: not-allowed !important;
        }
        
        [data-testid="stMetricValue"] {
            font-size: 1.8rem;
            color: #4A90E2;
        }
        
        * {
            transition: all 0.2s ease;
        }
        
        .modal-container {
            background: #F9FAFB;
            padding: 1.5rem;
            border-radius: 12px;
            border: 2px solid #E5E7EB;
        }
    </style>
    """, unsafe_allow_html=True)
    
    # Header
    st.markdown(f'<div class="main-header">Control de Actividades</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="sub-header">{user["nombre_completo"]} • {user.get("campaña_nombre", "N/A")}</div>', unsafe_allow_html=True)
    
    # Mostrar mensajes
    if 'last_error' in st.session_state:
        st.error(st.session_state['last_error'])
        if st.button("Limpiar error", type="secondary"):
            st.session_state.pop('last_error')
            st.rerun()
    
    if 'last_success' in st.session_state:
        st.success(st.session_state['last_success'])
        st.session_state.pop('last_success', None)

    # Inicializar estado
    if 'current_activity_id' not in st.session_state:
        st.session_state['current_activity_id'] = None
    if 'current_activity_name' not in st.session_state:
        st.session_state['current_activity_name'] = '---'
    if 'current_start_time' not in st.session_state:
        st.session_state['current_start_time'] = None
    if 'current_registro_id' not in st.session_state:
        st.session_state['current_registro_id'] = None
    if 'current_subactivity' not in st.session_state:
        st.session_state['current_subactivity'] = None
    if 'current_comment' not in st.session_state:
        st.session_state['current_comment'] = None

    # Estado Actual
    st.markdown('<div class="section-title">⏰ Estado actual</div>', unsafe_allow_html=True)
    
    activity_display = st.session_state.get('current_activity_name', '---')
    subactivity_display = st.session_state.get('current_subactivity', '')
    
    if activity_display != '---' and activity_display != 'Jornada Finalizada':
        full_activity = f"{activity_display}"
        if subactivity_display:
            full_activity += f" • {subactivity_display}"
        st.markdown(f'<div class="current-activity-badge">📍 {full_activity}</div>', unsafe_allow_html=True)
    else:
        st.info(f"📍 {activity_display}")
    
    # Cronómetro
    if st.session_state.get('current_start_time'):
        start_timestamp = int(st.session_state['current_start_time'].timestamp() * 1000)
        html(inject_timer_script(start_timestamp), height=110)
    else:
        st.markdown("""
        <div style="padding: 2rem; text-align: center; background: linear-gradient(135deg, #E8F4F8 0%, #D6EAF8 100%); 
                    border-radius: 16px; color: #5DADE2; font-size: 1.1rem; font-weight: 600;">
            ⏸️ Sin actividad en curso
        </div>
        """, unsafe_allow_html=True)

    st.markdown('<div class="section-title">📋 Registrar Actividad</div>', unsafe_allow_html=True)
    
    # Modal para subactividades
    if st.session_state.get('show_subactivity_modal'):
        activity_info = st.session_state['pending_activity']
        
        with st.container():
            st.markdown('<div class="modal-container">', unsafe_allow_html=True)
            subactivity, comment = show_subactivity_modal(conn, activity_info['id'], activity_info['name'])
            
            col1, col2, col3 = st.columns([1, 1, 2])
            
            if col1.button("✅ Confirmar", type="primary", width='stretch'):
                handle_activity_click(
                    conn, 
                    user['id'], 
                    activity_info['id'], 
                    activity_info['name'],
                    subactivity,
                    comment
                )
                st.rerun()
            
            if col2.button("❌ Cancelar", width='stretch'):
                st.session_state.pop('show_subactivity_modal')
                st.session_state.pop('pending_activity')
                st.rerun()
            
            st.markdown('</div>', unsafe_allow_html=True)
    
    # Botones de actividad
    try:
        activities_df = queries.get_active_activities(conn)
    except Exception as e:
        st.error(f"Error al cargar actividades: {e}")
        return
    
    if activities_df.empty:
        st.warning("No hay actividades disponibles.")
        return
    
    cols = st.columns(4)
    
    # Verificar si necesita modal de detalles
    activities_with_details = ['Seguimiento', 'Bandeja de correo', 'Reportes', 'Auxiliares']

    for index, row in activities_df.iterrows():
        col = cols[index % 4]
        activity_id = row['id']
        activity_name = row['nombre_actividad']
        
        disabled = bool(
            (activity_id == st.session_state.get('current_activity_id')) or 
            (st.session_state.get('current_activity_name') == "Jornada Finalizada")
        )
        
        emoji_map = {
            'Seguimiento': '📞',
            'Bandeja de correo': '📧',
            'Reportes': '📊',
            'Pausa': '☕',
            'Auxiliares': '🔧',
            'Reunión': '👥',
            'Salida': '🚪'
        }
        emoji = emoji_map.get(activity_name, '📌')
        
        if col.button(f"{emoji} {activity_name}", 
                     key=f"btn_{activity_id}", 
                     width='stretch', 
                     disabled=disabled,
                     type="primary"):
            
 
            if activity_name in activities_with_details:
                st.session_state['show_subactivity_modal'] = True
                st.session_state['pending_activity'] = {
                    'id': activity_id,
                    'name': activity_name
                }
                st.rerun()
            else:
                handle_activity_click(conn, user['id'], activity_id, activity_name)
                st.rerun()

    # Resumen Consolidado del Día
    st.markdown('<div class="section-title">📊 Resumen del día</div>', unsafe_allow_html=True)
    
    try:
        # Pasa 'today_date' como argumento
        summary_df = queries.get_today_summary(conn, user_id, today_date)
        
        if not summary_df.empty:
            # Calcular totales
            total_segundos = summary_df['total_segundos'].sum()
            total_horas = int(total_segundos // 3600)
            total_minutos = int((total_segundos % 3600) // 60)
            total_segs = int(total_segundos % 60)
            
            # Mostrar métricas principales
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("⏱️ Tiempo Total", f"{total_horas:02d}:{total_minutos:02d}:{total_segs:02d}")
            col2.metric("📋 Actividades", len(summary_df))
            col3.metric("🎯 En Curso", "Sí" if st.session_state.get('current_registro_id') else "No")
            col4.metric("📅 Fecha", datetime.now().strftime("%d/%m/%Y"))
            
            st.markdown("---")
            
            # Tabla consolidada
            st.markdown("**Consolidado por Actividad**")
            
            # Formatear datos para mostrar
            display_df = summary_df.copy()
            display_df['Tiempo'] = display_df['total_segundos'].apply(
                lambda x: f"{int(x//3600):02d}:{int((x%3600)//60):02d}:{int(x%60):02d}"
            )
            display_df['Porcentaje'] = (display_df['total_segundos'] / total_segundos * 100).round(1)
            display_df['Porcentaje'] = display_df['Porcentaje'].apply(lambda x: f"{x}%")
            
            # Mostrar solo columnas relevantes
            display_df = display_df[['nombre_actividad', 'Tiempo', 'Porcentaje']]
            display_df.columns = ['Actividad', 'Tiempo Total (HH:MM:SS)', '% del Día']
            
            st.dataframe(
                display_df,
                width='stretch',
                hide_index=True,
                column_config={
                    "Actividad": st.column_config.TextColumn("Actividad", width="medium"),
                    "Tiempo Total (HH:MM:SS)": st.column_config.TextColumn("Tiempo Total", width="small"),
                    "% del Día": st.column_config.TextColumn("% del Día", width="small")
                }
            )
            
            st.markdown("---")
            
            # Gráfico de barras (solo nombres sin modificación manual)
            st.markdown("**Distribución de Tiempo**")
            
            chart_df = summary_df.copy()
            chart_df['minutos'] = chart_df['total_segundos'] / 60
            chart_df = chart_df.set_index('nombre_actividad')['minutos']
            
            st.bar_chart(
                chart_df,
                height=280,
                width='stretch',
                color='#4A90E2'
            )
            
        else:
            st.info("📭 Aún no hay actividades completadas hoy.")
    except Exception as e:
        st.warning(f"No se pudo cargar el resumen: {e}")

    # Línea de tiempo (Histórico detallado)
    st.markdown('<div class="section-title">🕐 Histórico de Actividades</div>', unsafe_allow_html=True)
    try:
        # Pasa 'today_date' como argumento
        log_df = queries.get_today_log(conn, user_id, today_date) # (Usando la corrección anterior)
        
        if not log_df.empty:
            # Tabla detallada con toda la información
            st.markdown("**Registro completo del día**")
            
            # Crear DataFrame para mostrar
            display_log = log_df.copy()
            display_log.columns = ['Actividad', 'Subactividad', 'Comentario', 'Hora Inicio', 'Duración']
            
            # Reemplazar valores None con "-"
            display_log = display_log.fillna('-')
            
            st.dataframe(
                display_log,
                width='stretch',
                hide_index=True,
                column_config={
                    "Actividad": st.column_config.TextColumn("📋 Actividad", width="medium"),
                    "Subactividad": st.column_config.TextColumn("📌 Tipo", width="medium"),
                    "Comentario": st.column_config.TextColumn("💬 Resumen", width="large"),
                    "Hora Inicio": st.column_config.TextColumn("🕐 Inicio", width="small"),
                    "Duración": st.column_config.TextColumn("⏱️ Duración", width="small")
                }
            )
            
            st.markdown("---")
            
            # Timeline visual (opcional, más visual)
            st.markdown("**Línea de Tiempo Visual**")

            for _, row in log_df.iterrows():
                activity_name = row.get('nombre_actividad', 'N/A')
                subactivity = row.get('subactividad', '')
                comment = row.get('comentario', '')
                inicio = row.get('inicio', '')
                duration = row.get('duracion', 'En curso')
                color = get_activity_color(activity_name)
                
                emoji_map = {
                    'Seguimiento': '📞',
                    'Bandeja de correo': '📧',
                    'Reportes': '📊',
                    'Pausa': '☕',
                    'Auxiliares': '🔧',
                    'Reunión': '👥',
                    'Salida': '🚪'
                }
                emoji = emoji_map.get(activity_name, '📌')
                
                # Formatear la hora de inicio correctamente
                if isinstance(inicio, datetime):
                    inicio_str = inicio.strftime("%H:%M:%S")
                else:
                    inicio_str = str(inicio) if inicio else "N/A"
                
                timeline_html = f"""
                <div class="timeline-item" style="background-color: {color};">
                    <div style="flex: 1;">
                        <div class="timeline-activity">{emoji} {activity_name} <span style="color: #6B7280; font-size: 0.85rem;">• {inicio_str}</span></div>
                """
                
                if subactivity and subactivity != '-':
                    timeline_html += f'<div class="timeline-subactivity">→ {subactivity}</div>'
                
                if comment and comment != '-':
                    display_comment = comment if len(comment) <= 80 else comment[:77] + "..."
                    safe_comment = escape(display_comment)
                    timeline_html += f'<div class="timeline-comment">{safe_comment}</div>'
                
                # ✅ CORRECCIÓN: Cerrar el div antes de la duración
                timeline_html += f"""
                    </div>
                    <div class="timeline-time">{duration}</div>
                </div>
                """
                
                st.markdown(timeline_html, unsafe_allow_html=True)
        else:
            st.info("📭 Sin registros hoy.")
    except Exception as e:
        st.warning(f"No se pudo cargar el histórico: {e}")
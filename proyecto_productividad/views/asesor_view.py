import streamlit as st
from core import queries
from datetime import datetime
import time
import pandas as pd
from streamlit.components.v1 import html

def format_timedelta(td):
    """Formatea un objeto timedelta a HH:MM:SS."""
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"

def inject_timer_script(start_timestamp):
    """Inyecta JavaScript para cronómetro sin refresh visible"""
    timer_html = f"""
    <div id="timer-container" style="font-size: 2.5rem; font-weight: 700; color: #1f77b4; text-align: center; padding: 1rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 12px; color: white; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
        <div style="font-size: 0.9rem; font-weight: 400; opacity: 0.9; margin-bottom: 0.5rem;">⏱️ Tiempo Transcurrido</div>
        <div id="timer-display">00:00:00</div>
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

def handle_activity_click(conn, user_id, new_activity_id, new_activity_name):
    """
    Lógica central para detener la actividad anterior e iniciar la nueva.
    """
    now = datetime.now()

    # 1. Detener actividad actual (si existe)
    if st.session_state.get('current_registro_id'):
        try:
            queries.stop_activity(conn, st.session_state['current_registro_id'], now)
        except Exception as e:
            st.session_state['last_error'] = f"Error al detener actividad: {str(e)}"
            return

    # 2. Manejar "Salida" (es un evento final)
    if new_activity_name == 'Salida':
        try:
            reg_id = queries.start_activity(conn, user_id, new_activity_id, now)
            queries.stop_activity(conn, reg_id, now)
            
            st.session_state['current_activity_id'] = None
            st.session_state['current_activity_name'] = "Jornada Finalizada"
            st.session_state['current_start_time'] = None
            st.session_state['current_registro_id'] = None
            st.session_state['last_success'] = "Has marcado tu Salida. ¡Jornada finalizada!"
            st.session_state.pop('last_error', None)
        except Exception as e:
            st.session_state['last_error'] = f"Error al marcar salida: {str(e)}"
            return
    
    # 3. Iniciar nueva actividad (si no es "Salida")
    else:
        try:
            new_reg_id = queries.start_activity(conn, user_id, new_activity_id, now)
            
            st.session_state['current_activity_id'] = new_activity_id
            st.session_state['current_activity_name'] = new_activity_name
            st.session_state['current_start_time'] = now
            st.session_state['current_registro_id'] = new_reg_id
            st.session_state['last_success'] = f"✅ Actividad iniciada: {new_activity_name}"
            st.session_state.pop('last_error', None)
        except Exception as e:
            st.session_state['last_error'] = f"Error al iniciar actividad: {str(e)}"
            return

    st.cache_data.clear()

def get_activity_color(activity_name):
    """Retorna color según el tipo de actividad"""
    colors = {
        'Seguimiento': '#c7f0db',
        'Caso Nuevo': '#d4d4f7',
        'Reportaría': '#ffeaa7',
        'Pausa': '#b8e6e6',
        'Auxiliares': '#ffd7d7',
        'Reunión': '#e1d4f7',
        'Salida': '#dfe6e9'
    }
    return colors.get(activity_name, '#f0f0f0')

def show_asesor_dashboard(conn):
    user = st.session_state['user_info']
    
    # CSS personalizado
    st.markdown("""
    <style>
        .main-header {
            font-size: 2rem;
            font-weight: 700;
            color: #2c3e50;
            margin-bottom: 0.5rem;
        }
        .sub-header {
            color: #7f8c8d;
            font-size: 1rem;
            margin-bottom: 2rem;
        }
        .status-card {
            background: white;
            padding: 1.5rem;
            border-radius: 12px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            margin-bottom: 1.5rem;
        }
        .activity-button {
            height: 60px !important;
            font-size: 1rem !important;
            font-weight: 600 !important;
            border-radius: 8px !important;
            transition: all 0.3s ease !important;
        }
        .section-title {
            font-size: 1.3rem;
            font-weight: 600;
            color: #2c3e50;
            margin: 2rem 0 1rem 0;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }
        .timeline-item {
            padding: 1rem;
            border-radius: 8px;
            margin-bottom: 0.5rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }
        .timeline-activity {
            font-weight: 600;
            font-size: 1rem;
        }
        .timeline-time {
            font-weight: 700;
            font-size: 1.1rem;
        }
        .current-activity-badge {
            display: inline-block;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 0.5rem 1rem;
            border-radius: 20px;
            font-weight: 600;
            font-size: 1.1rem;
            margin-bottom: 1rem;
        }
    </style>
    """, unsafe_allow_html=True)
    
    # Header
    st.markdown(f'<div class="main-header">🎯 Control de Actividades</div>', unsafe_allow_html=True)
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

    # Estado Actual con Cronómetro
    st.markdown('<div class="section-title">⏰ Estado actual</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        activity_display = st.session_state.get('current_activity_name', '---')
        if activity_display != '---' and activity_display != 'Jornada Finalizada':
            st.markdown(f'<div class="current-activity-badge">{activity_display}</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div style="padding: 1rem; color: #7f8c8d; font-size: 1.1rem;">📍 {activity_display}</div>', unsafe_allow_html=True)
    
    with col2:
        if st.session_state.get('current_start_time'):
            start_timestamp = int(st.session_state['current_start_time'].timestamp() * 1000)
            html(inject_timer_script(start_timestamp), height=120)
        else:
            st.markdown('<div style="padding: 2rem; text-align: center; color: #95a5a6; font-size: 1.2rem;">⏸️ Sin actividad en curso</div>', unsafe_allow_html=True)

    # Botones de Actividad
    st.markdown('<div class="section-title">📋 Registrar Actividad</div>', unsafe_allow_html=True)
    
    try:
        activities_df = queries.get_active_activities(conn)
    except Exception as e:
        st.error(f"Error al cargar actividades: {e}")
        return
    
    if activities_df.empty:
        st.warning("No hay actividades disponibles.")
        return
    
    # Organizar botones en grid
    cols = st.columns(4)
    
    for index, row in activities_df.iterrows():
        col = cols[index % 4]
        activity_id = row['id']
        activity_name = row['nombre_actividad']
        
        disabled = (activity_id == st.session_state.get('current_activity_id') or 
                    st.session_state.get('current_activity_name') == "Jornada Finalizada")
        
        # Emoji según actividad
        emoji_map = {
            'Seguimiento': '📞',
            'Caso Nuevo': '📋',
            'Reportaría': '📊',
            'Pausa': '☕',
            'Auxiliares': '🔧',
            'Reunión': '👥',
            'Salida': '🚪'
        }
        emoji = emoji_map.get(activity_name, '📌')
        
        if col.button(f"{emoji} {activity_name}", 
                     key=f"btn_{activity_id}", 
                     use_container_width=True, 
                     disabled=disabled,
                     type="primary" if not disabled else "secondary"):
            handle_activity_click(conn, user['id'], activity_id, activity_name)
            st.rerun()

    # Totales del día (Gráfico de barras)
    st.markdown('<div class="section-title">📊 Totales del día</div>', unsafe_allow_html=True)
    
    try:
        summary_df = queries.get_today_summary(conn, user['id'])
        
        if not summary_df.empty:
            summary_df['minutos_usados'] = summary_df['total_segundos'] / 60
            st.bar_chart(summary_df.set_index('nombre_actividad')['minutos_usados'], 
                        height=300)
        else:
            st.info("📭 Aún no hay actividades completadas hoy.")
    except Exception as e:
        st.warning(f"No se pudo cargar el resumen: {e}")

    # Línea de tiempo
    st.markdown('<div class="section-title">🕐 Línea de tiempo</div>', unsafe_allow_html=True)
    
    try:
        log_df = queries.get_today_log(conn, user['id'])
        
        if not log_df.empty:
            for _, row in log_df.iterrows():
                activity_name = row.get('nombre_actividad', 'N/A')
                duration = row.get('duracion', 'En curso')
                color = get_activity_color(activity_name)
                
                # Obtener emoji
                emoji_map = {
                    'Seguimiento': '📞',
                    'Caso Nuevo': '📋',
                    'Reportaría': '📊',
                    'Pausa': '☕',
                    'Auxiliares': '🔧',
                    'Reunión': '👥',
                    'Salida': '🚪'
                }
                emoji = emoji_map.get(activity_name, '📌')
                
                st.markdown(f"""
                <div class="timeline-item" style="background-color: {color};">
                    <div class="timeline-activity">{emoji} {activity_name}</div>
                    <div class="timeline-time">{duration}</div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("📭 Sin registros hoy.")
    except Exception as e:
        st.warning(f"No se pudo cargar el histórico: {e}")

    # Auto-refresh solo si hay actividad activa y sin errores
    # Nota: El cronómetro ya no necesita refresh porque funciona con JavaScript
    # Solo refrescamos ocasionalmente para actualizar datos de BD
    if st.session_state.get('current_registro_id') and 'last_error' not in st.session_state:
        time.sleep(30)  # Refresh cada 30 segundos solo para sincronizar BD
        st.rerun()
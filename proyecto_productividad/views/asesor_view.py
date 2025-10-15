import streamlit as st
from core import queries
from datetime import datetime
import time
import pandas as pd

def format_timedelta(td):
    """Formatea un objeto timedelta a HH:MM:SS."""
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"

def handle_activity_click(conn, user_id, new_activity_id, new_activity_name):
    """
    Lógica central para detener la actividad anterior e iniciar la nueva.
    ✅ CORREGIDO: Mejor manejo de estado y caché
    """
    now = datetime.now()

    # 1. Detener actividad actual (si existe)
    if st.session_state.get('current_registro_id'):
        try:
            queries.stop_activity(conn, st.session_state['current_registro_id'], now)
        except Exception as e:
            st.error(f"Error al detener actividad: {e}")
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
            st.success("Has marcado tu Salida. ¡Jornada finalizada!")
        except Exception as e:
            st.error(f"Error al marcar salida: {e}")
            return
    
    # 3. Iniciar nueva actividad (si no es "Salida")
    else:
        try:
            new_reg_id = queries.start_activity(conn, user_id, new_activity_id, now)
            
            st.session_state['current_activity_id'] = new_activity_id
            st.session_state['current_activity_name'] = new_activity_name
            st.session_state['current_start_time'] = now
            st.session_state['current_registro_id'] = new_reg_id
            
            st.success(f"✅ Actividad iniciada: {new_activity_name}")
        except Exception as e:
            st.error(f"Error al iniciar actividad: {e}")
            return

    # ✅ Limpiar SOLO el caché de queries, no todo
    st.cache_data.clear()

def show_asesor_dashboard(conn):
    user = st.session_state['user_info']
    st.title(f"Panel de Asesor: {user['nombre_completo']}")
    st.caption(f"Campaña: {user.get('campaña_nombre', 'N/A')}")
    st.divider()

    # --- Inicializar estado si no existe ---
    if 'current_activity_id' not in st.session_state:
        st.session_state['current_activity_id'] = None
    if 'current_activity_name' not in st.session_state:
        st.session_state['current_activity_name'] = '---'
    if 'current_start_time' not in st.session_state:
        st.session_state['current_start_time'] = None
    if 'current_registro_id' not in st.session_state:
        st.session_state['current_registro_id'] = None

    # --- Sección del Cronómetro ---
    st.subheader("Actividad Actual")
    placeholder = st.empty()
    
    with placeholder.container():
        col1, col2 = st.columns(2)
        col1.metric("Actividad", st.session_state.get('current_activity_name', '---'))
        
        timer_str = "00:00:00"
        if st.session_state.get('current_start_time'):
            start_time = st.session_state['current_start_time']
            if start_time.tzinfo:
                now = datetime.now(start_time.tzinfo)
            else:
                now = datetime.now()
                
            elapsed = now - start_time
            timer_str = format_timedelta(elapsed)
            
        col2.metric("Tiempo Transcurrido", timer_str)

    st.divider()

    # --- Sección de Botones de Actividad ---
    st.subheader("Registrar Actividad")
    
    try:
        activities_df = queries.get_active_activities(conn)
    except Exception as e:
        st.error(f"Error al cargar actividades: {e}")
        return
    
    if activities_df.empty:
        st.warning("No hay actividades disponibles.")
        return
    
    # Definimos las columnas para los botones
    num_cols = 4
    cols = st.columns(num_cols)
    
    for index, row in activities_df.iterrows():
        col = cols[index % num_cols]
        activity_id = row['id']
        activity_name = row['nombre_actividad']
        
        # ✅ Deshabilitar el botón si es la actividad actual o si la jornada terminó
        disabled = (activity_id == st.session_state.get('current_activity_id') or 
                    st.session_state.get('current_activity_name') == "Jornada Finalizada")
        
        if col.button(activity_name, key=f"btn_{activity_id}", use_container_width=True, disabled=disabled):
            handle_activity_click(conn, user['id'], activity_id, activity_name)
            st.rerun()

    st.divider()

    # --- Sección de Estadísticas del Día ---
    st.subheader("Resumen del Día")
    
    try:
        summary_df = queries.get_today_summary(conn, user['id'])
        
        if not summary_df.empty:
            summary_df['minutos_usados'] = summary_df['total_segundos'] / 60
            st.bar_chart(summary_df.set_index('nombre_actividad')['minutos_usados'])
        else:
            st.info("Aún no hay actividades completadas hoy.")
    except Exception as e:
        st.warning(f"No se pudo cargar el resumen: {e}")

    st.subheader("Histórico del Día")
    
    try:
        log_df = queries.get_today_log(conn, user['id'])
        
        if not log_df.empty:
            st.dataframe(log_df, use_container_width=True, hide_index=True)
        else:
            st.info("Sin registros hoy.")
    except Exception as e:
        st.warning(f"No se pudo cargar el histórico: {e}")

    # --- Lógica de "Tiempo Real" ---
    # ✅ Solo rerun si hay una actividad en progreso
    if st.session_state.get('current_registro_id'):
        time.sleep(1)
        st.rerun()
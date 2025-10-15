import streamlit as st
from core import queries
from datetime import datetime

def show_supervisor_dashboard(conn):
    user = st.session_state['user_info']
    st.title(f"Panel de Supervisor: {user['nombre_completo']}")
    st.subheader(f"Viendo Campaña: {user.get('campaña_nombre', 'N/A')}")
    st.divider()

    # Filtro de fecha
    selected_date = st.date_input("Seleccionar Fecha", datetime.today())

    if selected_date:
        campaña_id = user['campaña_id']
        data_df = queries.get_supervisor_dashboard(conn, campaña_id, selected_date)
        
        if data_df.empty:
            st.warning(f"No se encontraron datos para la campaña '{user['campaña_nombre']}' en la fecha {selected_date}.")
        else:
            st.info("El 'Tiempo Total Jornada' se calcula desde el 'Ingreso' hasta la 'Salida'. El 'Tiempo Efectivo' excluye 'Breaks'.")
            
            # Estilo gerencial: métricas clave
            total_asesores = data_df.shape[0]
            
            col1, col2 = st.columns(2)
            col1.metric("Asesores en Campaña", total_asesores)
            col2.metric("Fecha de Reporte", selected_date.strftime("%Y-%m-%d"))

            st.dataframe(data_df, use_container_width=True)

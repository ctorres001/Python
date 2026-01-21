# 📦 BLOQUE 1: Encabezado, logo, paleta de colores y configuración
import streamlit as st
import pandas as pd
from PIL import Image
from io import BytesIO
import base64
import os
from datetime import datetime
import locale

# Establecer idioma español para nombres de meses
try:
    locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
except:
    locale.setlocale(locale.LC_TIME, 'es_ES')

st.set_page_config(
    layout="wide",
    page_title="Reporte General de Pendientes de Entrega FNB"
)

COLOR_VERDE = "#00A34B"
COLOR_AZUL = "#00A1DE"

logo_path = r"D:\\FNB\\Reportes\\19. Reportes IBR\\01. Pendientes de Entrega\\Logo\\logo.jpg"

def logo_base64(ruta_imagen):
    if os.path.exists(ruta_imagen):
        img = Image.open(ruta_imagen)
        buffered = BytesIO()
        img.save(buffered, format="JPEG")
        return base64.b64encode(buffered.getvalue()).decode()
    else:
        return None

logo_b64 = logo_base64(logo_path)

st.markdown(f"""
    <div style="display: flex; justify-content: space-between; align-items: center; padding: 10px 0 0 10px;">
        <div style="font-size:22px; font-weight:bold; color:{COLOR_VERDE};">
            Reporte General de Pendientes de Entrega FNB
        </div>
        <div style="padding-right: 10px;">
            {'<img src="data:image/jpeg;base64,' + logo_b64 + '" style="height:45px;">' if logo_b64 else ''}
        </div>
    </div>
    <hr style="margin-top:5px; margin-bottom:10px; border:1px solid {COLOR_AZUL};">
""", unsafe_allow_html=True)

# BLOQUE 1B: Seguridad por correo
ruta_correos = r"D:\\FNB\\Reportes\\19. Reportes IBR\\01. Pendientes de Entrega\\Correos\\correos_autorizados.csv"

@st.cache_data
def cargar_correos_autorizados(ruta_csv):
    if os.path.exists(ruta_csv):
        df_correos = pd.read_csv(ruta_csv)
        if "correo" in df_correos.columns:
            return df_correos["correo"].astype(str).str.lower().tolist()
    return []

correos_autorizados = cargar_correos_autorizados(ruta_correos)

with st.sidebar:
    correo_ingresado = st.text_input("Ingresa tu correo para acceder:").strip().lower()
    if correo_ingresado == "":
        st.stop()
    if correo_ingresado not in correos_autorizados:
        st.error("⛔ Acceso denegado. Tu correo no está autorizado.")
        st.stop()

    st.markdown(f"<h4 style='color:{COLOR_AZUL}; margin-bottom:0;'>Filtros</h4><hr style='margin-top:2px;'>", unsafe_allow_html=True)

    vista_actual = st.selectbox(
        "Vista:",
        ["General", "Por Canal de Venta", "Por Responsable de Venta", "Por Aliado Comercial", "Por Mes"],
        key="vista_actual"
    )

    if st.button("🪹 Limpiar filtros"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

    ruta_excel = r"D:\\FNB\\Reportes\\19. Reportes IBR\\01. Pendientes de Entrega\\Resumen General\\Resumen General Pendientes de Entrega FNB.xlsx"

    @st.cache_data(show_spinner="🗕 Cargando datos del reporte...")
    def cargar_datos_excel(ruta):
        if os.path.exists(ruta):
            df = pd.read_excel(ruta, sheet_name="Sheet1")
            return df
        else:
            st.error(f"⚠️ No se encontró el archivo en la ruta: {ruta}")
            return pd.DataFrame()

    df = cargar_datos_excel(ruta_excel)
    if df.empty:
        st.stop()

    df = df[df["ESTADO"] == "PENDIENTE DE ENTREGA"].copy()
    df["FECHA VENTA"] = pd.to_datetime(df["FECHA VENTA"], errors="coerce")
    df["AÑO"] = df["FECHA VENTA"].dt.year
    df["MES_NUM"] = df["FECHA VENTA"].dt.month
    df["MES_NOMBRE"] = df["FECHA VENTA"].dt.strftime('%B').str.capitalize()

    # 🔔 Alerta de meses anteriores
    mes_actual = datetime.now().month
    df_anteriores = df[df["MES_NUM"] < mes_actual - 1]
    if not df_anteriores.empty:
        alerta = df_anteriores.groupby("MES_NOMBRE").size().reset_index(name="Cantidad")
        meses_alerta = ", ".join(f"{row['MES_NOMBRE']} ({row['Cantidad']})" for _, row in alerta.iterrows())
        st.warning(f"🔔 Tienes ventas pendientes desde los siguientes meses: {meses_alerta}")

    anio = st.selectbox("Año:", sorted(df["AÑO"].dropna().unique()), key="AÑO")
    df_filtrado = df[df["AÑO"] == anio]

    meses_disponibles = (
        df_filtrado[["MES_NUM", "MES_NOMBRE"]]
        .drop_duplicates()
        .sort_values("MES_NUM")
    )
    mes_nombre = st.selectbox("Mes:", ["Todos los meses"] + meses_disponibles["MES_NOMBRE"].tolist(), key="MES_NOMBRE")
    if mes_nombre != "Todos los meses":
        df_filtrado = df_filtrado[df_filtrado["MES_NOMBRE"] == mes_nombre]

    canal_disponibles = sorted(df_filtrado["CANAL_VENTA"].dropna().unique())
    canal = st.selectbox("Canal de Venta:", ["Todos"] + canal_disponibles, key="CANAL_VENTA")
    if canal != "Todos":
        df_filtrado = df_filtrado[df_filtrado["CANAL_VENTA"] == canal]

    responsables = sorted(df_filtrado["RESPONSABLE DE VENTA"].dropna().unique())
    responsable = st.selectbox("Responsable de Venta:", ["Todos"] + responsables, key="RESPONSABLE DE VENTA")
    if responsable != "Todos":
        df_filtrado = df_filtrado[df_filtrado["RESPONSABLE DE VENTA"] == responsable]

    aliados = sorted(df_filtrado["ALIADO COMERCIAL"].dropna().unique())
    aliado = st.selectbox("Aliado Comercial:", ["Todos"] + aliados, key="ALIADO COMERCIAL")
    if aliado != "Todos":
        df_filtrado = df_filtrado[df_filtrado["ALIADO COMERCIAL"] == aliado]

    sedes = sorted(df_filtrado["SEDE"].dropna().unique())
    sede = st.selectbox("Sede:", ["Todos"] + sedes, key="SEDE")
    if sede != "Todos":
        df_filtrado = df_filtrado[df_filtrado["SEDE"] == sede]

    cardif_opcion = st.radio("Filtrar por Aliado CARDIF:", ("Todos", "Solo CARDIF", "Excluir CARDIF"), key="cardif_filtro")
    if cardif_opcion == "Solo CARDIF":
        df_filtrado = df_filtrado[df_filtrado["ALIADO COMERCIAL"] == "CARDIF"]
    elif cardif_opcion == "Excluir CARDIF":
        df_filtrado = df_filtrado[df_filtrado["ALIADO COMERCIAL"] != "CARDIF"]

# BLOQUE 2: Mostrar datos según vista seleccionada
if df_filtrado.empty:
    st.warning("⚠️ No hay datos para mostrar con los filtros seleccionados.")
else:
    df_vista = df_filtrado.copy()
    if vista_actual == "Por Canal de Venta":
        df_vista = df_vista.groupby("CANAL_VENTA")["IMPORTE (S./)"].agg(["sum", "count"]).reset_index()
    elif vista_actual == "Por Responsable de Venta":
        df_vista = df_vista.groupby("RESPONSABLE DE VENTA")["IMPORTE (S./)"].agg(["sum", "count"]).reset_index()
    elif vista_actual == "Por Aliado Comercial":
        df_vista = df_vista.groupby("ALIADO COMERCIAL")["IMPORTE (S./)"].agg(["sum", "count"]).reset_index()
    elif vista_actual == "Por Mes":
        df_vista = df_vista.groupby(["MES_NUM", "MES_NOMBRE"])["IMPORTE (S./)"].agg(["sum", "count"]).reset_index()
        df_vista = df_vista.sort_values("MES_NUM").drop(columns=["MES_NUM"])
    else:
        df_vista = df_vista.groupby([
            "CANAL_VENTA", "RESPONSABLE DE VENTA", "ALIADO COMERCIAL", "SEDE"]
        )["IMPORTE (S./)"].agg(["sum", "count"]).reset_index()

    df_vista.rename(columns={"sum": "Importe", "count": "Transacciones"}, inplace=True)
    total_importe = df_vista["Importe"].sum()
    total_transacciones = df_vista["Transacciones"].sum()

    df_vista = df_vista.sort_values(by=["Importe", "Transacciones"], ascending=False)

    st.dataframe(
        df_vista.style
        .format({
            "Importe": lambda x: f"S/ {x:,.0f}",
            "Transacciones": "{:,.0f}"
        })
        .set_properties(subset=["Importe"], **{"text-align": "right"}),
        use_container_width=True,
        height=600
    )

    st.markdown(f"""
    <div style="text-align:right; font-weight:bold; font-size:15px; padding:5px 10px;">
        TOTAL &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Importe: S/ {total_importe:,.0f} &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Transacciones: {total_transacciones:,}
    </div>
    """, unsafe_allow_html=True)

    # BLOQUE 3: Botón de exportación
    def exportar_excel(df_export):
        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df_export.to_excel(writer, index=False, sheet_name="Reporte")
            workbook = writer.book
            worksheet = writer.sheets["Reporte"]
            for i, col in enumerate(df_export.columns):
                max_len = max(df_export[col].astype(str).map(len).max(), len(col)) + 2
                worksheet.set_column(i, i, max_len)
        output.seek(0)
        return output

    excel_bytes = exportar_excel(df_filtrado)
    st.download_button(
        label="🗕 Exportar datos filtrados a Excel",
        data=excel_bytes,
        file_name="reporte_pendientes_entrega.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

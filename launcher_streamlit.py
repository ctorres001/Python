"""
Panel local para ejecutar scripts de reportes FNB con Streamlit.
Uso:
    streamlit run launcher_streamlit.py

Requisitos:
    pip install streamlit
"""

import subprocess
from pathlib import Path
import streamlit as st

# Configuración general
title = "Reportes Calidda FNB"
subtitle = "Elaborado por IBR - Carlos Torres"

BASE_DIR = Path(__file__).resolve().parent

# Grupos de scripts
CATEGORIES: dict[str, dict[str, Path]] = {
    "Reportes": {
        "Pendientes Entrega Proveedor": BASE_DIR / "02.1.PendientesEntregaProveedor.py",
        "Pendientes Entrega Canal": BASE_DIR / "02.2.PendientesEntregaCanal.py",
    },
    "Cargar Base de Datos": {
        # Agrega aquí scripts de carga, por ejemplo:
        # "10.1 - Carga BD Colocaciones SQL": BASE_DIR / "10.1.CargaBDColocacionesSQL.py",
    },
    "Utilitarios": {
        # Agrega aquí utilitarios, por ejemplo:
        "Estructura Reporte Diario": BASE_DIR / "01.2.RestructuraFNB.py",
    },
}


def run_script(script_path: Path, args: list[str] = None) -> tuple[str, str]:
    """Ejecuta un script Python y devuelve (stdout, stderr)."""
    if not script_path.exists():
        return "", f"No se encontró el archivo: {script_path}"

    try:
        cmd = ["python", str(script_path)]
        if args:
            cmd.extend(args)
        
        result = subprocess.run(
            cmd,
            cwd=BASE_DIR,
            capture_output=True,
            text=True,
            check=False,
        )
        return result.stdout, result.stderr
    except Exception as exc:  # pragma: no cover
        return "", f"Error ejecutando {script_path.name}: {exc}"


def render_section(section_title: str, scripts: dict[str, Path]) -> None:
    st.subheader(section_title)
    if not scripts:
        st.info("Sin scripts configurados en esta sección.")
        return

    cols = st.columns(2)
    for idx, (label, path) in enumerate(scripts.items()):
        with cols[idx % 2]:
            btn_key = f"btn-{section_title}-{label}"
            
            # Para scripts que requieren archivo de entrada (como Estructura Reporte Diario)
            if "Estructura Reporte" in label:
                uploaded_file = st.file_uploader(
                    f"Seleccione archivo Excel para {label}", 
                    type=["xlsx"],
                    key=f"upload-{btn_key}"
                )
                
                if st.button(f"Ejecutar {label}", use_container_width=True, key=btn_key):
                    if uploaded_file is not None:
                        # Mostrar barra de progreso
                        progress_bar = st.progress(0, text="Iniciando procesamiento...")
                        
                        # Guardar archivo temporal
                        temp_path = BASE_DIR / f"temp_{uploaded_file.name}"
                        with open(temp_path, "wb") as f:
                            f.write(uploaded_file.getbuffer())
                        
                        progress_bar.progress(20, text="Ejecutando script...")
                        
                        stdout, stderr = run_script(path, [str(temp_path)])
                        
                        progress_bar.progress(90, text="Finalizando...")
                        
                        # Limpiar archivo temporal
                        if temp_path.exists():
                            temp_path.unlink()
                        
                        progress_bar.progress(100, text="Completado")
                        
                        # Verificar si hay éxito o error en stdout
                        if "EXITO:" in stdout:
                            # Extraer número de registros
                            registros = stdout.split("EXITO:")[1].strip()
                            st.success(f"Procesamiento completado exitosamente. Registros procesados: {registros}")
                            progress_bar.empty()
                        elif "ERROR:" in stdout:
                            # Extraer mensaje de error
                            error_msg = stdout.split("ERROR:")[1].strip()
                            st.error(f"Error: {error_msg}")
                            progress_bar.empty()
                            with st.expander("Ver detalles del error"):
                                st.code(stderr, language="text")
                        else:
                            st.warning("El proceso finalizó pero no se pudo determinar el resultado")
                            progress_bar.empty()
                            with st.expander("Ver salida completa"):
                                st.code(stdout or "<sin salida>", language="text")
                                if stderr:
                                    st.code(stderr, language="text")
                    else:
                        st.warning("Por favor, seleccione un archivo Excel primero")
            else:
                # Scripts que no requieren entrada de archivo
                if st.button(f"Ejecutar {label}", use_container_width=True, key=btn_key):
                    progress_bar = st.progress(0, text="Ejecutando...")
                    stdout, stderr = run_script(path)
                    progress_bar.progress(100, text="Completado")
                    
                    if "ERROR" in stderr.upper() or "ERROR" in stdout.upper():
                        st.error("Se encontraron errores durante la ejecución")
                        with st.expander("Ver detalles"):
                            st.code(stdout or "<sin salida>", language="text")
                            if stderr:
                                st.code(stderr, language="text")
                    else:
                        st.success(f"{label} ejecutado correctamente")
                    
                    progress_bar.empty()


# UI
st.set_page_config(page_title=title, layout="centered")
st.title(title)
st.caption(subtitle)
st.divider()

st.markdown("### Panel de ejecución")
st.write("Selecciona el script a ejecutar en cada bloque. Se mostrarán los logs al terminar.")

# Renderizar secciones
for section_name, scripts in CATEGORIES.items():
    render_section(section_name, scripts)
    st.divider()

st.caption("Para agregar más botones, suma entradas en el dict CATEGORIES.")

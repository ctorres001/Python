@echo off
REM Script para lanzar el panel de Reportes Calidda FNB (1 sola pestaña y cerrar esta ventana)
REM Cambiar a la carpeta del proyecto
cd /d "D:\FNB\Proyectos\Python"

REM Intentar usar pythonw.exe del entorno virtual para no dejar consola abierta
set "VENV_PYTHONW=.venv1\Scripts\pythonw.exe"
if exist "%VENV_PYTHONW%" (
	set "PYW=%VENV_PYTHONW%"
) else (
	REM Fallback al pythonw del sistema
	set "PYW=pythonw.exe"
)

REM Iniciar Streamlit en modo headless para evitar que abra el navegador por su cuenta
start "" "%PYW%" -m streamlit run launcher_streamlit.py --server.headless true

REM Esperar unos segundos a que el servidor levante
timeout /t 3 /nobreak >nul

REM Abrir una sola pestaña del navegador hacia el panel
start "" http://localhost:8501

REM Cerrar esta ventana (el servidor queda ejecutándose en background)
exit

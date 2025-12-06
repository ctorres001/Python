import subprocess
import sys
import time
from pathlib import Path

# Script para INICIAR PostgreSQL
comando = r"D:\FNB\Proyectos\PostgreSQL\postgresql-18.1-1-windows-x64-binaries\pgsql\bin\pg_ctl.exe"
data_dir = r"D:\FNB\Proyectos\PostgreSQL\data"
logfile = r"D:\FNB\Proyectos\PostgreSQL\logfile.log"

try:
    # Crear log si no existe
    Path(logfile).parent.mkdir(parents=True, exist_ok=True)
    Path(logfile).touch(exist_ok=True)

    # Verificar si ya está corriendo
    print("Verificando estado de PostgreSQL...")
    status = subprocess.run(
        [comando, "-D", data_dir, "status"],
        capture_output=True,
        text=True,
        timeout=3,
        creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0
    )

    if status.returncode == 0:
        print("ℹ PostgreSQL ya está corriendo")
        print(status.stdout.strip())
        sys.exit(0)

    # Iniciar en segundo plano sin esperar
    print("Iniciando PostgreSQL en segundo plano...")
    subprocess.Popen(
        [comando, "-D", data_dir, "-l", logfile, "start"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0
    )

    # Esperar 3 segundos y verificar
    print("Esperando 3 segundos...")
    time.sleep(3)
    
    final = subprocess.run(
        [comando, "-D", data_dir, "status"],
        capture_output=True,
        text=True,
        timeout=3,
        creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0
    )
    
    if final.returncode == 0:
        print("✓ PostgreSQL iniciado correctamente")
        print(final.stdout.strip())
    else:
        print("⚠ PostgreSQL iniciándose... verifica en unos segundos")
        print(f"Revisa: {logfile}")

except Exception as e:
    print(f"✗ Error: {e}")
    sys.exit(1)

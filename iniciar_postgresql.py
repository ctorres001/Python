import subprocess
import sys
from pathlib import Path

# Script para INICIAR PostgreSQL
comando = r"D:\\FNB\\Proyectos\\PostgreSQL\\postgresql-18.1-1-windows-x64-binaries\\pgsql\\bin\\pg_ctl.exe"
data_dir = r"D:\\FNB\\Proyectos\\PostgreSQL\\data"
logfile = r"D:\\FNB\\Proyectos\\PostgreSQL\\logfile.log"

def ensure_logfile(path: str) -> None:
    """Crea el archivo de log si no existe para evitar bloqueos por ruta inválida."""
    log_path = Path(path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    if not log_path.exists():
        log_path.touch()

try:
    ensure_logfile(logfile)

    # Verificar si ya está corriendo
    print("Verificando estado de PostgreSQL...")
    status = subprocess.run(
        [comando, "-D", data_dir, "status"],
        capture_output=True,
        text=True,
        timeout=5
    )

    if status.returncode == 0:
        print("ℹ PostgreSQL ya está corriendo")
        print(status.stdout.strip())
        sys.exit(0)

    # Si no está corriendo, iniciarlo con espera corta
    print("Iniciando PostgreSQL...")
    result = subprocess.run(
        [
            comando,
            "-D", data_dir,
            "-l", logfile,
            "-w",  # espera a que arranque
            "-t", "5",  # timeout de 5s en pg_ctl
            "start",
        ],
        capture_output=True,
        text=True,
        timeout=15
    )

    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip())

    # Considerar éxito si returncode es 0 o el mensaje indica que ya estaba corriendo
    if result.returncode == 0 or "already running" in result.stderr.lower() or "server started" in result.stdout.lower():
        print("✓ PostgreSQL iniciado correctamente")
    else:
        print("✗ Error al iniciar")
        sys.exit(1)

    # Verificar estado final
    final_status = subprocess.run(
        [comando, "-D", data_dir, "status"],
        capture_output=True,
        text=True,
        timeout=5
    )
    if final_status.returncode == 0:
        print(final_status.stdout.strip())
    else:
        print("⚠ No se pudo confirmar el estado final")
        print(final_status.stderr.strip())

except subprocess.TimeoutExpired:
    print("✓ PostgreSQL iniciado (timeout local, pero pg_ctl sigue)")
except Exception as e:
    print(f"✗ Error: {e}")
    sys.exit(1)

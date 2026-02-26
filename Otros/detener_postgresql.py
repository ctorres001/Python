import subprocess
import sys

# Script para DETENER PostgreSQL
comando = r"D:\FNB\Proyectos\PostgreSQL\postgresql-18.1-1-windows-x64-binaries\pgsql\bin\pg_ctl.exe"
data_dir = r"D:\FNB\Proyectos\PostgreSQL\data"

try:
    # Verificar si está corriendo
    print("Verificando estado de PostgreSQL...")
    status = subprocess.run(
        [comando, "-D", data_dir, "status"],
        capture_output=True,
        text=True,
        timeout=5
    )

    if status.returncode != 0:
        print("ℹ PostgreSQL ya está detenido")
        sys.exit(0)

    # Si está corriendo, detenerlo de forma rápida
    print("Deteniendo PostgreSQL...")
    result = subprocess.run(
        [
            comando,
            "-D", data_dir,
            "-m", "fast",  # modo de parada rápida
            "-t", "5",      # timeout para pg_ctl
            "stop",
        ],
        capture_output=True,
        text=True,
        timeout=10
    )

    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip())

    if result.returncode == 0:
        print("✓ PostgreSQL detenido correctamente")
    else:
        print("✗ Error al detener")
        sys.exit(1)

except subprocess.TimeoutExpired:
    print("⏱ Tiempo de espera agotado")
except Exception as e:
    print(f"✗ Error: {e}")
    sys.exit(1)

import os
import subprocess
import datetime

# Configuración de conexión
PG_USER = "postgres"
PG_PASSWORD = "ibr2025"
PG_DB = "bd_calidda_fnb"
PG_HOST = "localhost"
PG_PORT = "5432"

# Ruta donde está pg_dump.exe
PG_PATH = r"D:\FNB\Proyectos\PostgreSQL\postgresql-18.1-1-windows-x64-binaries\pgsql\bin"

# Carpeta destino en OneDrive
BACKUP_DIR = r"C:\Users\carlos.torres2\OneDrive - IBR PERU\Backup BD"

def backup_postgres():
    # Crear nombre de archivo con fecha
    fecha = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = os.path.join(BACKUP_DIR, f"{PG_DB}_{fecha}.backup")

    # Ruta al ejecutable pg_dump
    pg_dump_path = os.path.join(PG_PATH, "pg_dump.exe")

    # Configurar variable de entorno para la contraseña
    os.environ["PGPASSWORD"] = PG_PASSWORD

    # Comando para ejecutar pg_dump
    command = [
        pg_dump_path,
        "-h", PG_HOST,
        "-p", PG_PORT,
        "-U", PG_USER,
        "-F", "c",   # formato custom
        "-b",        # incluye blobs
        "-v",        # verbose
        "-f", backup_file,
        PG_DB
    ]

    try:
        print(f"Generando backup en: {backup_file}")
        subprocess.run(command, check=True)
        print("✅ Backup completado con éxito")
    except subprocess.CalledProcessError as e:
        print("❌ Error al generar el backup:", e)

if __name__ == "__main__":
    backup_postgres()

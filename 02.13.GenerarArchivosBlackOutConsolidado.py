import pandas as pd
import os
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Rutas
ruta_origen = r"C:\Users\carlos.torres2\Gas Natural de Lima y Callao S.A. (GNLC)\FNB - 99. BlackOut\09-2025\Aliados"
ruta_destino = r"D:\FNB\Reportes\19. Reportes IBR\12. Pendientes de Entrega Blackout\Consolidado"
carpeta_excluir = "z_OTROS"

# Crear carpeta destino si no existe
os.makedirs(ruta_destino, exist_ok=True)

# Lista para almacenar todos los DataFrames
datos_consolidados = []
archivos_procesados = []
archivos_con_error = []

print("Iniciando consolidación de archivos Excel...")
print(f"Ruta origen: {ruta_origen}")
print(f"Excluyendo carpeta: {carpeta_excluir}\n")

# Recorrer todas las carpetas y subcarpetas recursivamente
for root, dirs, files in os.walk(ruta_origen):
    # Excluir la carpeta z_OTROS
    if carpeta_excluir in root:
        continue
    
    # Obtener la ruta relativa desde la carpeta origen
    ruta_relativa = os.path.relpath(root, ruta_origen)
    
    # Buscar archivos Excel en la carpeta actual
    archivos_excel = [f for f in files if f.endswith(('.xlsx', '.xls', '.xlsm'))]
    
    if archivos_excel:
        print(f"\nProcesando carpeta: {ruta_relativa}")
        
        for archivo in archivos_excel:
            ruta_archivo = os.path.join(root, archivo)
            
            try:
                # Leer solo la hoja "Base"
                df = pd.read_excel(ruta_archivo, sheet_name='Base')
                
                # Solo procesar si tiene datos
                if not df.empty:
                    # Agregar columnas de referencia
                    df['Carpeta_Origen'] = ruta_relativa
                    df['Archivo_Origen'] = archivo
                    
                    datos_consolidados.append(df)
                    print(f"  ✓ {archivo} - Hoja: Base ({len(df)} filas)")
                else:
                    print(f"  ⚠ {archivo} - Hoja 'Base' está vacía")
                
                archivos_procesados.append(f"{ruta_relativa}/{archivo}")
            
            except Exception as e:
                error_msg = f"{ruta_relativa}/{archivo}: {str(e)}"
                archivos_con_error.append(error_msg)
                print(f"  ✗ Error al procesar {archivo}: {str(e)}")

print("\n" + "="*70)

# Consolidar todos los DataFrames
if datos_consolidados:
    df_consolidado = pd.concat(datos_consolidados, ignore_index=True)
    
    # Generar nombre de archivo con fecha y hora
    fecha_hora = datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre_archivo_salida = f"Consolidado_Aliados_{fecha_hora}.xlsx"
    ruta_archivo_salida = os.path.join(ruta_destino, nombre_archivo_salida)
    
    # Guardar el archivo consolidado
    with pd.ExcelWriter(ruta_archivo_salida, engine='openpyxl') as writer:
        df_consolidado.to_excel(writer, sheet_name='Consolidado', index=False)
    
    print(f"\n✓ CONSOLIDACIÓN EXITOSA")
    print(f"Archivo generado: {nombre_archivo_salida}")
    print(f"Ubicación: {ruta_destino}")
    print(f"Total de filas consolidadas: {len(df_consolidado):,}")
    print(f"Total de columnas: {len(df_consolidado.columns)}")
    print(f"Archivos procesados: {len(archivos_procesados)}")
    
    if archivos_con_error:
        print(f"\n⚠ Archivos con errores: {len(archivos_con_error)}")
        for error in archivos_con_error:
            print(f"  - {error}")
else:
    print("\n✗ No se encontraron datos para consolidar")

print("\n" + "="*70)
print("Proceso finalizado")
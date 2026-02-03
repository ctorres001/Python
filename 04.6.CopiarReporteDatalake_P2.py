import shutil
import os
from pathlib import Path
import logging
from datetime import datetime

def copiar_archivo_reporte():
    """
    Copia el archivo Replica_Automatica_ReporteVentas.xlsx desde la ruta origen 
    hacia la ruta destino con timestamp en el nombre.
    """
    
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Definir rutas
    ruta_origen = Path(r"C:\Users\carlos.torres2\Gas Natural de Lima y Callao S.A. (GNLC)\FNB - 03. Integridad&Limpieza\00. Conciliacion DataLake - Reporte Ventas\AnálisisMicro\Replica_Automatica_ReporteVentas.xlsx")
    
    # Rutas destino
    ruta_destino_dir_1 = Path(r"D:\FNB\Reportes\19. Reportes IBR\10. Datalake Calidda FNB")
    ruta_destino_dir_2 = Path(r"C:\Users\carlos.torres2\OneDrive - IBR PERU\Reporte de Ventas")
    
    # Crear nombre con timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre_archivo = f"Replica_Automatica_ReporteVentas_{timestamp}.xlsx"
    
    rutas_destino = [
        ruta_destino_dir_1 / nombre_archivo,
        ruta_destino_dir_2 / nombre_archivo
    ]
    
    try:
        # Verificar que el archivo origen existe
        if not ruta_origen.exists():
            logging.error(f"El archivo origen no existe: {ruta_origen}")
            return False
            
        # Realizar la copia a ambos destinos
        copias_exitosas = 0
        total_destinos = len(rutas_destino)
        
        for i, ruta_destino in enumerate(rutas_destino, 1):
            try:
                # Crear directorio destino si no existe
                ruta_destino.parent.mkdir(parents=True, exist_ok=True)
                logging.info(f"Directorio destino {i} verificado/creado: {ruta_destino.parent}")
                
                # Realizar la copia
                shutil.copy2(ruta_origen, ruta_destino)
                logging.info(f"Archivo copiado exitosamente a destino {i}:")
                logging.info(f"  Destino: {ruta_destino}")
                
                # Verificar que la copia se realizó correctamente
                if ruta_destino.exists():
                    tamaño_origen = ruta_origen.stat().st_size
                    tamaño_destino = ruta_destino.stat().st_size
                    
                    if tamaño_origen == tamaño_destino:
                        logging.info(f"Verificación exitosa para destino {i}: Los tamaños coinciden")
                        copias_exitosas += 1
                    else:
                        logging.warning(f"Los tamaños de archivo no coinciden en destino {i}")
                else:
                    logging.error(f"El archivo no se creó en el destino {i}")
            
            except Exception as e:
                logging.error(f"Error copiando a destino {i}: {e}")
                continue
        
        logging.info(f"Origen: {ruta_origen}")
        
        if copias_exitosas == total_destinos:
            logging.info(f"Todas las copias ({copias_exitosas}/{total_destinos}) completadas exitosamente")
            return True
        elif copias_exitosas > 0:
            logging.warning(f"Copias parcialmente exitosas ({copias_exitosas}/{total_destinos})")
            return True
        else:
            logging.error("Ninguna copia fue exitosa")
            return False
            
    except PermissionError as e:
        logging.error(f"Error de permisos: {e}")
        return False
    except FileNotFoundError as e:
        logging.error(f"Archivo no encontrado: {e}")
        return False
    except Exception as e:
        logging.error(f"Error inesperado: {e}")
        return False

if __name__ == "__main__":
    print("=== Script de Copia de Reporte de Ventas ===")
    
    resultado = copiar_archivo_reporte()
    
    if resultado:
        print("\n✅ Copia completada exitosamente")
    else:
        print("\n❌ Error en la copia. Revisa los logs para más detalles")
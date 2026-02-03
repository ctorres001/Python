import shutil
import os
from pathlib import Path
import logging
from datetime import datetime
import time

def copiar_archivo_reporte():
    """
    Copia el archivo Replica_Automatica_ReporteVentas.xlsx desde la ruta origen 
    hacia la ruta destino con timestamp en el nombre, y luego elimina las copias creadas.
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
    
    # Lista para almacenar las rutas de archivos copiados exitosamente
    archivos_copiados = []
    
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
                        # Agregar a la lista de archivos copiados exitosamente
                        archivos_copiados.append(ruta_destino)
                    else:
                        logging.warning(f"Los tamaños de archivo no coinciden en destino {i}")
                        # Si el tamaño no coincide, intentar eliminar el archivo defectuoso
                        try:
                            ruta_destino.unlink()
                            logging.info(f"Archivo defectuoso eliminado de destino {i}")
                        except Exception as e:
                            logging.error(f"Error eliminando archivo defectuoso en destino {i}: {e}")
                else:
                    logging.error(f"El archivo no se creó en el destino {i}")
            
            except Exception as e:
                logging.error(f"Error copiando a destino {i}: {e}")
                continue
        
        logging.info(f"Origen: {ruta_origen}")
        
        # Evaluar el resultado de las copias
        exito_copias = False
        if copias_exitosas == total_destinos:
            logging.info(f"Todas las copias ({copias_exitosas}/{total_destinos}) completadas exitosamente")
            exito_copias = True
        elif copias_exitosas > 0:
            logging.warning(f"Copias parcialmente exitosas ({copias_exitosas}/{total_destinos})")
            exito_copias = True
        else:
            logging.error("Ninguna copia fue exitosa")
            return False
        
        # Esperar un momento antes de eliminar (para asegurar que las operaciones de escritura terminen)
        if archivos_copiados:
            logging.info("Esperando 2 segundos antes de eliminar las copias...")
            time.sleep(2)
            
            # Eliminar las copias creadas
            eliminar_copias_exitosas = 0
            for archivo_copiado in archivos_copiados:
                try:
                    if archivo_copiado.exists():
                        archivo_copiado.unlink()
                        logging.info(f"Copia eliminada exitosamente: {archivo_copiado}")
                        eliminar_copias_exitosas += 1
                    else:
                        logging.warning(f"El archivo ya no existe para eliminar: {archivo_copiado}")
                        
                except PermissionError as e:
                    logging.error(f"Error de permisos al eliminar {archivo_copiado}: {e}")
                except Exception as e:
                    logging.error(f"Error inesperado al eliminar {archivo_copiado}: {e}")
            
            # Reporte final de eliminaciones
            total_archivos_copiados = len(archivos_copiados)
            if eliminar_copias_exitosas == total_archivos_copiados:
                logging.info(f"Todas las copias ({eliminar_copias_exitosas}/{total_archivos_copiados}) eliminadas exitosamente")
            elif eliminar_copias_exitosas > 0:
                logging.warning(f"Eliminación parcial de copias ({eliminar_copias_exitosas}/{total_archivos_copiados})")
            else:
                logging.error("No se pudieron eliminar las copias creadas")
                return False
        
        return exito_copias
            
    except PermissionError as e:
        logging.error(f"Error de permisos: {e}")
        # Intentar limpiar archivos copiados en caso de error
        limpiar_archivos_copiados(archivos_copiados)
        return False
    except FileNotFoundError as e:
        logging.error(f"Archivo no encontrado: {e}")
        return False
    except Exception as e:
        logging.error(f"Error inesperado: {e}")
        # Intentar limpiar archivos copiados en caso de error
        limpiar_archivos_copiados(archivos_copiados)
        return False

def limpiar_archivos_copiados(archivos_copiados):
    """
    Función auxiliar para limpiar archivos copiados en caso de error.
    """
    if not archivos_copiados:
        return
        
    logging.info("Intentando limpiar archivos copiados debido a error...")
    for archivo in archivos_copiados:
        try:
            if archivo.exists():
                archivo.unlink()
                logging.info(f"Archivo limpiado: {archivo}")
        except Exception as e:
            logging.error(f"Error limpiando archivo {archivo}: {e}")

if __name__ == "__main__":
    print("=== Script de Copia y Eliminación de Reporte de Ventas ===")
    print("Este script:")
    print("1. Copia el archivo origen a las carpetas destino")
    print("2. Verifica que las copias sean correctas")
    print("3. Elimina las copias creadas (mantiene solo el archivo original)")
    print()
    
    resultado = copiar_archivo_reporte()
    
    if resultado:
        print("\n✅ Proceso completado exitosamente")
        print("   - Archivo copiado y verificado")
        print("   - Copias eliminadas correctamente")
        print("   - Archivo original permanece intacto")
    else:
        print("\n❌ Error en el proceso. Revisa los logs para más detalles")
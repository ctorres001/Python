import os
import pandas as pd
from datetime import datetime, timedelta
import glob
import re
from pathlib import Path
import gc
import numpy as np
from typing import Tuple, Set, Optional
import logging

class ComparativoClientesOptimizado:
    def __init__(self, ruta_base, ruta_resultados, chunk_size=100000):
        self.ruta_base = Path(ruta_base)
        self.ruta_resultados = Path(ruta_resultados)
        self.ruta_resultados.mkdir(parents=True, exist_ok=True)
        self.chunk_size = chunk_size
        
        # Configurar logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Columnas que queremos mantener en los resultados
        self.columnas_resultado = [
            'Tipo Docum', 'N.I.F.1', 'Int.cial.', 'Nombre', 
            'Saldo Créd', 'Cta.Contr.', 'Distrito'
        ]
        
        # Estadísticas consolidadas
        self.estadisticas = []
        
        # Configuración de tipos de datos para optimizar memoria
        self.dtype_config = {
            'Tipo Docum': 'category',
            'N.I.F.1': 'string',
            'Int.cial.': 'string',
            'Nombre': 'string',
            'Saldo Créd': 'float32',
            'Cta.Contr.': 'string',
            'Distrito': 'category'
        }
        
    def extraer_fecha_archivo(self, nombre_archivo):
        """Extrae la fecha del nombre del archivo (formato YYYYMMDD)"""
        match = re.search(r'(\d{8})', nombre_archivo)
        if match:
            fecha_str = match.group(1)
            return datetime.strptime(fecha_str, '%Y%m%d')
        return None
    
    def detectar_encoding_y_separador(self, ruta_archivo, muestra_filas=10):
        """Detecta la codificación y el separador del archivo"""
        codificaciones = ['utf-8', 'latin-1', 'windows-1252', 'cp1252', 'iso-8859-1']
        separadores = ['\t', ';', '|', ',']
        
        encoding_correcto = 'utf-8'
        
        # Probar diferentes codificaciones
        for encoding in codificaciones:
            try:
                with open(ruta_archivo, 'r', encoding=encoding) as f:
                    # Saltar las primeras 8 filas
                    for _ in range(8):
                        next(f)
                    
                    # Leer algunas líneas de muestra
                    lineas_muestra = [f.readline().strip() for _ in range(muestra_filas)]
                    
                    if lineas_muestra and any(lineas_muestra):
                        encoding_correcto = encoding
                        self.logger.info(f"Codificación detectada: {encoding}")
                        break
                        
            except (UnicodeDecodeError, UnicodeError):
                continue
        
        # Detectar separador con la codificación correcta
        mejor_separador = '\t'
        max_columnas = 0
        
        try:
            with open(ruta_archivo, 'r', encoding=encoding_correcto) as f:
                # Saltar las primeras 8 filas
                for _ in range(8):
                    next(f)
                
                lineas_muestra = [f.readline().strip() for _ in range(muestra_filas)]
            
            for sep in separadores:
                columnas_por_linea = [len(linea.split(sep)) for linea in lineas_muestra if linea]
                if columnas_por_linea:
                    promedio_columnas = np.mean(columnas_por_linea)
                    if promedio_columnas > max_columnas:
                        max_columnas = promedio_columnas
                        mejor_separador = sep
        
        except Exception as e:
            self.logger.warning(f"Error al detectar separador: {e}")
        
        return encoding_correcto, mejor_separador
    
    def cargar_archivo_optimizado(self, ruta_archivo):
        """Carga un archivo TXT de forma optimizada para grandes volúmenes"""
        try:
            self.logger.info(f"Cargando archivo: {os.path.basename(ruta_archivo)}")
            
            # Detectar codificación y separador
            encoding, separador = self.detectar_encoding_y_separador(ruta_archivo)
            self.logger.info(f"Separador detectado: '{separador}', Codificación: {encoding}")
            
            # Cargar archivo en chunks para optimizar memoria
            chunks = []
            total_rows = 0
            
            # Primera pasada: obtener nombres de columnas
            try:
                # AÑADIR thousands=',' aquí también por consistencia
                df_sample = pd.read_csv(ruta_archivo, sep=separador, skiprows=8, nrows=1000, 
                                        encoding=encoding, low_memory=False, thousands=',') 
                df_sample = df_sample.dropna(axis=1, how='all')
                columnas = [col.strip() for col in df_sample.columns]
                
                # Configurar tipos de datos dinámicamente
                dtype_dinamico = {}
                for col in columnas:
                    col_clean = col.strip()
                    if col_clean in self.dtype_config:
                        dtype_dinamico[col] = self.dtype_config[col_clean]
                
                self.logger.info(f"Columnas detectadas: {len(columnas)}")
                self.logger.info(f"Columnas encontradas: {columnas[:10]}...")
                
            except Exception as e:
                self.logger.error(f"Error al leer columnas: {e}")
                return None
            
            # Cargar archivo en chunks
            try:
                chunk_reader = pd.read_csv(
                    ruta_archivo, 
                    sep=separador, 
                    skiprows=8,
                    encoding=encoding,
                    chunksize=self.chunk_size,
                    dtype=dtype_dinamico,
                    low_memory=False,
                    on_bad_lines='skip',
                    thousands=','  # <--- LÍNEA CLAVE AGREGADA AQUÍ
                )
                
                for i, chunk in enumerate(chunk_reader):
                    # Limpiar columnas vacías
                    chunk = chunk.dropna(axis=1, how='all')
                    chunk.columns = [col.strip() for col in chunk.columns]
                    
                    # Filtrar solo las columnas que necesitamos si existen
                    columnas_disponibles = [col for col in self.columnas_resultado if col in chunk.columns]
                    if columnas_disponibles:
                        chunk = chunk[columnas_disponibles]
                    
                    chunks.append(chunk)
                    total_rows += len(chunk)
                    
                    if (i + 1) % 10 == 0:
                        self.logger.info(f"Procesados {(i + 1) * self.chunk_size:,} registros...")
                
                # Concatenar todos los chunks
                if chunks:
                    df_completo = pd.concat(chunks, ignore_index=True)
                    self.logger.info(f"Archivo cargado exitosamente: {total_rows:,} registros")
                    
                    # Liberar memoria
                    del chunks
                    gc.collect()
                    
                    return df_completo
                else:
                    self.logger.error("No se pudieron cargar datos del archivo")
                    return None
                    
            except Exception as e:
                self.logger.error(f"Error al procesar chunks: {e}")
                return None
            
        except Exception as e:
            self.logger.error(f"Error general al cargar {ruta_archivo}: {e}")
            return None
    
    def obtener_valores_unicos_optimizado(self, df, columna):
        """Obtiene valores únicos de forma optimizada para grandes datasets"""
        if df is None or columna not in df.columns:
            return set()
        
        # Procesar en chunks si el DataFrame es muy grande
        if len(df) > 500000:
            valores_unicos = set()
            for i in range(0, len(df), self.chunk_size):
                chunk = df[columna].iloc[i:i+self.chunk_size]
                valores_unicos.update(chunk.dropna().unique())
                
                if i % (self.chunk_size * 5) == 0:
                    self.logger.info(f"Procesando valores únicos: {i:,} registros")
            
            return valores_unicos
        else:
            return set(df[columna].dropna().unique())
    
    def comparar_conjuntos_optimizado(self, df_anterior, df_actual, columna_comparacion):
        """Compara dos dataframes de forma optimizada"""
        if df_anterior is None or df_actual is None:
            return pd.DataFrame(), pd.DataFrame()
        
        self.logger.info(f"Comparando columna '{columna_comparacion}'...")
        
        # Obtener conjuntos únicos
        set_anterior = self.obtener_valores_unicos_optimizado(df_anterior, columna_comparacion)
        set_actual = self.obtener_valores_unicos_optimizado(df_actual, columna_comparacion)
        
        self.logger.info(f"Valores únicos - Anterior: {len(set_anterior):,}, Actual: {len(set_actual):,}")
        
        # Encontrar diferencias
        nuevos = set_actual - set_anterior
        retirados = set_anterior - set_actual
        
        self.logger.info(f"Diferencias - Nuevos: {len(nuevos):,}, Retirados: {len(retirados):,}")
        
        # Crear DataFrames de resultado de forma optimizada
        df_nuevos = pd.DataFrame()
        df_retirados = pd.DataFrame()
        
        if nuevos:
            df_nuevos = df_actual[df_actual[columna_comparacion].isin(nuevos)]
            # Eliminar duplicados y mantener solo columnas necesarias
            columnas_disponibles = [col for col in self.columnas_resultado if col in df_nuevos.columns]
            df_nuevos = df_nuevos[columnas_disponibles].drop_duplicates(subset=[columna_comparacion])
        
        if retirados:
            df_retirados = df_anterior[df_anterior[columna_comparacion].isin(retirados)]
            # Eliminar duplicados y mantener solo columnas necesarias
            columnas_disponibles = [col for col in self.columnas_resultado if col in df_retirados.columns]
            df_retirados = df_retirados[columnas_disponibles].drop_duplicates(subset=[columna_comparacion])
        
        return df_nuevos, df_retirados
    
    def procesar_archivos(self):
        """Procesa todos los archivos TXT en la carpeta de forma optimizada"""
        # Buscar todos los archivos TXT
        patron_archivos = str(self.ruta_base / "*.txt")
        archivos = glob.glob(patron_archivos)
        
        if not archivos:
            self.logger.error(f"No se encontraron archivos TXT en {self.ruta_base}")
            return
        
        # Extraer fechas y ordenar archivos
        archivos_con_fecha = []
        for archivo in archivos:
            fecha = self.extraer_fecha_archivo(os.path.basename(archivo))
            if fecha:
                archivos_con_fecha.append((fecha, archivo))
        
        archivos_con_fecha.sort(key=lambda x: x[0])
        
        if len(archivos_con_fecha) < 2:
            self.logger.error("Se necesitan al menos 2 archivos para hacer comparaciones")
            return
        
        self.logger.info(f"Procesando {len(archivos_con_fecha)} archivos...")
        
        df_anterior = None
        fecha_anterior = None
        
        for i, (fecha_actual, archivo_actual) in enumerate(archivos_con_fecha):
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"PROCESANDO: {os.path.basename(archivo_actual)} ({fecha_actual.strftime('%d/%m/%Y')})")
            self.logger.info(f"{'='*60}")
            
            # Cargar archivo actual
            df_actual = self.cargar_archivo_optimizado(archivo_actual)
            
            if df_actual is None:
                self.logger.warning(f"Saltando archivo {os.path.basename(archivo_actual)} - No se pudo cargar")
                continue
            
            # Si es el primer archivo válido, solo guardarlo como referencia
            if df_anterior is None:
                df_anterior = df_actual
                fecha_anterior = fecha_actual
                self.logger.info("Primer archivo cargado como referencia")
                continue
            
            # Realizar comparaciones para ambas columnas
            for columna in ['Int.cial.', 'Cta.Contr.']:
                if columna in df_anterior.columns and columna in df_actual.columns:
                    self.procesar_comparacion_optimizada(df_anterior, df_actual, fecha_anterior, fecha_actual, columna)
                else:
                    self.logger.warning(f"Columna '{columna}' no encontrada en ambos archivos")
            
            # Liberar memoria del archivo anterior
            del df_anterior
            gc.collect()
            
            # Actualizar para la siguiente iteración
            df_anterior = df_actual
            fecha_anterior = fecha_actual
        
        # Generar reporte consolidado
        if self.estadisticas:
            self.generar_reporte_consolidado()
        else:
            self.logger.error("No se generaron estadísticas. Verifique que los archivos tengan las columnas correctas.")
        
    def procesar_comparacion_optimizada(self, df_anterior, df_actual, fecha_anterior, fecha_actual, columna):
        """Procesa la comparación para una columna específica de forma optimizada"""
        try:
            self.logger.info(f"\n--- COMPARANDO COLUMNA: {columna} ---")
            
            # Comparar conjuntos
            df_nuevos, df_retirados = self.comparar_conjuntos_optimizado(df_anterior, df_actual, columna)
            
            # Estadísticas
            total_anterior = df_anterior[columna].nunique()
            total_actual = df_actual[columna].nunique()
            cantidad_nuevos = len(df_nuevos)
            cantidad_retirados = len(df_retirados)
            
            # Guardar estadísticas
            estadistica = {
                'Fecha_Anterior': fecha_anterior.strftime('%Y%m%d'),
                'Fecha_Actual': fecha_actual.strftime('%Y%m%d'),
                'Columna': columna,
                'Total_Anterior': total_anterior,
                'Total_Actual': total_actual,
                'Nuevos': cantidad_nuevos,
                'Retirados': cantidad_retirados,
                'Diferencia_Neta': cantidad_nuevos - cantidad_retirados
            }
            
            self.estadisticas.append(estadistica)
            
            self.logger.info(f"RESULTADOS - Nuevos: {cantidad_nuevos:,}, Retirados: {cantidad_retirados:,}")
            self.logger.info(f"Total anterior: {total_anterior:,}, Total actual: {total_actual:,}")
            
            # Guardar archivos de detalle si hay cambios
            if cantidad_nuevos > 0 or cantidad_retirados > 0:
                self.guardar_archivos_detalle_optimizado(df_nuevos, df_retirados, fecha_anterior, fecha_actual, columna)
        
        except Exception as e:
            self.logger.error(f"Error al procesar comparación para {columna}: {e}")
    
    def guardar_archivos_detalle_optimizado(self, df_nuevos, df_retirados, fecha_anterior, fecha_actual, columna):
        """Guarda los archivos de detalle de forma optimizada"""
        fecha_str = f"{fecha_anterior.strftime('%Y%m%d')}_vs_{fecha_actual.strftime('%Y%m%d')}"
        nombre_archivo = f"Comparativo_{columna.replace('.', '_')}_{fecha_str}.xlsx"
        ruta_archivo = self.ruta_resultados / nombre_archivo
        
        try:
            self.logger.info(f"Guardando archivo: {nombre_archivo}")
            
            # ELIMINAMOS el parámetro 'options' que causaba el error
            with pd.ExcelWriter(ruta_archivo, engine='openpyxl') as writer:
                
                # Guardar nuevos registros
                if not df_nuevos.empty:
                    # Optimizar tipos de datos antes de guardar
                    df_nuevos_opt = df_nuevos.copy()
                    for col in df_nuevos_opt.select_dtypes(include=['object']):
                        if col in df_nuevos_opt.columns:
                            df_nuevos_opt[col] = df_nuevos_opt[col].astype('string')
                    
                    df_nuevos_opt.to_excel(writer, sheet_name='Nuevos', index=False)
                    self.logger.info(f"  - Hoja 'Nuevos': {len(df_nuevos_opt):,} registros")
                else:
                    pd.DataFrame(columns=self.columnas_resultado).to_excel(writer, sheet_name='Nuevos', index=False)
                    self.logger.info("  - Hoja 'Nuevos': Sin registros")
                
                # Guardar registros retirados
                if not df_retirados.empty:
                    df_retirados_opt = df_retirados.copy()
                    for col in df_retirados_opt.select_dtypes(include=['object']):
                        if col in df_retirados_opt.columns:
                            df_retirados_opt[col] = df_retirados_opt[col].astype('string')
                    
                    df_retirados_opt.to_excel(writer, sheet_name='Retirados', index=False)
                    self.logger.info(f"  - Hoja 'Retirados': {len(df_retirados_opt):,} registros")
                else:
                    pd.DataFrame(columns=self.columnas_resultado).to_excel(writer, sheet_name='Retirados', index=False)
                    self.logger.info("  - Hoja 'Retirados': Sin registros")
            
            self.logger.info(f"✅ Archivo guardado exitosamente: {nombre_archivo}")
            
        except Exception as e:
            self.logger.error(f"❌ Error al guardar {nombre_archivo}: {e}")
    
    def generar_reporte_consolidado(self):
        """Genera el reporte consolidado con todas las estadísticas"""
        if not self.estadisticas:
            self.logger.error("No hay estadísticas para generar el reporte consolidado")
            return
        
        self.logger.info("\n" + "="*60)
        self.logger.info("GENERANDO REPORTE CONSOLIDADO")
        self.logger.info("="*60)
        
        df_estadisticas = pd.DataFrame(self.estadisticas)
        
        # Agregar columnas de fecha formateadas
        df_estadisticas['Fecha_Anterior_Formato'] = pd.to_datetime(df_estadisticas['Fecha_Anterior'], format='%Y%m%d').dt.strftime('%d/%m/%Y')
        df_estadisticas['Fecha_Actual_Formato'] = pd.to_datetime(df_estadisticas['Fecha_Actual'], format='%Y%m%d').dt.strftime('%d/%m/%Y')
        
        # Reordenar columnas
        columnas_ordenadas = [
            'Fecha_Anterior_Formato', 'Fecha_Actual_Formato', 'Columna',
            'Total_Anterior', 'Total_Actual', 'Nuevos', 'Retirados', 'Diferencia_Neta'
        ]
        df_estadisticas = df_estadisticas[columnas_ordenadas]
        
        # Renombrar columnas para el reporte
        df_estadisticas.columns = [
            'Fecha Anterior', 'Fecha Actual', 'Columna Análisis',
            'Total Anterior', 'Total Actual', 'Registros Nuevos', 
            'Registros Retirados', 'Diferencia Neta'
        ]
        
        # Guardar reporte consolidado
        nombre_reporte = "Reporte_Consolidado_Estadisticas.xlsx"
        ruta_reporte = self.ruta_resultados / nombre_reporte
        
        try:
            with pd.ExcelWriter(ruta_reporte, engine='openpyxl') as writer:
                df_estadisticas.to_excel(writer, sheet_name='Estadísticas Generales', index=False)
                
                # Crear resumen por columna
                for columna in df_estadisticas['Columna Análisis'].unique():
                    df_columna = df_estadisticas[df_estadisticas['Columna Análisis'] == columna]
                    nombre_hoja = f"Resumen_{columna.replace('.', '_')}"[:31]  # Límite de Excel
                    df_columna.to_excel(writer, sheet_name=nombre_hoja, index=False)
            
            self.logger.info(f"✅ Reporte consolidado guardado: {nombre_reporte}")
            self.logger.info(f"📊 Total de comparaciones procesadas: {len(df_estadisticas)}")
            
            # Mostrar resumen final
            self.mostrar_resumen_final(df_estadisticas)
            
        except Exception as e:
            self.logger.error(f"❌ Error al generar reporte consolidado: {e}")
    
    def mostrar_resumen_final(self, df_estadisticas):
        """Muestra un resumen final del procesamiento"""
        self.logger.info("\n" + "="*60)
        self.logger.info("RESUMEN FINAL DEL PROCESAMIENTO")
        self.logger.info("="*60)
        
        for columna in df_estadisticas['Columna Análisis'].unique():
            df_col = df_estadisticas[df_estadisticas['Columna Análisis'] == columna]
            total_nuevos = df_col['Registros Nuevos'].sum()
            total_retirados = df_col['Registros Retirados'].sum()
            
            self.logger.info(f"\n🔍 COLUMNA: {columna}")
            self.logger.info(f"  📈 Total registros nuevos: {total_nuevos:,}")
            self.logger.info(f"  📉 Total registros retirados: {total_retirados:,}")
            self.logger.info(f"  📊 Diferencia neta: {total_nuevos - total_retirados:,}")


def main():
    """Función principal optimizada"""
    ruta_base = r"D:\FNB\Reportes\04 Reporte Clientes Potenciales\Comparativo OFFLINE Setiembre 2025"
    ruta_resultados = r"D:\FNB\Reportes\04 Reporte Clientes Potenciales\Comparativo OFFLINE Setiembre 2025\Comparativos"
    
    # Configurar tamaño de chunk según memoria disponible
    # Para archivos de 2M registros, usar chunks de 50K-100K registros
    chunk_size = 75000  # Ajustar según la memoria RAM disponible
    
    print("🚀 INICIANDO ANÁLISIS COMPARATIVO OPTIMIZADO PARA GRANDES VOLÚMENES")
    print(f"📂 Carpeta origen: {ruta_base}")
    print(f"📁 Carpeta resultados: {ruta_resultados}")
    print(f"⚙️ Tamaño de chunk: {chunk_size:,} registros")
    print("="*80)
    
    # Crear instancia y procesar
    comparativo = ComparativoClientesOptimizado(ruta_base, ruta_resultados, chunk_size)
    
    inicio = datetime.now()
    comparativo.procesar_archivos()
    fin = datetime.now()
    
    tiempo_total = fin - inicio
    print(f"\n⏱️ TIEMPO TOTAL DE PROCESAMIENTO: {tiempo_total}")
    print(f"✅ ANÁLISIS COMPLETADO EXITOSAMENTE!")
    print(f"🔍 Revisa la carpeta '{ruta_resultados}' para ver los resultados")


if __name__ == "__main__":
    main()
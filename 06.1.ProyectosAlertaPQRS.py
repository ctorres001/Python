import pandas as pd
import numpy as np
import os
import glob
import matplotlib.pyplot as plt
import win32com.client as win32
from PIL import Image, ImageOps
from datetime import datetime, timedelta
import shutil


class SistemaPQRSAutomatico:
    def __init__(self):
        # === CONFIGURACIÓN DE RUTAS ===
        self.carpeta_base = r"C:\Users\carlos.torres2\OneDrive - IBR PERU\Proyectos IBR\Alerta Reclamos Aliados"
        self.carpeta_reporte = os.path.join(self.carpeta_base, "Reporte")
        self.carpeta_pqrs = os.path.join(self.carpeta_reporte, "PQRS")
        self.carpeta_destinatarios = os.path.join(self.carpeta_reporte, "Destinatarios")
        self.carpeta_firma = os.path.join(self.carpeta_reporte, "Firma")
        self.carpeta_pasados = os.path.join(self.carpeta_reporte, "Pasados")
        self.carpeta_archivos = os.path.join(self.carpeta_base, "Archivos")
        self.carpeta_imagenes = os.path.join(self.carpeta_archivos, "tablas_img")
        
        # Crear carpetas necesarias
        os.makedirs(self.carpeta_archivos, exist_ok=True)
        os.makedirs(self.carpeta_imagenes, exist_ok=True)
        os.makedirs(self.carpeta_pasados, exist_ok=True)

    def buscar_excel_en_carpeta(self, carpeta):
        """Busca el primer archivo Excel en una carpeta"""
        archivos = glob.glob(os.path.join(carpeta, "*.xlsx"))
        if not archivos:
            raise FileNotFoundError(f"No se encontró ningún archivo .xlsx en {carpeta}")
        return archivos[0]

    def buscar_imagen_firma(self):
        """Busca la imagen de firma en la carpeta"""
        extensiones = ["*.jpg", "*.jpeg", "*.png"]
        for ext in extensiones:
            archivos = glob.glob(os.path.join(self.carpeta_firma, ext))
            if archivos:
                return archivos[0]
        raise FileNotFoundError(f"No se encontró ninguna imagen de firma en {self.carpeta_firma}")

    def limpiar(self, df):
        """Limpia espacios en blanco de columnas de texto y normaliza nombres de columnas"""
        # Limpiar y normalizar nombres de columnas
        df.columns = df.columns.str.strip().str.upper()
        
        # Limpiar datos de texto
        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].astype(str).str.strip()
        return df

    def mapear_columnas(self, df):
        """Mapea las columnas del DataFrame a nombres estándar"""
        # Diccionario de mapeo de columnas (clave: nombre esperado, valor: posibles variaciones)
        mapeo_columnas = {
            'FECHA INICIAL': ['FECHA INICIAL', 'FECHA_INICIAL', 'FECHAINICIAL'],
            'MES INICIAL': ['MES INICIAL', 'MES_INICIAL', 'MESINCIAL'],
            'FECHA ENVIO DE CORREO': ['FECHA ENVIO DE CORREO', 'FECHA_ENVIO_DE_CORREO', 'FECHAENVIO'],
            'FECHA DE VENCIMIENTO': ['FECHA DE VENCIMIENTO', 'FECHA_DE_VENCIMIENTO', 'FECHAVENCIMIENTO', 'VENCIMIENTO'],
            'FECHA DE CIERRE SAP (BO)': ['FECHA DE CIERRE SAP (BO)', 'FECHA_DE_CIERRE_SAP_(BO)', 'FECHA DE CIERRE SAP', 'FECHACIERRE', 'FECHA DE CIERRE PQRS'],
            'TIEMPO DE CIERRE (DÍAS)': ['TIEMPO DE CIERRE (DÍAS)', 'TIEMPO_DE_CIERRE_(DÍAS)', 'TIEMPO DE CIERRE', 'TIEMPOCIERRE'],
            'PLAZO': ['PLAZO', 'PLAZOS'],
            'ESTADO': ['ESTADO', 'STATUS'],
            'MOTIVO': ['MOTIVO', 'MOTIVOS'],
            'AVISO': ['AVISO', 'AVISOS'],
            'TIPIFICACION': ['TIPIFICACION', 'TIPIFICACIÓN', 'TIPO'],
            'PETITORIO': ['PETITORIO', 'PETITORIOS'],
            'CUENTA CONTRATO': ['CUENTA CONTRATO', 'CUENTA_CONTRATO', 'CUENTACONTRATO', 'CUENTA'],
            'PEDIDO DE VENTA': ['PEDIDO DE VENTA', 'PEDIDO_DE_VENTA', 'PEDIDOVENTA', 'PEDIDO'],
            'FECHA DE VENTA': ['FECHA DE VENTA', 'FECHA_DE_VENTA', 'FECHAVENTA'],
            'CANAL': ['CANAL', 'CANALES'],
            'RESPONSABLE DE VENTA2': ['RESPONSABLE DE VENTA2', 'RESPONSABLE_DE_VENTA2', 'RESPONSABLE DE VENTA', 'RESPONSABLEVENTA'],
            'PROVEEDOR': ['PROVEEDOR', 'PROVEEDORES'],
            'MARCA': ['MARCA', 'MARCAS'],
            'PRODUCTO': ['PRODUCTO', 'PRODUCTOS'],
            'IMPORTE COLOCADO': ['IMPORTE COLOCADO', 'IMPORTE_COLOCADO', 'IMPORTECOLOCADO'],
            'IMPORTE FINANCIADO': ['IMPORTE FINANCIADO', 'IMPORTE_FINANCIADO', 'IMPORTEFINANCIADO'],
            'ASESOR DE VENTA': ['ASESOR DE VENTA', 'ASESOR_DE_VENTA', 'ASESORVENTA', 'ASESOR'],
            'DETALLE DE TIPIFICACIÓN': ['DETALLE DE TIPIFICACIÓN', 'DETALLE_DE_TIPIFICACIÓN', 'DETALLE DE TIPIFICACION', 'DETALLETIPIFICACION'],
            'CONCLUSIÓN': ['CONCLUSIÓN', 'CONCLUSION', 'CONCLUSIONES'],
            'PENALIDAD - FUERA PLAZO': ['PENALIDAD - FUERA PLAZO', 'PENALIDAD_-_FUERA_PLAZO', 'PENALIDAD FUERA PLAZO', 'PENALIDAD']
        }
        
        # Crear diccionario de renombrado
        rename_dict = {}
        columnas_encontradas = []
        
        for col_estandar, variaciones in mapeo_columnas.items():
            for variacion in variaciones:
                if variacion in df.columns:
                    rename_dict[variacion] = col_estandar
                    columnas_encontradas.append(col_estandar)
                    break
        
        # Renombrar columnas
        df_renamed = df.rename(columns=rename_dict)
        
        # Mostrar mapeo realizado
        if rename_dict:
            print("📋 MAPEO DE COLUMNAS REALIZADO:")
            for original, nuevo in rename_dict.items():
                print(f"  '{original}' → '{nuevo}'")
        
        # Mostrar columnas no encontradas
        columnas_no_encontradas = set(mapeo_columnas.keys()) - set(columnas_encontradas)
        if columnas_no_encontradas:
            print("⚠️ COLUMNAS NO ENCONTRADAS:")
            for col in sorted(columnas_no_encontradas):
                print(f"  - {col}")
            print("\n📝 COLUMNAS DISPONIBLES EN EL ARCHIVO:")
            for i, col in enumerate(sorted(df.columns), 1):
                print(f"  {i:2d}. '{col}'")
        
        return df_renamed

    def cargar_y_limpiar(self, ruta, header_row=0, sheet_name=0):
        """Carga, limpia y mapea un archivo Excel"""
        df = pd.read_excel(ruta, header=header_row, sheet_name=sheet_name)
        df = self.limpiar(df)
        df = self.mapear_columnas(df)
        return df

    def obtener_fecha_mes_anterior(self):
        """Obtiene el primer y último día del mes anterior"""
        hoy = datetime.now()
        primer_dia_mes_actual = hoy.replace(day=1)
        ultimo_dia_mes_anterior = primer_dia_mes_actual - timedelta(days=1)
        primer_dia_mes_anterior = ultimo_dia_mes_anterior.replace(day=1)
        
        return primer_dia_mes_anterior, ultimo_dia_mes_anterior

    def paso_1_generar_archivos_excel(self):
        """PASO 1: Generar archivos Excel por proveedor con filtros aplicados"""
        print("=== PASO 1: GENERANDO ARCHIVOS EXCEL PQRS ===")

        try:
            # Verificar rutas exactas
            print(f"🔍 Buscando archivos en las rutas especificadas:")
            print(f"  📁 PQRS: {self.carpeta_pqrs}")
            print(f"  📁 Destinatarios: {self.carpeta_destinatarios}")
            
            # Carga de datos
            ruta_pqrs = self.buscar_excel_en_carpeta(self.carpeta_pqrs)
            ruta_destinatarios = self.buscar_excel_en_carpeta(self.carpeta_destinatarios)
            
            print(f"✅ Archivo PQRS encontrado: {os.path.basename(ruta_pqrs)}")
            print(f"✅ Archivo Destinatarios encontrado: {os.path.basename(ruta_destinatarios)}")
            
            # Cargar archivo PQRS analizando estructura
            print("\n🔍 ANALIZANDO ESTRUCTURA DEL ARCHIVO PQRS...")
            
            # Primero ver qué hojas tiene el archivo
            try:
                excel_file = pd.ExcelFile(ruta_pqrs)
                print(f"📋 Hojas disponibles en el archivo: {excel_file.sheet_names}")
                # cerrar el objeto para liberar el archivo
                excel_file.close()
                
                # Buscar hoja con diferentes variaciones posibles
                hojas_posibles = ["BASE TOTAL", "Base Total", "base total", "BASE_TOTAL", "Base_Total"]
                sheet_name = None
                
                for hoja in hojas_posibles:
                    if hoja in excel_file.sheet_names:
                        print(f"✅ Encontrada hoja '{hoja}'. Cargando desde ahí...")
                        sheet_name = hoja
                        break
                
                # Si no encuentra ninguna variación, buscar por contenido parcial
                if sheet_name is None:
                    for hoja_archivo in excel_file.sheet_names:
                        if "BASE" in hoja_archivo.upper() and "TOTAL" in hoja_archivo.upper():
                            print(f"✅ Encontrada hoja similar '{hoja_archivo}'. Cargando desde ahí...")
                            sheet_name = hoja_archivo
                            break
                
                # Si aún no encuentra nada, usar la primera hoja
                if sheet_name is None:
                    print("⚠️ Hoja 'BASE TOTAL' (o variaciones) no encontrada. Usando la primera hoja disponible.")
                    print(f"📋 Hojas disponibles: {excel_file.sheet_names}")
                    sheet_name = 0
                
            except Exception as e:
                print(f"❌ Error al analizar hojas: {e}")
                sheet_name = 0
            
            # Cargar archivo PQRS directamente con header=2
            print("\n🔍 CARGANDO ARCHIVO PQRS CON HEADER=2...")
            
            # Cargar directamente con header=2 (fila 2 como nombres de columnas)
            print(f"📋 Cargando archivo con header=2 (fila 2 como nombres de columnas)")
            pqrs_original = pd.read_excel(ruta_pqrs, header=2, sheet_name=sheet_name)
            header_row = 2
            
            print(f"📋 Primeras columnas encontradas: {list(pqrs_original.columns[:5])}")
            
            pqrs_original.columns = pqrs_original.columns.str.strip()  # Solo limpiar espacios
            
            print(f"📊 Registros iniciales en PQRS: {len(pqrs_original)}")
            print(f"📋 Total de columnas encontradas: {len(pqrs_original.columns)}")
            print("\n📝 COLUMNAS EXACTAS EN EL ARCHIVO PQRS:")
            for i, col in enumerate(pqrs_original.columns, 1):
                print(f"  {i:2d}. '{col}'")
            
            # Ahora aplicar limpieza y mapeo
            print(f"\n🔄 APLICANDO LIMPIEZA Y MAPEO...")
            pqrs = self.limpiar(pqrs_original.copy())
            pqrs = self.mapear_columnas(pqrs)
            
            # Cargar destinatarios (probablemente con header=0)
            destinatarios = self.cargar_y_limpiar(ruta_destinatarios, header_row=0, sheet_name=0)

            # Validar columnas críticas (solo las más importantes para continuar)
            columnas_criticas = ['ESTADO', 'PROVEEDOR']
            
            for col in columnas_criticas:
                if col not in pqrs.columns:
                    print(f"\n❌ COLUMNA CRÍTICA '{col}' NO ENCONTRADA DESPUÉS DEL MAPEO")
                    print("🔍 Buscando columnas similares...")
                    columnas_similares = [c for c in pqrs.columns if col.lower() in c.lower() or c.lower() in col.lower()]
                    if columnas_similares:
                        print(f"  Posibles coincidencias: {columnas_similares}")
                    raise ValueError(f"Columna crítica '{col}' no encontrada. No se puede continuar.")

            print(f"✅ Columnas críticas validadas: {columnas_criticas}")

            # Filtrar por ESTADO = "CERRADO"
            pqrs_cerrados = pqrs[pqrs['ESTADO'].str.upper() == 'CERRADO'].copy()
            print(f"📊 Registros con estado CERRADO: {len(pqrs_cerrados)}")

            if len(pqrs_cerrados) == 0:
                print("❌ No hay registros con estado CERRADO")
                return [], None

            # SIN FILTRO DE FECHA - Procesar todos los registros cerrados
            print("ℹ️ Procesando TODOS los registros con estado CERRADO (sin filtro de fecha)")
            pqrs_filtrados = pqrs_cerrados.copy()
            print(f"📊 Total de registros a procesar: {len(pqrs_filtrados)}")

            if len(pqrs_filtrados) == 0:
                print("❌ No hay registros para procesar")
                return [], None

            # Cruzar con destinatarios
            print("🔗 Realizando cruce con destinatarios...")
            destinatarios.rename(columns={'PROVEEDOR': 'PROVEEDOR_DEST'}, inplace=True)
            pqrs_final = pqrs_filtrados.merge(
                destinatarios, 
                left_on='PROVEEDOR', 
                right_on='PROVEEDOR_DEST', 
                how='inner'
            )

            print(f"📊 Registros después del cruce con destinatarios: {len(pqrs_final)}")

            if len(pqrs_final) == 0:
                print("❌ No hay registros después del cruce con destinatarios")
                print("🔍 Verificando proveedores...")
                proveedores_pqrs = set(pqrs_filtrados['PROVEEDOR'].dropna().unique())
                proveedores_dest = set(destinatarios['PROVEEDOR_DEST'].dropna().unique())
                print(f"  📊 Proveedores en PQRS: {len(proveedores_pqrs)}")
                print(f"  📊 Proveedores en Destinatarios: {len(proveedores_dest)}")
                coincidencias = proveedores_pqrs.intersection(proveedores_dest)
                print(f"  🎯 Coincidencias: {len(coincidencias)}")
                if len(coincidencias) > 0:
                    print(f"  ✅ Primeras coincidencias: {list(coincidencias)[:5]}")
                return [], None

            # Seleccionar columnas disponibles
            columnas_deseadas = [
                'FECHA INICIAL', 'MES INICIAL', 'FECHA ENVIO DE CORREO', 'FECHA DE VENCIMIENTO',
                'FECHA DE CIERRE SAP (BO)', 'TIEMPO DE CIERRE (DÍAS)', 'PLAZO', 'ESTADO',
                'MOTIVO', 'AVISO', 'TIPIFICACION', 'PETITORIO', 'CUENTA CONTRATO',
                'PEDIDO DE VENTA', 'FECHA DE VENTA', 'CANAL', 'RESPONSABLE DE VENTA2',
                'PROVEEDOR', 'MARCA', 'PRODUCTO', 'IMPORTE COLOCADO', 'IMPORTE FINANCIADO',
                'ASESOR DE VENTA', 'DETALLE DE TIPIFICACIÓN', 'CONCLUSIÓN', 'PENALIDAD - FUERA PLAZO'
            ]
            
            columnas_finales = [col for col in columnas_deseadas if col in pqrs_final.columns]
            pqrs_final = pqrs_final[columnas_finales]
            
            print(f"📋 Columnas incluidas en archivos finales: {len(columnas_finales)}")
            columnas_faltantes = set(columnas_deseadas) - set(columnas_finales)
            if columnas_faltantes:
                print(f"⚠️ Columnas no incluidas: {', '.join(sorted(columnas_faltantes))}")

            # Eliminar archivos anteriores
            for f in glob.glob(os.path.join(self.carpeta_archivos, '*.xlsx')):
                os.remove(f)

            # Fecha para nombre de archivo
            fecha_ayer = (datetime.now() - timedelta(days=1)).strftime("%d-%m-%Y")

            # Generar archivos por proveedor
            archivos_generados = []
            proveedores_info = {}

            for proveedor, df_proveedor in pqrs_final.groupby('PROVEEDOR'):
                nombre_seguro = proveedor.replace('/', '-').replace('\\', '-').replace(':', '-').replace('*', '-') \
                    .replace('?', '-').replace('"', '').replace('<', '-').replace('>', '-').replace('|', '-')
                
                nombre_archivo = f"PQRS {nombre_seguro} Cierre al {fecha_ayer} - Plazos de atención.xlsx"
                ruta_archivo = os.path.join(self.carpeta_archivos, nombre_archivo)

                with pd.ExcelWriter(ruta_archivo, engine='xlsxwriter') as writer:
                    df_proveedor.to_excel(writer, sheet_name='PQRS Cerrados', index=False)
                    self._formatear_excel(writer, 'PQRS Cerrados', df_proveedor)

                # Calcular estadísticas
                total_casos = len(df_proveedor)
                
                # Verificar si existe la columna PLAZO para calcular estadísticas
                if 'PLAZO' in df_proveedor.columns:
                    fuera_plazo = len(df_proveedor[df_proveedor['PLAZO'].str.upper().str.contains('FUERA', na=False)])
                    porcentaje_fuera = (fuera_plazo / total_casos) * 100 if total_casos > 0 else 0
                else:
                    print(f"⚠️ Columna 'PLAZO' no encontrada para {proveedor}. Se asumirá que todos están dentro de plazo.")
                    fuera_plazo = 0
                    porcentaje_fuera = 0.0

                proveedores_info[proveedor] = {
                    'archivo': nombre_archivo,
                    'total_casos': total_casos,
                    'fuera_plazo': fuera_plazo,
                    'porcentaje_fuera': porcentaje_fuera,
                    'dataframe': df_proveedor
                }

                archivos_generados.append(nombre_archivo)
                print(f"✅ Archivo generado: {nombre_archivo}")

            print(f"✅ PASO 1 COMPLETADO: {len(archivos_generados)} archivos generados")
            return archivos_generados, proveedores_info

        except Exception as e:
            print(f"❌ Error en PASO 1: {e}")
            import traceback
            print("🔍 Traceback completo:")
            traceback.print_exc()
            return [], None

    def _formatear_excel(self, writer, sheet_name, df):
        """Formatea el archivo Excel con estilos"""
        workbook = writer.book
        worksheet = writer.sheets[sheet_name]

        formato_header = workbook.add_format({
            'bold': True, 'font_name': 'Aptos', 'font_size': 8, 'bg_color': '#000000',
            'font_color': '#FFFFFF', 'align': 'center', 'valign': 'vcenter'
        })
        formato_celda = workbook.add_format({
            'font_name': 'Aptos', 'font_size': 8, 'align': 'left', 'valign': 'vcenter'
        })
        formato_fecha = workbook.add_format({
            'font_name': 'Aptos', 'font_size': 8, 'num_format': 'dd/mm/yyyy',
            'align': 'center', 'valign': 'vcenter'
        })
        formato_numero = workbook.add_format({
            'font_name': 'Aptos', 'font_size': 8, 'num_format': '#,##0.00',
            'align': 'right', 'valign': 'vcenter'
        })

        # Headers
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, formato_header)

        # Datos
        for row in range(1, len(df) + 1):
            for col, column in enumerate(df.columns):
                cell_value = df.iloc[row - 1, col]

                if pd.isna(cell_value) or (
                        isinstance(cell_value, float) and (np.isnan(cell_value) or np.isinf(cell_value))):
                    worksheet.write(row, col, '', formato_celda)
                elif column in ['FECHA INICIAL', 'FECHA ENVIO DE CORREO', 'FECHA DE VENCIMIENTO', 
                               'FECHA DE CIERRE SAP (BO)', 'FECHA DE VENTA']:
                    worksheet.write(row, col, cell_value, formato_fecha)
                elif column in ['IMPORTE COLOCADO', 'IMPORTE FINANCIADO', 'PENALIDAD - FUERA PLAZO']:
                    worksheet.write(row, col, cell_value, formato_numero)
                else:
                    worksheet.write(row, col, cell_value, formato_celda)

        # Ajustar ancho de columnas
        worksheet.set_default_row(11.25)
        for i, col in enumerate(df.columns):
            column_len = max(df[col].astype(str).map(len).max(), len(str(col)))
            worksheet.set_column(i, i, min(column_len + 2, 50))

    def paso_2_generar_imagenes(self, proveedores_info):
        """PASO 2: Generar imágenes de tablas dinámicas"""
        print("=== PASO 2: GENERANDO IMÁGENES DE TABLAS DINÁMICAS ===")

        try:
            imagenes_generadas = []

            for proveedor, info in proveedores_info.items():
                df = info['dataframe']

                # 🔹 Renombrar proveedor largo solo para mostrar en la tabla
                df["PROVEEDOR"] = df["PROVEEDOR"].replace(
                    {"IMPORT & EXPORT SANY INTERNACIONAL S.A.C": "SANY INTERNACIONAL S.A.C"}
                )

                # Validar columnas necesarias
                if 'DETALLE DE TIPIFICACIÓN' not in df.columns:
                    print(f"❌ Columna 'DETALLE DE TIPIFICACIÓN' no encontrada para {proveedor}")
                    continue
                if 'PLAZO' not in df.columns:
                    print(f"⚠️ Columna 'PLAZO' no encontrada para {proveedor}. Se creará por defecto.")
                    df['PLAZO'] = 'DENTRO DE PLAZO'

                # Crear tabla dinámica
                pivot = pd.crosstab(
                    [df['PROVEEDOR'], df['DETALLE DE TIPIFICACIÓN']],
                    df['PLAZO']
                ).reset_index()

                # Calcular totales por fila
                pivot["TOTAL"] = pivot.drop(columns=["PROVEEDOR", "DETALLE DE TIPIFICACIÓN"]).sum(axis=1)

                # Agregar fila de subtotal por proveedor
                subtotales = pivot.groupby("PROVEEDOR").sum().reset_index()
                subtotales["DETALLE DE TIPIFICACIÓN"] = ""
                subtotales["PROVEEDOR"] = "ZZZ_TOTAL_PROVEEDOR"
                pivot = pd.concat([pivot, subtotales], ignore_index=True)

                # Ordenar: DETALLE por TOTAL (desc) y subtotales al final
                pivot["ES_SUBTOTAL"] = (pivot["PROVEEDOR"] == "TOTAL PROVEEDOR").astype(int)
                pivot = pivot.sort_values(
                    by=["PROVEEDOR", "ES_SUBTOTAL", "TOTAL"],
                    ascending=[True, True, False]
                ).drop(columns="ES_SUBTOTAL")

                # Simular combinación de PROVEEDOR
                pivot["PROVEEDOR_DISPLAY"] = pivot["PROVEEDOR"].replace("ZZZ_TOTAL_PROVEEDOR", "TOTAL PROVEEDOR")
                pivot.loc[pivot.duplicated(subset="PROVEEDOR"), "PROVEEDOR_DISPLAY"] = ""
                pivot.drop(columns="PROVEEDOR", inplace=True)
                pivot.insert(0, "PROVEEDOR", pivot.pop("PROVEEDOR_DISPLAY"))


                # Crear imagen
                fig, ax = plt.subplots(figsize=(14, max(8, len(pivot) * 0.6)))
                ax.axis("off")

                tabla = ax.table(
                    cellText=pivot.values,
                    colLabels=pivot.columns,
                    cellLoc="center",
                    loc="center"
                )

                tabla.auto_set_font_size(False)
                tabla.set_fontsize(9)
                tabla.scale(1.2, 1.5)

                # 🔹 Ajustar manualmente anchos de columnas
                col_widths = {
                    "PROVEEDOR": 0.35,
                    "DETALLE DE TIPIFICACIÓN": 0.45,
                }
                for j, col_name in enumerate(pivot.columns):
                    if col_name in col_widths:
                        for i in range(len(pivot) + 1):  # incluye cabecera
                            cell = tabla[i, j]
                            cell.set_width(col_widths[col_name])

                # Cabecera en negro
                for i in range(len(pivot.columns)):
                    tabla[(0, i)].set_facecolor("#000000")
                    tabla[(0, i)].set_text_props(weight="bold", color="white")

                # Subtotales en gris
                for row in range(1, len(pivot) + 1):
                    if pivot.iloc[row - 1]["PROVEEDOR"] == "TOTAL PROVEEDOR":
                        for col in range(len(pivot.columns)):
                            tabla[(row, col)].set_facecolor("#E0E0E0")
                            tabla[(row, col)].set_text_props(weight="bold")

                # Guardar imagen
                nombre_imagen = f"tabla_dinamica_pqrs_{proveedor.replace('/', '-').replace('\\', '-')}.png"
                temp_path = os.path.join(self.carpeta_imagenes, nombre_imagen)
                plt.tight_layout()
                plt.savefig(temp_path, dpi=300, bbox_inches="tight")
                plt.close()

                # Recorte y resize
                try:
                    img = Image.open(temp_path).convert("RGB")
                    gray = ImageOps.grayscale(img)
                    inverted = ImageOps.invert(gray)
                    bbox = inverted.getbbox()
                    if bbox:
                        img_cropped = img.crop(bbox)
                        new_width = 927  # 24.5 cm a 96 dpi
                        w_percent = new_width / float(img_cropped.size[0])
                        new_height = int(float(img_cropped.size[1]) * w_percent)
                        img_resized = img_cropped.resize((new_width, new_height), Image.Resampling.LANCZOS)
                        img_resized.save(temp_path)
                        imagenes_generadas.append(temp_path)
                        print(f"✅ Imagen generada: {nombre_imagen}")
                except Exception as e:
                    print(f"❌ Error al procesar imagen para {proveedor}: {e}")
                    continue

            print(f"✅ PASO 2 COMPLETADO: {len(imagenes_generadas)} imágenes generadas")
            return imagenes_generadas

        except Exception as e:
            print(f"❌ Error en PASO 2: {e}")
            return []

    def paso_3_enviar_correos(self, proveedores_info):
        """PASO 3: Generar y enviar correos automáticos"""
        print("=== PASO 3: GENERANDO CORREOS AUTOMÁTICOS PQRS ===")

        try:
            # Verificar archivos necesarios
            ruta_destinatarios = self.buscar_excel_en_carpeta(self.carpeta_destinatarios)
            ruta_firma = self.buscar_imagen_firma()

            df_destinatarios = pd.read_excel(ruta_destinatarios)
            df_destinatarios.set_index('PROVEEDOR', inplace=True)

            # Iniciar Outlook
            outlook = win32.Dispatch('Outlook.Application')

            correos_generados = []
            fecha_ayer = (datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y")

            for proveedor, info in proveedores_info.items():
                try:
                    # Verificar si el proveedor existe en destinatarios
                    if proveedor not in df_destinatarios.index:
                        print(f"❌ Proveedor no encontrado en destinatarios: {proveedor}")
                        continue

                    # Datos del correo
                    archivo_excel = os.path.join(self.carpeta_archivos, info['archivo'])
                    imagen_tabla = os.path.join(self.carpeta_imagenes, 
                        f'tabla_dinamica_pqrs_{proveedor.replace("/", "-").replace("\\", "-")}.png')

                    if not os.path.exists(imagen_tabla):
                        print(f"❌ Imagen de tabla no encontrada para {proveedor}")
                        continue

                    # Destinatarios
                    to = df_destinatarios.loc[proveedor, 'DESTINATARIOS DIRECTOS']
                    cc = df_destinatarios.loc[proveedor, 'DESTINATARIOS EN COPIA']
                    cc = cc if pd.notna(cc) else ""

                    # Crear correo
                    mail = outlook.CreateItem(0)
                    
                    # Configurar remitente personalizado
                    #mail.SentOnBehalfOfName = "Reportes IBR <noreply@ibr.com>"
                    #mail.SenderName = "Reportes IBR"
                    
                    mail.To = to
                    mail.CC = cc
                    mail.Subject = f"PQRS {proveedor} Cierre al {fecha_ayer} - Plazos de atención"
                    mail.Attachments.Add(archivo_excel)

                    # Adjuntar imagen de tabla
                    img_id = "tabla_pqrs"
                    attachment = mail.Attachments.Add(imagen_tabla)
                    attachment.PropertyAccessor.SetProperty(
                        "http://schemas.microsoft.com/mapi/proptag/0x3712001F", img_id)

                    # Adjuntar imagen de firma
                    firma_id = "firmaimg"
                    firma_attachment = mail.Attachments.Add(ruta_firma)
                    firma_attachment.PropertyAccessor.SetProperty(
                        "http://schemas.microsoft.com/mapi/proptag/0x3712001F", firma_id)

                    # 🔹 Adjuntar imagen de plazos como archivo normal (NO incrustado)
                    ruta_plazos = r"C:\Users\carlos.torres2\OneDrive - IBR PERU\Proyectos IBR\Alerta Reclamos Aliados\Reporte\Plazos\Plazos de Atención.jpg"
                    if os.path.exists(ruta_plazos):
                        mail.Attachments.Add(ruta_plazos)

                    # Seleccionar plantilla según si hay casos fuera de plazo
                    total_casos = info['total_casos']
                    fuera_plazo = info['fuera_plazo']
                    porcentaje_fuera = info['porcentaje_fuera']

                    if fuera_plazo > 0:
                        # Plantilla CON casos fuera de plazo
                        html_body = f"""
                        <html>
                        <body style="font-family: Aptos, sans-serif; font-size: 11pt;">
                        Buenos días:<br><br>
                         
                        Equipo {proveedor}, se comparte la base de PQRS Cerrados al {fecha_ayer}.<br><br>

                        Se procedió con el cierre de <b>{total_casos}</b> casos, de los cuales, <b>{fuera_plazo}</b> se encuentran fuera de plazo, equivalente al <b>{porcentaje_fuera:.2f}%</b>.<br><br>

                        Su apoyo por favor para revisar los casos que están fuera de plazo.<br><br>

                        <img src="cid:{img_id}"><br><br>

                        <b>Consideraciones:</b><br>
                        • Se adjunta los plazos de atención establecidos.<br>
                        • El proveedor puede enviar los sustentos que tenga a la mano para realizar el análisis y reducir el número de casos fuera de plazo.<br>
                        • Los plazos de atención se determinan desde el primer correo enviado por el BO hasta la fecha de cierre del PQRS (Solución).<br><br>

                        <span style="font-size: 9pt;">
                        Metodología de cálculo - TIEMPO DE CIERRE:<br>
                        DÍAS_LABORABLES(FECHA_ENVÍO_CORREO, FECHA_CIERRE_PQRS, FERIADOS) - 1<br><br>

                        Donde:<br>
                        • FECHA_ENVÍO_CORREO: Primer contacto del Back Office con el proveedor.<br>
                        • FECHA_CIERRE_PQRS: Fecha de solución registrada.<br>
                        • FERIADOS: Base de días no laborables (sábados, domingos y feriados oficiales).<br>
                        • -1: Se descuenta el día inicial de envío del correo<br><br>
                        </span>

                        Quedo atento a cualquier observación,<br><br>

                        <img src="cid:{firma_id}" style="width:200px; height:auto;">
                        </body>
                        </html>
                        """
                    else:
                        # Plantilla SIN casos fuera de plazo
                        html_body = f"""
                        <html>
                        <body style="font-family: Aptos, sans-serif; font-size: 11pt;">
                        Buenos días:<br><br>
                         
                        Equipo {proveedor}, se comparte la base de PQRS Cerrados al {fecha_ayer}.<br><br>

                        Se procedió con el cierre de <b>{total_casos}</b> casos, los cuales se encuentran <b>Dentro de plazo</b>.<br><br>

                        <img src="cid:{img_id}"><br><br>

                        <b>Consideraciones:</b><br>
                        • Se adjunta los plazos de atención establecidos.<br>
                        • El proveedor puede enviar los sustentos que tenga a la mano para realizar el análisis y reducir el número de casos fuera de plazo.<br>
                        • Los plazos de atención se determinan desde el primer correo enviado por el BO hasta la fecha de cierre del PQRS (Solución).<br><br>

                        <span style="font-size: 9pt;">
                        Metodología de cálculo - TIEMPO DE CIERRE:<br>
                        DÍAS_LABORABLES(FECHA_ENVÍO_CORREO, FECHA_CIERRE_PQRS, FERIADOS) - 1<br><br>

                        Donde:<br>
                        • FECHA_ENVÍO_CORREO: Primer contacto del Back Office con el proveedor.<br>
                        • FECHA_CIERRE_PQRS: Fecha de solución registrada.<br>
                        • FERIADOS: Base de días no laborables (sábados, domingos y feriados oficiales).<br>
                        • -1: Se descuenta el día inicial de envío del correo<br><br>
                        </span>
                        
                        Quedo atento a cualquier observación,<br><br>

                        <img src="cid:{firma_id}" style="width:200px; height:auto;">
                        </body>
                        </html>
                        """

                    mail.HTMLBody = html_body
                    mail.Display()  # Mostrar correo para revisión antes de enviar
                    correos_generados.append(f"{proveedor} - {info['archivo']}")
                    print(f"✅ Correo preparado para: {proveedor}")

                except Exception as e:
                    print(f"❌ Error generando correo para {proveedor}: {e}")
                    continue

            print(f"✅ PASO 3 COMPLETADO: {len(correos_generados)} correos preparados")
            return correos_generados

        except Exception as e:
            print(f"❌ Error en PASO 3: {e}")
            return []

    def paso_4_mover_archivo_origen(self):
        """PASO 4: Mover archivo origen al histórico"""
        print("=== PASO 4: MOVIENDO ARCHIVO ORIGEN ===")

        try:
            carpeta_origen = r"C:\Users\carlos.torres2\OneDrive - IBR PERU\Proyectos IBR\Alerta Reclamos Aliados\Reporte\PQRS"
            carpeta_destino = r"C:\Users\carlos.torres2\OneDrive - IBR PERU\Proyectos IBR\Alerta Reclamos Aliados\Reporte\Pasados"

            # Crear carpeta destino si no existe
            os.makedirs(carpeta_destino, exist_ok=True)

            # Listar archivos en origen
            archivos = [f for f in os.listdir(carpeta_origen) if f.endswith(".xlsx")]

            if not archivos:
                print(f"❌ No se encontró ningún archivo Excel en {carpeta_origen}")
                return

            for archivo in archivos:
                ruta_origen = os.path.join(carpeta_origen, archivo)
                ruta_destino = os.path.join(carpeta_destino, archivo)

                # Si ya existe en destino → agregar fecha y hora
                if os.path.exists(ruta_destino):
                    base, ext = os.path.splitext(archivo)
                    fecha_actual = datetime.now().strftime("%Y%m%d_%H%M%S")
                    nuevo_nombre = f"{base}_{fecha_actual}{ext}"
                    ruta_destino = os.path.join(carpeta_destino, nuevo_nombre)

                shutil.move(ruta_origen, ruta_destino)
                print(f"✅ Archivo movido: {ruta_destino}")

        except Exception as e:
            print(f"❌ Error en PASO 4: {e}")

    def ejecutar_proceso_completo(self):
        """Ejecuta todo el proceso de manera secuencial"""
        print("🚀 INICIANDO SISTEMA DE REPORTES AUTOMÁTICOS PQRS")
        print("=" * 70)

        # PASO 1: Generar archivos Excel
        resultado_paso1 = self.paso_1_generar_archivos_excel()
        if isinstance(resultado_paso1, tuple):
            archivos_excel, proveedores_info = resultado_paso1
        else:
            archivos_excel = resultado_paso1
            proveedores_info = None

        if not archivos_excel or not proveedores_info:
            print("❌ No se pudieron generar archivos Excel. Proceso detenido.")
            return

        print("\n" + "=" * 70)

        # PASO 2: Generar imágenes
        imagenes = self.paso_2_generar_imagenes(proveedores_info)
        if not imagenes:
            print("❌ No se pudieron generar imágenes. Proceso detenido.")
            return

        print("\n" + "=" * 70)

        # PASO 3: Generar correos
        correos = self.paso_3_enviar_correos(proveedores_info)

        print("\n" + "=" * 70)

        # PASO 4: Mover archivo origen
        archivo_movido = self.paso_4_mover_archivo_origen()

        print("\n" + "=" * 70)
        print("🎉 PROCESO COMPLETADO EXITOSAMENTE")
        print(f"📊 Archivos Excel por proveedor: {len(archivos_excel)}")
        print(f"🖼️  Imágenes de tablas generadas: {len(imagenes)}")
        print(f"📧 Correos preparados: {len(correos)}")
        print(f"📁 Archivo origen movido: {'✅' if archivo_movido else '❌'}")
        print("=" * 70)

        # Resumen por proveedor
        print("\n📋 RESUMEN POR PROVEEDOR:")
        for proveedor, info in proveedores_info.items():
            status_plazo = f"⚠️  {info['fuera_plazo']} fuera de plazo ({info['porcentaje_fuera']:.1f}%)" \
                if info['fuera_plazo'] > 0 else "✅ Todos dentro de plazo"
            print(f"  • {proveedor}: {info['total_casos']} casos - {status_plazo}")


# === EJECUCIÓN PRINCIPAL ===
if __name__ == "__main__":
    sistema = SistemaPQRSAutomatico()
    sistema.ejecutar_proceso_completo()
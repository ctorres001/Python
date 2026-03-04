import pandas as pd
import numpy as np
import os
import glob
import matplotlib.pyplot as plt
import win32com.client as win32
from PIL import Image, ImageOps
from datetime import datetime


class SistemaReportesAutomaticos:
    def __init__(self):
        # === CONFIGURACIÓN DE RUTAS ===
        self.carpeta_base = r"D:\FNB\Reportes\19. Reportes IBR\01. Pendientes de Entrega"
        self.carpeta_base_datos = os.path.join(self.carpeta_base, "Base")
        self.carpeta_archivos = os.path.join(self.carpeta_base, "Archivos")
        self.carpeta_faltantes = os.path.join(self.carpeta_base, "Canal Faltante")
        self.carpeta_imagenes = os.path.join(self.carpeta_archivos, "tablas_img")
        self.carpeta_resumen = os.path.join(self.carpeta_base, "Resumen")
        
        # RUTAS A ARCHIVOS COMUNES
        self.carpeta_canal = r"D:\FNB\Reportes\19. Reportes IBR\Archivos comunes\Canal"
        self.carpeta_feriados = r"D:\FNB\Reportes\19. Reportes IBR\Archivos comunes\Feriados"
        self.firma_path = r"D:\FNB\Reportes\19. Reportes IBR\Archivos comunes\Firma\Firma_resized.jpg"
        
        # CAMBIO DE RUTA PARA DESTINATARIOS
        self.destinatarios_path = os.path.join(self.carpeta_base, 'Destinatarios')

        # Crear carpetas necesarias
        os.makedirs(self.carpeta_archivos, exist_ok=True)
        os.makedirs(self.carpeta_imagenes, exist_ok=True)
        os.makedirs(self.carpeta_faltantes, exist_ok=True)
        os.makedirs(self.carpeta_resumen, exist_ok=True)

    def buscar_excel_en_carpeta(self, carpeta):
        """Busca el primer archivo Excel en una carpeta"""
        archivos = glob.glob(os.path.join(carpeta, "*.xlsx"))
        if not archivos:
            raise FileNotFoundError(f"No se encontró ningún archivo .xlsx en {carpeta}")
        return archivos[0]

    def limpiar(self, df):
        """Limpia espacios en blanco de columnas de texto"""
        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].astype(str).str.strip()
        return df

    def cargar_y_limpiar(self, ruta):
        """Carga y limpia un archivo Excel"""
        df = pd.read_excel(ruta)
        return self.limpiar(df)

    def calcular_dias_habiles(self, fecha_venta, feriados):
        """Calcula días hábiles desde fecha de venta hasta hoy"""
        if pd.isna(fecha_venta):
            return 0
        hoy = datetime.now().date()
        if isinstance(fecha_venta, pd.Timestamp):
            fecha_venta = fecha_venta.date()
        if fecha_venta > hoy:
            return 0
        dias_totales = pd.date_range(fecha_venta, hoy, freq='D')
        # Se excluyen solo domingos (weekday=6) y feriados, los sábados sí se cuentan
        dias_habiles = [d for d in dias_totales if d.weekday() != 6 and d.date() not in feriados]
        return max(len(dias_habiles) - 1, 0)

    def _cargar_mapeo_canales(self, ruta_archivo):
        """
        Carga el mapeo de SEDE → CANAL desde el archivo Excel.
        Columna A: SEDE, Columna B: CANAL
        Normaliza ambas columnas a mayúsculas para matching confiable.
        """
        try:
            df_canal = pd.read_excel(ruta_archivo, sheet_name='Hoja1')
            if len(df_canal.columns) >= 2:
                # Normalizar SEDE (columna A) a mayúsculas
                sedes_normalizadas = df_canal.iloc[:, 0].astype(str).str.strip().str.upper()
                # Normalizar CANAL (columna B) a mayúsculas
                canales_normalizados = df_canal.iloc[:, 1].astype(str).str.strip().str.upper()
                # Crear el diccionario de mapeo
                mapeo = dict(zip(sedes_normalizadas, canales_normalizados))
                print(f"✅ Mapeo de canales cargado: {len(mapeo)} sedes registradas")
                return mapeo
        except Exception as e:
            print(f"⚠️ Error leyendo archivo de canales: {e}")
        return {}

    def _determinar_canal_vectorizado(self, df, mapeo, ruta_archivo):
        """
        Determina el canal de venta basándose ÚNICAMENTE en la columna SEDE.
        Normaliza la columna SEDE a mayúsculas para hacer matching con el mapeo.
        Valida que todas las sedes tengan un canal asignado válido.
        
        Retorna: serie de canales
        """
        # Normalizar SEDE a mayúsculas para matching
        sede = df['SEDE'].astype(str).str.strip().str.upper()
        
        # VALIDACIÓN 1: Verificar que todas las sedes estén en el mapeo
        sedes_unicas = set(sede.unique())
        sedes_no_encontradas = sedes_unicas - mapeo.keys()
        # Limpiar valores inválidos de sedes_no_encontradas
        sedes_no_encontradas = {s for s in sedes_no_encontradas 
                                if s and str(s).strip() and str(s).upper() not in ['NAN', 'NONE', 'NULL', 'N/A', '']}
        
        if sedes_no_encontradas:
            mensaje_error = (
                f"ERROR: Se encontraron {len(sedes_no_encontradas)} sede(s) que NO están en el archivo de canales:\n"
                f"{', '.join(sorted(sedes_no_encontradas))}\n\n"
                f"Por favor, agregue estas sedes al archivo:\n{ruta_archivo}"
            )
            print(f"❌ {mensaje_error}")
            raise ValueError(mensaje_error)
        
        # Asignar canal usando el mapeo de SEDE
        canal = sede.map(mapeo)
        
        # VALIDACIÓN 2: Verificar que ningún canal esté vacío o sea un valor inválido
        valores_invalidos = ['', 'NAN', 'NONE', 'NULL', 'N/A']
        mask_canal_invalido = canal.isin(valores_invalidos) | canal.isna()
        
        if mask_canal_invalido.any():
            sedes_con_canal_invalido = sede.loc[mask_canal_invalido].unique()
            canales_invalidos = canal.loc[mask_canal_invalido].unique()
            mensaje_error = (
                f"ERROR: Se encontraron {len(sedes_con_canal_invalido)} sede(s) con canal vacío o inválido:\n"
                f"Sedes sin canal válido: {', '.join(sorted(sedes_con_canal_invalido))}\n"
                f"Canales encontrados: {', '.join([str(c) for c in canales_invalidos])}\n\n"
                f"Por favor, verifique que todas las sedes tengan un canal asignado en:\n{ruta_archivo}"
            )
            print(f"❌ {mensaje_error}")
            raise ValueError(mensaje_error)
        
        print(f"✅ Canales asignados correctamente para {len(sedes_unicas)} sede(s) única(s)")
        return canal

    def actualizar_tabla_excel(self, ruta):
        """Placeholder para actualización de tabla Excel"""
        pass

    def paso_1_generar_archivos_excel(self):
        """PASO 1: Generar archivos Excel por proveedor Y archivo resumen general"""
        print("=== PASO 1: GENERANDO ARCHIVOS EXCEL ===")

        try:
            # Carga de datos
            ruta_base = self.buscar_excel_en_carpeta(self.carpeta_base_datos)
            # Usar archivo actualizado de canales
            ruta_canal = os.path.join(self.carpeta_canal, "Canal_Actualizado_01032026.xlsx")
            if not os.path.exists(ruta_canal):
                print(f"⚠️ No se encontró {ruta_canal}, buscando alternativa...")
                ruta_canal = self.buscar_excel_en_carpeta(self.carpeta_canal)
            ruta_feriados = self.buscar_excel_en_carpeta(self.carpeta_feriados)

            base = self.cargar_y_limpiar(ruta_base)
            feriados_df = pd.read_excel(ruta_feriados)
            feriados = set(pd.to_datetime(feriados_df.iloc[:, 0]).dt.date)


            # === AJUSTE SOLICITADO ===
            # Si Nro. PEDIDO VENTA está vacío o incompleto → usar Nro. DE CONTRATO
            if 'Nro. PEDIDO VENTA' in base.columns and 'Nro. DE CONTRATO' in base.columns:

                # Convertimos ambos a texto y limpiamos espacios
                base['Nro. PEDIDO VENTA'] = base['Nro. PEDIDO VENTA'].astype(str).str.strip()
                base['Nro. DE CONTRATO'] = base['Nro. DE CONTRATO'].astype(str).str.strip()

                # Consideramos estos valores como vacíos / inválidos
                base['Nro. PEDIDO VENTA'] = base['Nro. PEDIDO VENTA'].replace(
                    ["", "-", "0", "nan", "None"], pd.NA
                )

                # Reemplazamos los vacíos con el contrato
                base['Nro. PEDIDO VENTA'] = base['Nro. PEDIDO VENTA'].fillna(base['Nro. DE CONTRATO'])


            # Convertir 'Nro. PEDIDO VENTA' a cadena y eliminar '.0' si existe
            if 'Nro. PEDIDO VENTA' in base.columns:
                base['Nro. PEDIDO VENTA'] = base['Nro. PEDIDO VENTA'].astype(str).str.replace(r'\.0$', '', regex=True)


            # Procesamiento de datos
            base['FECHA VENTA'] = pd.to_datetime(base['FECHA VENTA'], errors='coerce', dayfirst=True)
            base['SEDE'] = base['SEDE'].astype(str).str.strip().str.upper()

            # Cargar mapeo de canales y determinar canal (con validaciones integradas)
            mapeo = self._cargar_mapeo_canales(ruta_canal)
            base['Canal de Venta'] = self._determinar_canal_vectorizado(base, mapeo, ruta_canal)
            base['Canal de Venta'] = base['Canal de Venta'].replace('CHATBOT', 'DIGITAL')

            # Tipo de producto
            base['TipoProducto'] = base['PRODUCTO'].str.contains("PUNTO|DUCTE|ADICIONAL", case=False, na=False)
            base['TipoProducto'] = np.where(
                base['TipoProducto'] & ~base['PRODUCTO'].str.contains("MULTIPUNTO", case=False, na=False),
                "CON CONSTRUCCIÓN", 
                "PRODUCTO SOLO"
            )

            # Calcular tiempo
            base['Tiempo'] = base['FECHA VENTA'].apply(lambda x: self.calcular_dias_habiles(x, feriados) + 1)

            # Evaluar rango
            def evaluar_rango(row):
                if row['ALIADO COMERCIAL'] == 'MALL HOGAR':
                    return "FUERA DE PLAZO" if row['Tiempo'] > 10 else "DENTRO DE PLAZO"
                elif row['Canal de Venta'] == 'MOTOS':
                    return "FUERA DE PLAZO" if row['Tiempo'] > 30 else "DENTRO DE PLAZO"
                elif row['CATEGORIA'] == 'MUEBLES':
                    return "FUERA DE PLAZO" if row['Tiempo'] > 15 else "DENTRO DE PLAZO"
                elif row['TipoProducto'] == 'CON CONSTRUCCIÓN':
                    return "FUERA DE PLAZO" if row['Tiempo'] > 15 else "DENTRO DE PLAZO"
                elif row['TipoProducto'] == 'PRODUCTO SOLO':
                    return "FUERA DE PLAZO" if row['Tiempo'] > 4 else "DENTRO DE PLAZO"
                else:
                    return ""

            base['Rango'] = base.apply(evaluar_rango, axis=1)
            base['ALIADO COMERCIAL'] = base['ALIADO COMERCIAL'].str.replace('.', '', regex=False)
            base['prexcant'] = base['PRECIO'] * base['CANTIDAD']

            # Seleccionar columnas finales
            columnas_finales = [
                'RESPONSABLE DE VENTA', 'SEDE', 'ALIADO COMERCIAL', 'CUENTA CONTRATO', 'CLIENTE',
                'DNI', 'TELÉFONO', 'Nro. PEDIDO VENTA', 'IMPORTE (S./)', 'FECHA VENTA', 'ESTADO',
                'ASESOR DE VENTAS', 'PRODUCTO', 'SKU', 'CANTIDAD', 'PRECIO', 'CATEGORIA',
                'Canal de Venta', 'TipoProducto', 'Tiempo', 'Rango', 'prexcant'
            ]

            columnas_existentes = [col for col in columnas_finales if col in base.columns]
            base = base[columnas_existentes]
            base = base.sort_values(by=['FECHA VENTA', 'Nro. PEDIDO VENTA'])

            # Eliminar archivos anteriores de ambas carpetas
            for f in glob.glob(os.path.join(self.carpeta_archivos, '*.xlsx')):
                os.remove(f)
            for f in glob.glob(os.path.join(self.carpeta_resumen, '*.xlsx')):
                os.remove(f)

            # Fecha y hora para nombre de archivo
            fecha_hora_actual = datetime.now().strftime("%d-%m-%Y %H-%M")

            # === GENERAR ARCHIVO RESUMEN GENERAL ===
            nombre_resumen = f"Resumen General Pendientes de Entrega FNB - {fecha_hora_actual}.xlsx"
            ruta_resumen = os.path.join(self.carpeta_resumen, nombre_resumen)

            with pd.ExcelWriter(ruta_resumen, engine='xlsxwriter') as writer:
                base.to_excel(writer, sheet_name='Resumen General', index=False)
                self._formatear_excel(writer, 'Resumen General', base)

            print(f"✅ Archivo resumen general generado: {nombre_resumen}")

            # === GENERAR ARCHIVOS POR PROVEEDOR ===
            archivos_generados = []
            for proveedor, df_proveedor in base.groupby('ALIADO COMERCIAL'):
                nombre_seguro = proveedor.replace('/', '-').replace('\\', '-').replace(':', '-').replace('*', '-') \
                    .replace('?', '-').replace('"', '').replace('<', '-').replace('>', '-').replace('|', '-')
                nombre_archivo = f"Pendientes de Entrega FNB - {nombre_seguro} - {fecha_hora_actual}.xlsx"
                ruta_archivo = os.path.join(self.carpeta_archivos, nombre_archivo)

                with pd.ExcelWriter(ruta_archivo, engine='xlsxwriter') as writer:
                    df_proveedor.to_excel(writer, sheet_name='Pendientes', index=False)
                    self._formatear_excel(writer, 'Pendientes', df_proveedor)

                archivos_generados.append(nombre_archivo)
                print(f"✅ Archivo generado: {nombre_archivo}")

            print(f"✅ PASO 1 COMPLETADO: 1 archivo resumen + {len(archivos_generados)} archivos por proveedor")
            return archivos_generados, nombre_resumen

        except Exception as e:
            print(f"❌ Error en PASO 1: {e}")
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
                elif column == 'FECHA VENTA':
                    worksheet.write(row, col, cell_value, formato_fecha)
                elif column in ['PRECIO', 'IMPORTE (S./)', 'prexcant']:
                    worksheet.write(row, col, cell_value, formato_numero)
                else:
                    worksheet.write(row, col, cell_value, formato_celda)

        # Ajustar ancho de columnas
        worksheet.set_default_row(11.25)
        for i, col in enumerate(df.columns):
            column_len = max(df[col].astype(str).map(len).max(), len(str(col)))
            worksheet.set_column(i, i, min(column_len + 2, 50))

    def paso_2_generar_imagenes(self):
        """PASO 2: Generar imágenes de tablas dinámicas"""
        print("=== PASO 2: GENERANDO IMÁGENES DE TABLAS ===")

        try:
            files = [f for f in os.listdir(self.carpeta_archivos) if f.endswith('.xlsx')]
            imagenes_generadas = []

            for file in files:
                file_path = os.path.join(self.carpeta_archivos, file)
                df = pd.read_excel(file_path)

                required_cols = {'prexcant', 'Nro. PEDIDO VENTA', 'FECHA VENTA', 'RESPONSABLE DE VENTA', 'Rango'}
                if not required_cols.issubset(df.columns):
                    print(f"❌ El archivo '{file}' no tiene las columnas necesarias.")
                    continue

                # Crear columna PERIODO
                df['PERIODO'] = pd.to_datetime(df['FECHA VENTA']).dt.to_period('M').astype(str)

                # Crear tabla dinámica
                pivot = df.pivot_table(
                    index=['PERIODO', 'RESPONSABLE DE VENTA'],
                    columns='Rango',
                    values=['prexcant', 'Nro. PEDIDO VENTA'],
                    aggfunc={'prexcant': 'sum', 'Nro. PEDIDO VENTA': pd.Series.nunique},
                    fill_value=0,
                    margins=True,
                    margins_name='TOTAL'
                )

                # Renombrar columnas
                pivot.columns = [f"{'IMPORTE S/' if col[0] == 'prexcant' else '# TRX'} - {col[1]}" for col in
                                 pivot.columns]
                pivot.reset_index(inplace=True)

                # Reordenar columnas
                columnas_orden = [
                    'RESPONSABLE DE VENTA',
                    'IMPORTE S/ - FUERA DE PLAZO',
                    '# TRX - FUERA DE PLAZO',
                    'IMPORTE S/ - DENTRO DE PLAZO',
                    '# TRX - DENTRO DE PLAZO',
                    'IMPORTE S/ - TOTAL',
                    '# TRX - TOTAL'
                ]
                for col in columnas_orden:
                    if col not in pivot.columns:
                        pivot[col] = 0

                pivot = pivot[['PERIODO'] + columnas_orden]

                # Ordenar por PERIODO y por IMPORTE
                pivot['PERIODO_ORD'] = pd.to_datetime(pivot['PERIODO'], errors='coerce')
                pivot['IMPORTE_ORD'] = pivot['IMPORTE S/ - TOTAL'].astype(float)
                pivot = pivot.sort_values(by=['PERIODO_ORD', 'IMPORTE_ORD'], ascending=[False, False])
                pivot.drop(columns=['PERIODO_ORD', 'IMPORTE_ORD'], inplace=True)

                # Formatear columnas numéricas
                for col in pivot.columns:
                    if 'IMPORTE' in col:
                        pivot[col] = pivot[col].astype(int).apply(lambda x: f"{x:,}")
                    elif '# TRX' in col:
                        pivot[col] = pivot[col].astype(int)

                # Simular combinación de PERIODO
                pivot['PERIODO_DISPLAY'] = pivot['PERIODO']
                pivot.loc[pivot.duplicated(subset='PERIODO'), 'PERIODO_DISPLAY'] = ''
                pivot.drop(columns='PERIODO', inplace=True)
                pivot.insert(0, 'PERIODO', pivot.pop('PERIODO_DISPLAY'))

                # Insertar salto de línea en títulos
                pivot.columns = [
                    col.replace(' - ', '\n').replace('RESPONSABLE DE VENTA', 'RESPONSABLE\nDE VENTA')
                    if isinstance(col, str) else col
                    for col in pivot.columns
                ]

                # Crear tabla como imagen
                fig, ax = plt.subplots(figsize=(12, max(6, len(pivot) * 0.5)))
                ax.axis('off')

                tabla = ax.table(
                    cellText=pivot.values,
                    colLabels=pivot.columns,
                    cellLoc='center',
                    loc='center'
                )

                tabla.auto_set_font_size(False)
                tabla.set_fontsize(8.5)
                tabla.scale(1.2, 1.4)

                # Guardar imagen temporal
                temp_path = os.path.join(self.carpeta_imagenes, f'tabla_dinamica_{file.replace(".xlsx", "")}.png')
                plt.tight_layout()
                plt.savefig(temp_path, dpi=300, bbox_inches='tight')
                plt.close()

                # Recortar bordes blancos
                img = Image.open(temp_path).convert("RGB")
                gray = ImageOps.grayscale(img)
                inverted = ImageOps.invert(gray)

                bbox = inverted.getbbox()
                if bbox:
                    img_cropped = img.crop(bbox)
                    img_cropped.save(temp_path)
                    imagenes_generadas.append(temp_path)
                    print(f"✅ Imagen generada: {os.path.basename(temp_path)}")

            print(f"✅ PASO 2 COMPLETADO: {len(imagenes_generadas)} imágenes generadas")
            return imagenes_generadas

        except Exception as e:
            print(f"❌ Error en PASO 2: {e}")
            return []

    def paso_3_enviar_correos(self):
        """PASO 3: Generar y mostrar correos automáticos"""
        print("=== PASO 3: GENERANDO CORREOS AUTOMÁTICOS ===")

        try:
            # Verificar archivos necesarios - NUEVA RUTA PARA DESTINATARIOS
            listado_correos_path = self.buscar_excel_en_carpeta(self.destinatarios_path)

            if not os.path.exists(listado_correos_path):
                print(f"❌ No se encontró el listado de correos en: {self.destinatarios_path}")
                return []

            if not os.path.exists(self.firma_path):
                print(f"❌ No se encontró la imagen de firma: {self.firma_path}")
                return []

            # Leer listado de correos
            df_proveedores = pd.read_excel(listado_correos_path)
            df_proveedores.set_index('Proveedor', inplace=True)

            # Iniciar Outlook
            outlook = win32.Dispatch('Outlook.Application')

            correos_generados = []

            # Recorrer archivos Excel
            for file in os.listdir(self.carpeta_archivos):
                if not file.endswith('.xlsx'):
                    continue

                excel_path = os.path.join(self.carpeta_archivos, file)
                image_path = os.path.join(self.carpeta_imagenes, f'tabla_dinamica_{file.replace(".xlsx", "")}.png')

                if not os.path.exists(image_path):
                    print(f"❌ Imagen no encontrada para {file}")
                    continue

                # Redimensionar imagen de tabla
                try:
                    with Image.open(image_path) as img:
                        new_width = 927  # 24.5 cm a 96 dpi
                        w_percent = new_width / float(img.size[0])
                        new_height = int(float(img.size[1]) * w_percent)
                        img_resized = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
                        img_resized.save(image_path)
                except Exception as e:
                    print(f"❌ Error al redimensionar imagen de tabla: {e}")
                    continue

                # Leer Excel
                try:
                    df = pd.read_excel(excel_path)
                except:
                    print(f"❌ No se pudo leer: {file}")
                    continue

                if 'ALIADO COMERCIAL' not in df.columns:
                    print(f"❌ Columna 'ALIADO COMERCIAL' no encontrada en {file}")
                    continue

                aliados = df['ALIADO COMERCIAL'].dropna().unique()

                for aliado in aliados:
                    if aliado not in df_proveedores.index:
                        print(f"❌ Aliado no encontrado en listado: {aliado}")
                        continue

                    df_filtrado = df[df['ALIADO COMERCIAL'] == aliado]
                    if df_filtrado.empty:
                        continue

                    # Cálculos
                    total_pedidos = df_filtrado['Nro. PEDIDO VENTA'].nunique()
                    total_importe = df_filtrado['prexcant'].sum()
                    fuera = df_filtrado[df_filtrado['Rango'] == 'FUERA DE PLAZO']
                    total_fuera = fuera['Nro. PEDIDO VENTA'].nunique()
                    importe_fuera = fuera['prexcant'].sum()
                    porcentaje_fuera = (total_fuera / total_pedidos) * 100 if total_pedidos > 0 else 0

                    # Destinatarios
                    to = df_proveedores.loc[aliado, 'Destinatarios directos']
                    cc = df_proveedores.loc[aliado, 'Destinatarios en copia']
                    cc = cc if pd.notna(cc) else ""

                    # Crear correo
                    mail = outlook.CreateItem(0)
                    mail.To = to
                    mail.CC = cc
                    mail.Subject = file.replace('.xlsx', '')
                    mail.Attachments.Add(excel_path)

                    # Adjuntar imagen de tabla
                    img_id = "tabla1"
                    attachment = mail.Attachments.Add(image_path)
                    attachment.PropertyAccessor.SetProperty("http://schemas.microsoft.com/mapi/proptag/0x3712001F",
                                                            img_id)

                    # Adjuntar imagen de firma
                    firma_id = "firmaimg"
                    firma_attachment = mail.Attachments.Add(self.firma_path)
                    firma_attachment.PropertyAccessor.SetProperty(
                        "http://schemas.microsoft.com/mapi/proptag/0x3712001F", firma_id)

                    # HTML con firma embebida
                    html_body = f"""
                    <html>
                    <body style="font-family: Aptos, sans-serif; font-size: 11pt;">
                    Buenos días:<br><br>

                    Tenemos a la fecha <b>{total_pedidos}</b> pedidos por entregar por un total de <b>S/ {total_importe:,.0f}</b>, 
                    de los cuales <b>{total_fuera}</b> se encuentran fuera de fecha (<b>S/ {importe_fuera:,.0f}</b>), 
                    equivalente al <b>{porcentaje_fuera:.2f}%</b>.<br><br>

                    <img src="cid:{img_id}"><br><br>

                    Adjunto la BD de pendientes de entrega para que nos puedan enviar el status de cada uno de ellos.<br><br>

                    Atentamente,<br><br>

                    <img src="cid:{firma_id}">
                    </body>
                    </html>
                    """

                    mail.HTMLBody = html_body
                    mail.Send()  # Mostrar correo para revisión antes de enviar
                    correos_generados.append(f"{aliado} - {file}")
                    print(f"✅ Correo listo para: {aliado}")

            print(f"✅ PASO 3 COMPLETADO: {len(correos_generados)} correos generados")
            return correos_generados

        except Exception as e:
            print(f"❌ Error en PASO 3: {e}")
            return []

    def ejecutar_proceso_completo(self):
        """Ejecuta todo el proceso de manera secuencial"""
        print("🚀 INICIANDO SISTEMA DE REPORTES AUTOMÁTICOS")
        print("=" * 60)

        # PASO 1: Generar archivos Excel
        resultado_paso1 = self.paso_1_generar_archivos_excel()
        if isinstance(resultado_paso1, tuple):
            archivos_excel, archivo_resumen = resultado_paso1
        else:
            archivos_excel = resultado_paso1
            archivo_resumen = None

        if not archivos_excel:
            print("❌ No se pudieron generar archivos Excel. Proceso detenido.")
            return

        print("\n" + "=" * 60)

        # PASO 2: Generar imágenes
        imagenes = self.paso_2_generar_imagenes()
        if not imagenes:
            print("❌ No se pudieron generar imágenes. Proceso detenido.")
            return

        print("\n" + "=" * 60)

        # PASO 3: Generar correos
        correos = self.paso_3_enviar_correos()

        print("\n" + "=" * 60)
        print("🎉 PROCESO COMPLETADO EXITOSAMENTE")
        print(f"📊 Archivo resumen general: {'✅' if archivo_resumen else '❌'}")
        print(f"📊 Archivos Excel por proveedor: {len(archivos_excel)}")
        print(f"🖼️  Imágenes generadas: {len(imagenes)}")
        print(f"📧 Correos preparados: {len(correos)}")
        print("=" * 60)


# === EJECUCIÓN PRINCIPAL ===
if __name__ == "__main__":
    sistema = SistemaReportesAutomaticos()
    sistema.ejecutar_proceso_completo()
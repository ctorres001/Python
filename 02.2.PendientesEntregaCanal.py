import pandas as pd
import numpy as np
import os
import re
import glob
import matplotlib.pyplot as plt
import win32com.client as win32
from PIL import Image, ImageOps
from datetime import datetime, timedelta


class SistemaReportesPorCanal:
    def __init__(self):
        # === CONFIGURACIÓN DE RUTAS ===
        self.carpeta_base = r"D:\FNB\Reportes\19. Reportes IBR\01. Pendientes de Entrega"
        self.carpeta_base_datos = os.path.join(self.carpeta_base, "Base")
        self.carpeta_archivos = os.path.join(self.carpeta_base, "Archivos Canal")
        self.carpeta_feriados = os.path.join(self.carpeta_base, "Feriados")
        self.carpeta_canal = os.path.join(self.carpeta_base, "Canal")
        self.carpeta_imagenes = os.path.join(self.carpeta_archivos, "tablas_img")
        self.destinatarios_path = os.path.join(self.carpeta_base, 'Destinatarios')
        self.firma_path = os.path.join(self.carpeta_base, 'Firma', 'Firma_resized.jpg')

        # Crear carpetas necesarias
        os.makedirs(self.carpeta_archivos, exist_ok=True)
        os.makedirs(self.carpeta_imagenes, exist_ok=True)

        # Canales agrupados por grupo de envío
        self.canales_linda = [
            'ALO CÁLIDDA', 'CSC', 'CANAL PROVEEDOR',
            'FFVV - PUERTA A PUERTA', 'TIENDAS CÁLIDDA'
        ]
        self.canales_por_grupo = {
            'LINDA': self.canales_linda,
            'DIGITAL': ['DIGITAL'],
            'JOSÉ': ['MOTOS'],
            'EDGAR': ['MATERIALES Y ACABADOS DE CONSTRUCCIÓN']
        }

        # Escenarios en Excel (columna "Canal") para destinatarios por grupo
        self.grupo_a_escenario = {
            'LINDA': 'GENERAL',
            'DIGITAL': 'DIGITAL',
            'JOSÉ': 'MOTOS',
            'EDGAR': 'MATERIALES Y ACABADOS DE CONSTRUCCIÓN'
        }

        # Nombres de contacto por grupo
        self.saludos_por_grupo = {
            'LINDA': 'Linda',
            'DIGITAL': 'Shesyra',
            'JOSÉ': 'José',
            'EDGAR': 'Edgar'
        }

    # ------------------------ UTILIDADES ------------------------

    def buscar_excel_en_carpeta(self, carpeta):
        archivos = glob.glob(os.path.join(carpeta, "*.xlsx"))
        if not archivos:
            raise FileNotFoundError(f"No se encontró ningún archivo .xlsx en {carpeta}")
        return archivos[0]

    def limpiar(self, df):
        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].astype(str).str.strip()
        return df

    def cargar_y_limpiar(self, ruta):
        df = pd.read_excel(ruta)
        return self.limpiar(df)

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

        # Escribir encabezados
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, formato_header)

        # Escribir contenido
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

    def calcular_dias_habiles(self, fecha_venta, feriados):
        if pd.isna(fecha_venta):
            return 0
        hoy = datetime.now().date()
        if isinstance(fecha_venta, pd.Timestamp):
            fecha_venta = fecha_venta.date()
        if fecha_venta > hoy:
            return 0
        dias_totales = pd.date_range(fecha_venta, hoy, freq='D')
        # lunes(0) a sábado(5) como hábil según tu lógica original
        dias_habiles = [d for d in dias_totales if d.weekday() < 6 and d.date() not in feriados]
        return max(len(dias_habiles) - 1, 0)

    # --- Reemplazo global de etiquetas HTML por <br> y '-' para viñetas ---
    def simplificar_html_global(self, html):
        """
        Reemplaza globalmente:
          - <ul> y </ul> -> '' (se eliminan)
          - <li> -> '- ' (guion de viñeta), </li> -> '<br>'
          - <p> -> '' y </p> -> '<br>'
        NOTA: NO colapsa múltiples <br>; dejamos el extra para más aire visual.
        """
        if html is None:
            return ""
        s = str(html)

        # Quitar <ul>
        s = re.sub(r'(?i)<\s*ul[^>]*>', '', s)
        s = re.sub(r'(?i)</\s*ul\s*>', '', s)

        # Viñetas
        s = re.sub(r'(?i)<\s*li[^>]*>', '- ', s)
        s = re.sub(r'(?i)</\s*li\s*>', '<br>', s)

        # Párrafos
        s = re.sub(r'(?i)<\s*p[^>]*>', '', s)
        s = re.sub(r'(?i)</\s*p\s*>', '<br>', s)

        return s

    # ------------------------ PASO 1 ------------------------

    def paso_1_generar_archivos_por_canal(self):
        print("=== PASO 1: GENERANDO ARCHIVOS EXCEL POR CANAL ===")
        try:
            ruta_base = self.buscar_excel_en_carpeta(self.carpeta_base_datos)
            ruta_canal = self.buscar_excel_en_carpeta(self.carpeta_canal)
            ruta_feriados = self.buscar_excel_en_carpeta(self.carpeta_feriados)

            base = self.cargar_y_limpiar(ruta_base)
            canal = self.cargar_y_limpiar(ruta_canal)
            feriados_df = pd.read_excel(ruta_feriados)
            feriados = set(pd.to_datetime(feriados_df.iloc[:, 0]).dt.date)

            base['FECHA VENTA'] = pd.to_datetime(base['FECHA VENTA'], errors='coerce', dayfirst=True)
            base['SEDE'] = base['SEDE'].astype(str).str.strip().str.upper()
            canal['Nombre Tienda de Venta'] = canal['Nombre Tienda de Venta'].astype(str).str.strip().str.upper()

            base = base.merge(canal, how='left', left_on='SEDE', right_on='Nombre Tienda de Venta')

            def derivar_canal(row):
                if pd.isna(row['FECHA VENTA']):
                    return row['Canal_Reporte']

                # Retail especial para TOPITOP desde 01/08/2025
                if row['FECHA VENTA'] >= pd.Timestamp(2025, 8, 1) and row['RESPONSABLE DE VENTA'] in ["TOPITOP"]:
                    return "RETAIL"

                # Retail general para Conecta e Integra desde 01/02/2024
                if row['FECHA VENTA'] >= pd.Timestamp(2024, 2, 1) and row['RESPONSABLE DE VENTA'] in [
                    "CONECTA RETAIL S.A.", "INTEGRA RETAIL S.A.C."]:
                    return "RETAIL"

                # Materiales y acabados (excepto responsables excluidos)
                if row['CATEGORIA'] == "MATERIALES Y ACABADOS DE CONSTRUCCIÓN" and row['RESPONSABLE DE VENTA'] not in [
                    "A & G INGENIERIA", "INCOSER GAS PERU S.A.C.", "PROMART"]:
                    return "MATERIALES Y ACABADOS DE CONSTRUCCIÓN"

                # Motos, excepto responsables retail
                if row['CATEGORIA'] in ["MOTOS", "MOTOS ELECTRICAS", "ACCESORIOS MOTOS"] and row['RESPONSABLE DE VENTA'] not in [
                    "CONECTA RETAIL S.A.", "INTEGRA RETAIL S.A.C."]:
                    return "MOTOS"

                # En cualquier otro caso, se respeta el canal del reporte
                return row['Canal_Reporte']

            base['Canal de Venta'] = base.apply(derivar_canal, axis=1)
            base['Canal de Venta'] = base['Canal de Venta'].replace('CHATBOT', 'DIGITAL')

            base['TipoProducto'] = base['PRODUCTO'].str.contains("PUNTO|DUCTE|ADICIONAL", case=False, na=False)
            base['TipoProducto'] = np.where(base['TipoProducto'], "CON CONSTRUCCIÓN", "PRODUCTO SOLO")
            base['Tipo Producto'] = np.where(
                (base['ALIADO COMERCIAL'] == "GASODOMESTICOS") & (base['TipoProducto'] == "CON CONSTRUCCIÓN"),
                "CON CONSTRUCCIÓN", "PRODUCTO SOLO")

            base['Tiempo'] = base['FECHA VENTA'].apply(lambda x: self.calcular_dias_habiles(x, feriados) + 1)

            def evaluar_rango(row):
                if row['ALIADO COMERCIAL'] == 'MALL HOGAR':
                    return "FUERA DE PLAZO" if row['Tiempo'] > 10 else "DENTRO DE PLAZO"
                elif row['Canal de Venta'] == 'MOTOS':
                    return "FUERA DE PLAZO" if row['Tiempo'] > 30 else "DENTRO DE PLAZO"
                elif row['CATEGORIA'] == 'MUEBLES':
                    return "FUERA DE PLAZO" if row['Tiempo'] > 15 else "DENTRO DE PLAZO"
                elif row['Tipo Producto'] == 'CON CONSTRUCCIÓN':
                    return "FUERA DE PLAZO" if row['Tiempo'] > 15 else "DENTRO DE PLAZO"
                elif row['Tipo Producto'] == 'PRODUCTO SOLO':
                    return "FUERA DE PLAZO" if row['Tiempo'] > 4 else "DENTRO DE PLAZO"
                else:
                    return ""

            base['Rango'] = base.apply(evaluar_rango, axis=1)
            base['prexcant'] = base['PRECIO'] * base['CANTIDAD']
            base = base[base['ALIADO COMERCIAL'].str.upper() != 'CARDIF']

            columnas_finales = [
                'RESPONSABLE DE VENTA', 'SEDE', 'ALIADO COMERCIAL', 'CUENTA CONTRATO', 'CLIENTE',
                'DNI', 'TELÉFONO', 'Nro. PEDIDO VENTA', 'IMPORTE (S./)', 'FECHA VENTA', 'ESTADO',
                'ASESOR DE VENTAS', 'PRODUCTO', 'SKU', 'CANTIDAD', 'PRECIO', 'CATEGORIA',
                'Canal de Venta', 'TipoProducto', 'Tiempo', 'Rango', 'prexcant'
            ]
            columnas_existentes = [col for col in columnas_finales if col in base.columns]
            base = base[columnas_existentes]

            for f in glob.glob(os.path.join(self.carpeta_archivos, '*.xlsx')):
                os.remove(f)

            ayer = (datetime.now() - timedelta(days=1)).strftime("%d-%m-%Y")

            archivos_generados = {}

            for grupo, canales in self.canales_por_grupo.items():
                df_grupo = base[base['Canal de Venta'].isin(canales)]
                if df_grupo.empty:
                    continue
                # Determinar el nombre para el archivo
                if grupo == 'LINDA':
                    nombre_para_archivo = "Canales Propios, Proveedor, FFVV y Tiendas Cálidda"
                elif grupo == 'JOSÉ':
                    nombre_para_archivo = "Motos"
                elif grupo == 'EDGAR':
                    nombre_para_archivo = "Materiales y Acabados de Construcción"
                else:
                    nombre_para_archivo = grupo.capitalize()

                nombre_archivo = f"Pendientes de Entrega FNB - {nombre_para_archivo} - {ayer}.xlsx"

                ruta_archivo = os.path.join(self.carpeta_archivos, nombre_archivo)

                with pd.ExcelWriter(ruta_archivo, engine='xlsxwriter') as writer:
                    df_grupo.to_excel(writer, sheet_name='Pendientes', index=False)
                    self._formatear_excel(writer, 'Pendientes', df_grupo)

                archivos_generados[grupo] = ruta_archivo
                print(f"✅ Archivo generado para grupo {grupo}: {nombre_archivo}")

            return base, archivos_generados

        except Exception as e:
            print(f"❌ Error en PASO 1: {e}")
            return None, {}

    # ------------------------ PASO 2 ------------------------

    def paso_2_generar_imagenes_por_canal(self, base):
        print("=== PASO 2: GENERANDO IMÁGENES POR CANAL INDIVIDUAL ===")

        try:
            imagenes_generadas = {}

            # Recorremos todos los canales únicos presentes en la data
            canales_unicos = base['Canal de Venta'].dropna().unique()

            for canal in canales_unicos:
                df_canal = base[base['Canal de Venta'] == canal].copy()
                if df_canal.empty:
                    continue

                df_canal['PERIODO'] = pd.to_datetime(df_canal['FECHA VENTA']).dt.to_period('M').astype(str)

                pivot = df_canal.pivot_table(
                    index='ALIADO COMERCIAL',
                    columns='Rango',
                    values=['prexcant', 'Nro. PEDIDO VENTA'],
                    aggfunc={'prexcant': 'sum', 'Nro. PEDIDO VENTA': pd.Series.nunique},
                    fill_value=0,
                    margins=True,
                    margins_name='TOTAL'
                )

                pivot.columns = [f"{'IMPORTE S/' if col[0] == 'prexcant' else '# TRX'} - {col[1]}" for col in
                                 pivot.columns]
                pivot.reset_index(inplace=True)

                columnas_orden = [
                    'ALIADO COMERCIAL',
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

                pivot = pivot[columnas_orden]

                # NUEVO: Ordenar por IMPORTE S/ - TOTAL de mayor a menor (excluyendo fila TOTAL)
                fila_total = pivot[pivot['ALIADO COMERCIAL'] == 'TOTAL'].copy()
                pivot_sin_total = pivot[pivot['ALIADO COMERCIAL'] != 'TOTAL'].copy()

                # Convertir la columna IMPORTE S/ - TOTAL a numérica para ordenar
                pivot_sin_total['IMPORTE_NUMERICO'] = pivot_sin_total['IMPORTE S/ - TOTAL']
                pivot_sin_total = pivot_sin_total.sort_values('IMPORTE_NUMERICO', ascending=False)
                pivot_sin_total.drop('IMPORTE_NUMERICO', axis=1, inplace=True)

                # Recombinar con la fila TOTAL al final
                pivot = pd.concat([pivot_sin_total, fila_total], ignore_index=True)

                # Formato visual
                for col in pivot.columns:
                    if 'IMPORTE' in col:
                        pivot[col] = pivot[col].astype(int).apply(lambda x: f"{x:,}")
                    elif '# TRX' in col:
                        pivot[col] = pivot[col].astype(int)

                # Insertar saltos de línea en encabezados
                pivot.columns = [
                    col.replace(' - ', '\n').replace('ALIADO COMERCIAL', 'ALIADO\nCOMERCIAL')
                    if isinstance(col, str) else col
                    for col in pivot.columns
                ]

                # Crear figura y tabla con mejor ajuste
                fig, ax = plt.subplots(figsize=(14, max(4.5, len(pivot) * 0.5)))
                ax.axis('off')

                tabla = ax.table(
                    cellText=pivot.values,
                    colLabels=pivot.columns,
                    cellLoc='center',
                    loc='center'
                )

                tabla.auto_set_font_size(False)
                tabla.set_fontsize(9)
                tabla.scale(1.3, 1.6)

                # Ajustar ancho de columnas - ALIADO COMERCIAL más estrecha
                for i in range(len(pivot) + 1):  # +1 para incluir header
                    tabla[(i, 0)].set_width(0.25)  # Columna ALIADO COMERCIAL más estrecha

                # Ajustar otras columnas
                for col in range(1, len(pivot.columns)):
                    for row in range(len(pivot) + 1):
                        tabla[(row, col)].set_width(0.12)

                # Guardar imagen
                nombre_archivo = canal.replace("/", "-").replace("\\", "-")
                ruta_imagen = os.path.join(self.carpeta_imagenes, f"canal_{nombre_archivo}.png")

                # Configurar figura sin fondo y sin márgenes
                plt.subplots_adjust(left=0, right=1, top=1, bottom=0)
                ax.set_xlim(0, 1)
                ax.set_ylim(0, 1)

                # Guardar imagen con dimensiones exactas
                plt.savefig(ruta_imagen, dpi=300, bbox_inches='tight', pad_inches=0,
                            facecolor='white', edgecolor='none', transparent=False)
                plt.close()

                # Recortar imagen usando threshold de color
                img = Image.open(ruta_imagen).convert("RGB")

                # Convertir a array numpy para procesamiento
                import numpy as np
                img_array = np.array(img)

                # Encontrar límites donde NO es completamente blanco
                mask = np.any(img_array < 250, axis=2)  # Pixels que no son blancos puros
                coords = np.argwhere(mask)

                if len(coords) > 0:
                    y0, x0 = coords.min(axis=0)
                    y1, x1 = coords.max(axis=0) + 1
                    img = img.crop((x0, y0, x1, y1))

                # Redimensionar a ancho específico
                new_width = 927
                w_percent = new_width / float(img.size[0])
                new_height = int(float(img.size[1]) * w_percent)
                img_resized = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
                img_resized.save(ruta_imagen)

                imagenes_generadas[canal] = ruta_imagen
                print(f"✅ Imagen generada: canal_{nombre_archivo}.png")

            return imagenes_generadas

        except Exception as e:
            print(f"❌ Error en PASO 2: {e}")
            return {}

    # ------------------------ PASO 3 ------------------------

    def paso_3_enviar_correos_por_canal(self, base, archivos_generados, imagenes_generadas):
        print("=== PASO 3: ENVIANDO CORREOS POR CANAL ===")

        try:
            import locale
            locale.setlocale(locale.LC_TIME, 'es_ES.utf8' if os.name != 'nt' else 'Spanish_Peru')

            ayer = (datetime.now() - timedelta(days=1)).strftime("%d-%m-%Y")

            ruta_listado = os.path.join(self.destinatarios_path, "Listado por canal de venta.xlsx")
            if not os.path.exists(ruta_listado):
                print(f"❌ No se encontró el listado de correos en: {ruta_listado}")
                return []

            if not os.path.exists(self.firma_path):
                print(f"❌ No se encontró la imagen de firma: {self.firma_path}")
                return []

            df_destinatarios = pd.read_excel(ruta_listado)
            if 'Canal' not in df_destinatarios.columns:
                raise ValueError("El archivo de destinatarios debe tener la columna 'Canal'.")
            df_destinatarios.set_index('Canal', inplace=True)

            def parse_emails(val):
                if pd.isna(val):
                    return []
                # Aceptar separadores ';' o ','
                parts = [p.strip() for p in str(val).replace(',', ';').split(';')]
                return [p for p in parts if p]

            outlook = win32.Dispatch('Outlook.Application')
            correos_enviados = []

            # Etiquetas de nombre para asunto del correo en grupo LINDA
            nombre_grupo_linda = "Canales Propios, Proveedor, FFVV y Tiendas Cálidda"

            for grupo, canales in self.canales_por_grupo.items():
                df_grupo = base[base['Canal de Venta'].isin(canales)].copy()
                if df_grupo.empty:
                    continue

                archivo_excel = archivos_generados.get(grupo)

                total_pedidos = df_grupo['Nro. PEDIDO VENTA'].nunique()
                total_importe = df_grupo['prexcant'].sum()

                fuera = df_grupo[df_grupo['Rango'] == 'FUERA DE PLAZO']
                total_fuera = fuera['Nro. PEDIDO VENTA'].nunique()
                importe_fuera = fuera['prexcant'].sum()
                porcentaje_fuera = (total_fuera / total_pedidos) * 100 if total_pedidos > 0 else 0

                saludo = self.saludos_por_grupo[grupo]

                # Lista mensual por FECHA VENTA (guardamos en <ul>/<li> y luego simplificamos a - + <br>)
                df_grupo = df_grupo[df_grupo['FECHA VENTA'].notna()].copy()
                df_grupo['MES_N'] = df_grupo['FECHA VENTA'].dt.month
                df_grupo['MES'] = df_grupo['FECHA VENTA'].dt.strftime('%B').str.lower()

                resumen_mensual = []
                for mes_n in sorted(df_grupo['MES_N'].unique()):
                    df_mes = df_grupo[df_grupo['MES_N'] == mes_n]
                    mes_txt = df_mes['MES'].iloc[0]
                    total_mes = df_mes['Nro. PEDIDO VENTA'].nunique()
                    fuera_mes = df_mes[df_mes['Rango'] == 'FUERA DE PLAZO']['Nro. PEDIDO VENTA'].nunique()
                    porc_mes = (fuera_mes / total_mes * 100) if total_mes > 0 else 0
                    resumen_mensual.append(
                        f"<li>Pendientes de entrega de {mes_txt}: {total_mes} ventas ({fuera_mes} fuera de plazo – {porc_mes:.1f}%)</li>"
                    )
                resumen_html = "<ul>" + "".join(resumen_mensual) + "</ul>"

                # --- Destinatarios desde Excel por ESCENARIO ---
                escenario = self.grupo_a_escenario.get(grupo, grupo)
                if escenario not in df_destinatarios.index:
                    print(f"❌ No se encontró destinatario para escenario '{escenario}' en el Excel.")
                    continue

                directos_raw = df_destinatarios.loc[escenario].get('Destinatarios directos', '')
                copias_raw = df_destinatarios.loc[escenario].get('Destinatarios en copia', '')

                to_list = parse_emails(directos_raw)
                cc_list = parse_emails(copias_raw)

                if not to_list and not cc_list:
                    print(f"⚠️ Escenario '{escenario}' no tiene correos en 'Destinatarios directos' ni 'en copia'.")
                    continue

                to_str = "; ".join(to_list)
                cc_str = "; ".join(cc_list)

                # Crear correo
                mail = outlook.CreateItem(0)
                mail.To = to_str
                mail.CC = cc_str

                # Asunto
                if escenario == 'GENERAL':
                    nombre_para_asunto = "Canales Propios, Proveedor, FFVV y Tiendas Cálidda"
                elif grupo == 'JOSÉ':
                    nombre_para_asunto = "Motos"
                elif grupo == 'EDGAR':
                    nombre_para_asunto = "Materiales y Acabados de Construcción"
                else:
                    nombre_para_asunto = escenario  # DIGITAL u otros

                mail.Subject = f"Pendientes de Entrega FNB - {nombre_para_asunto} - {ayer}"

                if archivo_excel:
                    mail.Attachments.Add(archivo_excel)

                # Insertar imágenes: una por canal
                imagenes_html = ""
                for canal in canales:
                    subtitulo = f"<b>Canal: {canal}</b><br><br>"  # añadimos <br> extra
                    ruta_img = imagenes_generadas.get(canal)
                    if ruta_img and os.path.exists(ruta_img):
                        img_id = f"img_{canal.replace(' ', '_')}"
                        attachment = mail.Attachments.Add(ruta_img)
                        attachment.PropertyAccessor.SetProperty("http://schemas.microsoft.com/mapi/proptag/0x3712001F", img_id)
                        imagen = f'<img src="cid:{img_id}">'
                        imagenes_html += f"<br>{subtitulo}{imagen}<br>"  # <br> extra
                    else:
                        imagenes_html += f"{subtitulo}<i>No se encontró imagen para este canal.</i><br>"

                # Firma
                firma_id = "firmaimg"
                firma_attachment = mail.Attachments.Add(self.firma_path)
                firma_attachment.PropertyAccessor.SetProperty("http://schemas.microsoft.com/mapi/proptag/0x3712001F", firma_id)

                # Cuerpo HTML con <br> extra entre bloques
                html_body = f"""
                <html>
                <body style="font-family: Aptos, sans-serif; font-size: 11pt;">
                Buenos días:<br><br>

                {saludo}, tenemos a la fecha <b>{total_pedidos}</b> pedidos por entregar por un total de <b>S/ {total_importe:,.0f}</b>,
                de los cuales <b>{total_fuera}</b> se encuentran fuera de fecha (<b>S/ {importe_fuera:,.0f}</b>),
                equivalente al <b>{porcentaje_fuera:.2f}%</b>.<br><br>

                {resumen_html}

                {imagenes_html}<br>

                Quedo atento a cualquier observación,<br><br>

                Atentamente,<br><br>
                <img src="cid:{firma_id}"><br><br>
                </body>
                </html>
                """

                # Reemplazo global de <ul>/<li>/<p> por <br> y '-' en viñetas (sin colapsar <br>)
                html_body = self.simplificar_html_global(html_body)
                mail.HTMLBody = html_body

                mail.Send()
                correos_enviados.append(escenario)
                print(f"✅ Correo preparado: {escenario}")

            return correos_enviados

        except Exception as e:
            print(f"❌ Error en PASO 3: {e}")
            return []

    # ------------------------ EJECUCIÓN ------------------------

    def ejecutar_proceso_completo(self):
        """Ejecuta el proceso completo de generación y envío de reportes por canal"""
        print("🚀 INICIANDO SISTEMA DE REPORTES POR CANAL")
        print("=" * 60)

        # PASO 1: Generar archivos por canal
        base, archivos = self.paso_1_generar_archivos_por_canal()
        if base is None or not archivos:
            print("❌ No se pudieron generar archivos Excel. Proceso detenido.")
            return

        print("\n" + "=" * 60)

        # PASO 2: Generar imágenes por canal
        imagenes = self.paso_2_generar_imagenes_por_canal(base)
        if not imagenes:
            print("❌ No se pudieron generar imágenes. Proceso detenido.")
            return

        print("\n" + "=" * 60)

        # PASO 3: Enviar correos
        enviados = self.paso_3_enviar_correos_por_canal(base, archivos, imagenes)

        print("\n" + "=" * 60)
        print("🎉 PROCESO COMPLETADO")
        print(f"📊 Archivos generados: {len(archivos)}")
        print(f"🖼️  Imágenes generadas: {len(imagenes)}")
        print(f"📧 Correos enviados: {len(enviados)}")
        print("=" * 60)


# === EJECUCIÓN PRINCIPAL ===
if __name__ == "__main__":
    sistema = SistemaReportesPorCanal()
    sistema.ejecutar_proceso_completo()

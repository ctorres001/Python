import os
import re
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from PIL import Image, ImageChops
import win32com.client as win32
from datetime import datetime, timedelta


class SistemaSolicitudesAnulacion:
    def __init__(self):
        # === RUTAS (carpetas) - ajusta si necesitas otra ruta ===
        self.folder_base = r"D:\FNB\Reportes\19. Reportes IBR\08. Reporte Solicitudes Anulación\Base"
        self.folder_exonerados = r"D:\FNB\Reportes\19. Reportes IBR\08. Reporte Solicitudes Anulación\Exonerados"
        self.folder_destinatarios = r"D:\FNB\Reportes\19. Reportes IBR\08. Reporte Solicitudes Anulación\Destinatarios"
        self.folder_archivos = r"D:\FNB\Reportes\19. Reportes IBR\08. Reporte Solicitudes Anulación\Archivos"
        self.folder_imagenes = r"D:\FNB\Reportes\19. Reportes IBR\08. Reporte Solicitudes Anulación\Imagenes"

        # Crear carpetas de salida si no existen
        os.makedirs(self.folder_archivos, exist_ok=True)
        os.makedirs(self.folder_imagenes, exist_ok=True)

        # Fecha del reporte = día anterior (formato dd/mm/YYYY)
        self.fecha_reporte = (datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y")

        # === Escenarios: cada valor es lista de tuplas (estado, responsable, etiqueta_caso) ===
        self.escenarios = {
            "GENERAL": [
                ("Derivado al responsable de venta", None, "Caso 1"),
                ("Por derivar al responsable de la venta", None, "Caso 2"),
                ("Procede Anulación", None, "Caso 3"),
                ("Asignado a BO", None, "Caso 4")
            ],
            "IBR PERU": [("Derivado al responsable de venta", "IBR PERU", None)],
            "SALESLAND": [("Derivado al responsable de venta", "SALESLAND", None)],
            "FORTEL": [("Derivado al responsable de venta", "FORTEL", None)],
            "DIGITAL": [
                ("Derivado al responsable de venta", ["CHATBOT", "CrediWeb", "CrediMovil"], "Derivado al responsable de venta"),
                ("Asignado a BO", ["CHATBOT", "CrediWeb", "CrediMovil"], "Asignado a BO")
            ]
        }

    # -----------------------
    # Utilidades de archivos
    # -----------------------
    def buscar_excel_en_carpeta(self, carpeta_o_archivo):
        """
        Si se pasa un archivo válido, lo retorna.
        Si se pasa una carpeta, retorna el primer .xlsx encontrado.
        Lanza FileNotFoundError si no encuentra nada.
        """
        if os.path.isfile(carpeta_o_archivo):
            return carpeta_o_archivo
        if os.path.isdir(carpeta_o_archivo):
            for f in os.listdir(carpeta_o_archivo):
                if f.lower().endswith((".xlsx", ".xls")):
                    return os.path.join(carpeta_o_archivo, f)
        raise FileNotFoundError(f"No se encontró archivo Excel en: {carpeta_o_archivo}")

    def sanitize_filename(self, s: str):
        s = str(s)
        s = re.sub(r'[\\/*?:"<>|]', "_", s)
        s = s.replace(" ", "_")
        return s

    # -----------------------
    # Carga y preprocesamiento
    # -----------------------
    def cargar_y_preprocesar(self):
        """Carga la base, convierte columnas 'fecha', crea TIPO VENTA y excluye exonerados."""
        try:
            ruta_base = self.buscar_excel_en_carpeta(self.folder_base)
        except FileNotFoundError as e:
            print(f"❌ Error: {e}")
            return pd.DataFrame()

        try:
            df = pd.read_excel(ruta_base, dtype=object)
        except Exception as e:
            print(f"❌ No se pudo leer {ruta_base}: {e}")
            return pd.DataFrame()

        # Quitar espacios en nombres de columnas
        df.columns = df.columns.str.strip()

        # Convertir todas las columnas que contengan 'fecha' a datetime (dayfirst=True)
        for col in df.columns:
            if "fecha" in col.lower():
                df[col] = pd.to_datetime(df[col], errors="coerce", format='mixed', dayfirst=True)

        # Crear columna TIPO VENTA si no existe
        if "TIPO VENTA" not in df.columns:
            if "FECHA ENTREGA" in df.columns:
                df["TIPO VENTA"] = np.where(pd.notna(df["FECHA ENTREGA"]), "PRODUCTO ENTREGADO", "PENDIENTE DE ENTREGA")
            else:
                df["TIPO VENTA"] = "PENDIENTE DE ENTREGA"

        # Filtrar exonerados (si existe archivo)
        try:
            ruta_exo = self.buscar_excel_en_carpeta(self.folder_exonerados)
            df_exo = pd.read_excel(ruta_exo, dtype=object)
            df_exo.columns = df_exo.columns.str.strip()
            if "Exonerado" in df_exo.columns and "N° PEDIDO VENTA" in df.columns:
                lista_exo = df_exo["Exonerado"].dropna().astype(str).tolist()
                df = df[~df["N° PEDIDO VENTA"].astype(str).isin(lista_exo)]
        except FileNotFoundError:
            pass
        except Exception as e:
            print(f"⚠️ Error al procesar exonerados: {e}")

        return df

    # -----------------------
    # Generar pivot (tabla) - OPTIMIZADO
    # -----------------------
    def generar_tabla_pivot(self, df, filtro_estado=None, filtro_resp=None):
        """
        Devuelve DataFrame con columnas:
        ['ALIADO COMERCIAL', 'SEDE', 'PENDIENTE DE ENTREGA', 'PRODUCTO ENTREGADO', 'TOTAL']
        Ordena por TOTAL de mayor a menor y asegura que la fila de totales aparezca al final.
        """
        if df is None or df.empty:
            return pd.DataFrame()

        df_fil = df.copy()

        if filtro_estado:
            df_fil = df_fil[df_fil["ESTADO ANULACION"] == filtro_estado]

        if filtro_resp:
            if isinstance(filtro_resp, list):
                df_fil = df_fil[df_fil["RESPONSABLE DE VENTA"].isin(filtro_resp)]
            else:
                df_fil = df_fil[df_fil["RESPONSABLE DE VENTA"] == filtro_resp]

        if df_fil.empty:
            return pd.DataFrame()

        # Determinar columna para contar
        key_for_count = None
        for candidate in ["ID", "Nro. PEDIDO VENTA", "N° PEDIDO VENTA", "Nro. PEDIDO"]:
            if candidate in df_fil.columns:
                key_for_count = candidate
                break

        if not key_for_count:
            # Si no hay columna específica, crear una temporal
            df_fil["_count_helper"] = 1
            key_for_count = "_count_helper"

        # Crear pivot
        try:
            grp = df_fil.groupby(["ALIADO COMERCIAL", "SEDE", "TIPO VENTA"])[key_for_count].count().unstack(
                fill_value=0)
        except Exception as e:
            print(f"⚠️ Error en pivot: {e}")
            return pd.DataFrame()

        # Asegurar ambas columnas
        for col in ["PENDIENTE DE ENTREGA", "PRODUCTO ENTREGADO"]:
            if col not in grp.columns:
                grp[col] = 0

        # Calcular total por fila
        grp["TOTAL"] = grp[["PENDIENTE DE ENTREGA", "PRODUCTO ENTREGADO"]].sum(axis=1)

        # Reset index
        df_pivot = grp.reset_index()

        # ORDENAR POR TOTAL DE MAYOR A MENOR (ANTES de agregar la fila de totales)
        df_pivot = df_pivot.sort_values("TOTAL", ascending=False).reset_index(drop=True)

        # Ordenar columnas
        cols_order = ["ALIADO COMERCIAL", "SEDE", "PENDIENTE DE ENTREGA", "PRODUCTO ENTREGADO", "TOTAL"]
        df_pivot = df_pivot[[c for c in cols_order if c in df_pivot.columns]]

        # Agregar fila de totales AL FINAL
        tot_pend = int(df_pivot["PENDIENTE DE ENTREGA"].sum())
        tot_prod = int(df_pivot["PRODUCTO ENTREGADO"].sum())
        tot_all = int(df_pivot["TOTAL"].sum())

        total_row = {"ALIADO COMERCIAL": "TOTAL", "SEDE": ""}
        if "PENDIENTE DE ENTREGA" in df_pivot.columns:
            total_row["PENDIENTE DE ENTREGA"] = tot_pend
        if "PRODUCTO ENTREGADO" in df_pivot.columns:
            total_row["PRODUCTO ENTREGADO"] = tot_prod
        total_row["TOTAL"] = tot_all

        # Concatenar la fila de totales AL FINAL
        df_pivot = pd.concat([df_pivot, pd.DataFrame([total_row])], ignore_index=True, sort=False)

        # Limpiar columna temporal si se creó
        if "_count_helper" in df_fil.columns:
            df_fil.drop(columns=["_count_helper"], inplace=True)

        return df_pivot

    # -----------------------
    # Formateo Excel (xlsxwriter) - CORREGIDO PARA APLICAR SOLO AL RANGO CON DATOS
    # -----------------------
    def _formatear_excel(self, writer, sheet_name, df):
        workbook = writer.book
        worksheet = writer.sheets[sheet_name]

        # Formatos
        header_format = workbook.add_format({
            'bold': True,
            'font_name': 'Aptos',
            'font_size': 9,
            'bg_color': '#000000',
            'font_color': '#FFFFFF',
            'align': 'center',
            'valign': 'vcenter',
            'border': 1
        })

        text_format = workbook.add_format({
            'font_name': 'Aptos',
            'font_size': 9,
            'align': 'left',
            'valign': 'vcenter',
            'border': 1
        })

        date_format = workbook.add_format({
            'font_name': 'Aptos',
            'font_size': 9,
            'num_format': 'dd/mm/yyyy',
            'align': 'center',
            'valign': 'vcenter',
            'border': 1
        })

        number_format = workbook.add_format({
            'font_name': 'Aptos',
            'font_size': 9,
            'num_format': '#,##0',
            'align': 'right',
            'valign': 'vcenter',
            'border': 1
        })

        # Dimensiones reales de datos
        max_row, max_col = df.shape

        # Formatear SOLO el rango con datos
        for col_idx, col in enumerate(df.columns):
            # Formatear encabezado
            worksheet.write(0, col_idx, col, header_format)

            # Calcular ancho óptimo de columna
            try:
                col_values = df[col].astype(str).replace('nan', '').replace('<NA>', '')
                max_len = max(col_values.str.len().max(), len(str(col)))
            except Exception:
                max_len = len(str(col))
            ancho = min(max_len + 2, 50)

            # Decidir formato según tipo de datos
            col_lower = str(col).lower()
            is_fecha_col = "fecha" in col_lower
            is_numeric_col = pd.api.types.is_numeric_dtype(df[col])

            # Aplicar formato SOLO a las celdas de datos (filas 1 a max_row)
            for row_idx in range(1, max_row + 1):
                cell_value = df.iloc[row_idx - 1, col_idx]

                # Manejar valores nulos/NaT especialmente para fechas
                if pd.isna(cell_value):
                    worksheet.write(row_idx, col_idx, "", text_format)
                elif is_fecha_col and pd.api.types.is_datetime64_any_dtype(df[col]):
                    # Para columnas de fecha, verificar si es NaT antes de escribir
                    if pd.isna(cell_value):
                        worksheet.write(row_idx, col_idx, "", text_format)
                    else:
                        try:
                            # Convertir a datetime de Python si es necesario
                            if hasattr(cell_value, 'to_pydatetime'):
                                cell_value = cell_value.to_pydatetime()
                            worksheet.write(row_idx, col_idx, cell_value, date_format)
                        except:
                            # Si falla la conversión, escribir como texto
                            worksheet.write(row_idx, col_idx, str(cell_value), text_format)
                elif is_numeric_col:
                    worksheet.write(row_idx, col_idx, cell_value, number_format)
                else:
                    worksheet.write(row_idx, col_idx, cell_value, text_format)

            # Establecer ancho de columna
            worksheet.set_column(col_idx, col_idx, ancho)

        # Configurar filtros SOLO en el rango con datos
        if max_row > 0 and max_col > 0:
            worksheet.autofilter(0, 0, max_row, max_col - 1)

        # Ajustar alto de filas para mejor visualización
        worksheet.set_default_row(14)

    # -----------------------
    # Guardar imagen (recorte + reducción 50%) - OPTIMIZADO
    # -----------------------
    def guardar_imagen_tabla(self, df, ruta_img):
        """Genera imagen de la tabla optimizada y la reduce al 50%."""
        if df is None or df.empty:
            return False

        # Estimar tamaño de figura según contenido real
        max_lens = []
        for col in df.columns:
            col_values = df[col].fillna('').astype(str)
            lens = [len(str(col))] + col_values.str.len().tolist()
            max_lens.append(max(lens))

        # Calcular dimensiones
        total_chars = sum(max_lens) + (len(df.columns) * 2)
        fig_w = max(8, total_chars * 0.12)
        fig_h = max(2, (df.shape[0] + 1) * 0.4)

        fig, ax = plt.subplots(figsize=(fig_w, fig_h))
        ax.axis('off')

        # Crear tabla
        tabla = ax.table(
            cellText=df.values,
            colLabels=df.columns,
            cellLoc='center',
            loc='center'
        )
        tabla.auto_set_font_size(False)
        tabla.set_fontsize(9)
        tabla.scale(1.2, 1.5)

        # Guardar con configuración optimizada
        plt.tight_layout()
        try:
            plt.savefig(ruta_img, bbox_inches='tight', dpi=150, pad_inches=0.1,
                        facecolor='white', edgecolor='none')
        except Exception:
            plt.savefig(ruta_img, dpi=150)
        finally:
            plt.close(fig)

        # Optimizar imagen
        try:
            img = Image.open(ruta_img).convert("RGB")

            # Recortar márgenes blancos
            bg = Image.new(img.mode, img.size, (255, 255, 255))
            diff = ImageChops.difference(img, bg)
            bbox = diff.getbbox()
            if bbox:
                img = img.crop(bbox)

            # Reducir al 50%
            new_size = (max(1, img.width // 2), max(1, img.height // 2))
            img = img.resize(new_size, Image.LANCZOS)
            img.save(ruta_img, quality=95, optimize=True)
            return True
        except Exception as e:
            print(f"⚠️ No se pudo optimizar imagen {ruta_img}: {e}")
            return False

    # -----------------------
    # PASO 1: Generar archivos Excel e imágenes - OPTIMIZADO
    # -----------------------
    def paso_1_generar(self):
        print("=== PASO 1: GENERANDO ARCHIVOS EXCEL POR ESCENARIO ===")
        archivos_generados = []
        imagenes_generadas = []

        base = self.cargar_y_preprocesar()
        if base.empty:
            print("⚠️ La base está vacía después del preprocesamiento.")
            return archivos_generados, imagenes_generadas

        fecha_safe = self.sanitize_filename(self.fecha_reporte)

        # Columnas fijas para el Excel
        columnas_fijas = [
            "RESPONSABLE DE VENTA", "SEDE", "ALIADO COMERCIAL", "CUENTA CONTRATO", "CLIENTE", "DNI",
            "N° PEDIDO VENTA", "IMPORTE (S./)", "CRÉDITO UTILIZADO", "NRO. DE CUOTAS", "FECHA VENTA",
            "FECHA ENTREGA", "TIPO DESPACHO", "ESTADO", "ASESOR DE VENTAS", "USUARIO SOLICITANTE",
            "FECHA SOLICITUD", "MOTIVO", "COMENTARIOS", "ESTADO ANULACION",
            "ULTIMO USUARIO ASIGNADO", "TIPO VENTA"
        ]

        for escenario, casos in self.escenarios.items():
            for caso in casos:
                estado, resp, etiqueta = caso

                if escenario == "GENERAL":
                    etiqueta_safe = estado
                else:
                    etiqueta_safe = etiqueta if etiqueta else (resp if resp else estado)
                etiqueta_safe = str(etiqueta_safe).strip()

                # Filtrar detalle
                df_detalle = base.copy()
                if estado:
                    df_detalle = df_detalle[df_detalle["ESTADO ANULACION"] == estado]
                if resp:
                    if isinstance(resp, list):
                        df_detalle = df_detalle[df_detalle["RESPONSABLE DE VENTA"].isin(resp)]
                    else:
                        df_detalle = df_detalle[df_detalle["RESPONSABLE DE VENTA"] == resp]

                if df_detalle.empty:
                    print(f"⚠️ Sin datos para {escenario} / {etiqueta_safe}, se omite.")
                    continue

                # Mantener solo las columnas fijas si existen
                cols_existentes = [c for c in columnas_fijas if c in df_detalle.columns]
                df_detalle = df_detalle[cols_existentes]

                # === Caso FORTEL (TIENDAS CÁLIDDA) ===
                if escenario == "FORTEL":
                    for sede in df_detalle["SEDE"].dropna().unique():
                        df_sede = df_detalle[df_detalle["SEDE"] == sede]
                        df_pivot = self.generar_tabla_pivot(df_sede)

                        # Procesar pivot para FORTEL
                        for col in ["PENDIENTE DE ENTREGA", "PRODUCTO ENTREGADO", "SEDE"]:
                            if col in df_pivot.columns:
                                df_pivot.drop(columns=[col], inplace=True)

                        # Renombrar TOTAL a SOLICITUDES y ordenar
                        df_pivot.rename(columns={"TOTAL": "SOLICITUDES"}, inplace=True)

                        # Separar fila de totales, ordenar datos y reintegrar
                        total_row = df_pivot[df_pivot["ALIADO COMERCIAL"] == "TOTAL"]
                        data_rows = df_pivot[df_pivot["ALIADO COMERCIAL"] != "TOTAL"]
                        data_rows = data_rows.sort_values(by="SOLICITUDES", ascending=False)
                        df_pivot = pd.concat([data_rows, total_row], ignore_index=True)

                        # Generar archivos para FORTEL
                        asunto = f"Reporte de Solicitudes de Anulación - TIENDAS CÁLIDDA - {fecha_safe}"
                        nombre_excel = f"{self.sanitize_filename(asunto)}_{self.sanitize_filename(sede)}.xlsx"
                        ruta_excel = os.path.join(self.folder_archivos, nombre_excel)

                        with pd.ExcelWriter(ruta_excel, engine="xlsxwriter") as writer:
                            df_sede.to_excel(writer, sheet_name="Detalle", index=False)
                            self._formatear_excel(writer, "Detalle", df_sede)

                        archivos_generados.append(("TIENDAS CÁLIDDA", sede, ruta_excel))

                        # Generar imagen
                        nombre_img = f"FORTEL_{self.sanitize_filename(sede)}.png"
                        ruta_img = os.path.join(self.folder_imagenes, nombre_img)
                        if self.guardar_imagen_tabla(df_pivot, ruta_img):
                            imagenes_generadas.append(("TIENDAS CÁLIDDA", sede, ruta_img))
                    continue

                # === Generar pivot para otros escenarios ===
                df_pivot = self.generar_tabla_pivot(df_detalle)

                # Remover columnas innecesarias
                for col in ["PENDIENTE DE ENTREGA", "PRODUCTO ENTREGADO"]:
                    if col in df_pivot.columns:
                        df_pivot.drop(columns=[col], inplace=True)

                # Renombrar TOTAL a SOLICITUDES
                if "TOTAL" in df_pivot.columns:
                    df_pivot.rename(columns={"TOTAL": "SOLICITUDES"}, inplace=True)

                # Separar fila de totales, ordenar datos y reintegrar
                total_row = df_pivot[df_pivot[
                                         "ALIADO COMERCIAL"] == "TOTAL"] if "ALIADO COMERCIAL" in df_pivot.columns else pd.DataFrame()

                # === Procesamiento específico por escenario ===
                if escenario in ["IBR PERU", "SALESLAND", "DIGITAL"]:
                    if "SEDE" in df_pivot.columns:
                        df_pivot.drop(columns=["SEDE"], inplace=True)
                    
                    # REAGRUPAR datos para eliminar duplicados de ALIADO COMERCIAL
                    if not total_row.empty:
                        total_row = total_row[["ALIADO COMERCIAL", "SOLICITUDES"]]
                        data_rows = df_pivot[df_pivot["ALIADO COMERCIAL"] != "TOTAL"]
                        
                        # Consolidar duplicados agrupando por ALIADO COMERCIAL
                        if not data_rows.empty and "SOLICITUDES" in data_rows.columns:
                            data_rows = data_rows.groupby("ALIADO COMERCIAL", as_index=False)["SOLICITUDES"].sum()
                            data_rows = data_rows.sort_values(by="SOLICITUDES", ascending=False)
                            
                            # Recalcular el total correctamente
                            total_row.loc[total_row.index[0], "SOLICITUDES"] = data_rows["SOLICITUDES"].sum()
                            df_pivot = pd.concat([data_rows, total_row], ignore_index=True)
                        else:
                            df_pivot = df_pivot[["ALIADO COMERCIAL", "SOLICITUDES"]]
                    else:
                        df_pivot = df_pivot[["ALIADO COMERCIAL", "SOLICITUDES"]]

                elif escenario == "GENERAL":
                    if estado in ["Derivado al responsable de venta", "Por derivar al responsable de la venta",
                                  "Procede Anulación"]:
                        if "ALIADO COMERCIAL" in df_pivot.columns:
                            df_pivot.drop(columns=["ALIADO COMERCIAL"], inplace=True)
                        if "SEDE" in df_pivot.columns:
                            # Reagrupar por SEDE manteniendo orden
                            data_rows = df_pivot[df_pivot["SEDE"] != ""] if "SEDE" in df_pivot.columns else df_pivot
                            if not data_rows.empty:
                                data_rows = data_rows.groupby("SEDE", as_index=False).sum(numeric_only=True)
                                data_rows = data_rows.sort_values(by="SOLICITUDES", ascending=False)

                                # Calcular nuevo total
                                nuevo_total = {"SEDE": "TOTAL", "SOLICITUDES": data_rows["SOLICITUDES"].sum()}
                                df_pivot = pd.concat([data_rows, pd.DataFrame([nuevo_total])], ignore_index=True)

                    elif estado == "Asignado a BO":
                        if "SEDE" in df_pivot.columns:
                            df_pivot.drop(columns=["SEDE"], inplace=True)
                        if "ALIADO COMERCIAL" in df_pivot.columns:
                            # Reagrupar por ALIADO COMERCIAL manteniendo orden
                            data_rows = df_pivot[df_pivot["ALIADO COMERCIAL"] != "TOTAL"]
                            if not data_rows.empty:
                                data_rows = data_rows.groupby("ALIADO COMERCIAL", as_index=False).sum(numeric_only=True)
                                data_rows = data_rows.sort_values(by="SOLICITUDES", ascending=False)

                                # Calcular nuevo total
                                nuevo_total = {"ALIADO COMERCIAL": "TOTAL",
                                               "SOLICITUDES": data_rows["SOLICITUDES"].sum()}
                                df_pivot = pd.concat([data_rows, pd.DataFrame([nuevo_total])], ignore_index=True)

                # === Generar archivos Excel ===
                if escenario == "GENERAL":
                    nombre_excel = f"{self.sanitize_filename(etiqueta_safe)} - {fecha_safe}.xlsx"
                elif escenario == "DIGITAL":
                    asunto = f"Reporte de Solicitudes de Anulación - {escenario} - {etiqueta_safe} - {fecha_safe}"
                    nombre_excel = f"{self.sanitize_filename(asunto)}.xlsx"
                else:
                    asunto = f"Reporte de Solicitudes de Anulación - {escenario} - {fecha_safe}"
                    nombre_excel = f"{self.sanitize_filename(asunto)}.xlsx"

                ruta_excel = os.path.join(self.folder_archivos, nombre_excel)
                with pd.ExcelWriter(ruta_excel, engine="xlsxwriter") as writer:
                    df_detalle.to_excel(writer, sheet_name="Detalle", index=False)
                    self._formatear_excel(writer, "Detalle", df_detalle)
                archivos_generados.append((escenario, etiqueta_safe, ruta_excel))

                # === Generar imagen ===
                nombre_img = f"{self.sanitize_filename(escenario)}_{self.sanitize_filename(etiqueta_safe)}.png"
                ruta_img = os.path.join(self.folder_imagenes, nombre_img)
                if not df_pivot.empty and self.guardar_imagen_tabla(df_pivot, ruta_img):
                    imagenes_generadas.append((escenario, etiqueta_safe, ruta_img))

        print(f"✅ PASO 1 COMPLETADO: {len(archivos_generados)} archivos, {len(imagenes_generadas)} imágenes")
        return archivos_generados, imagenes_generadas

    # -----------------------
    # Envío de correos - OPTIMIZADO
    # -----------------------
    def enviar_correos(self, archivos_generados, imagenes_generadas):
        import time

        try:
            ruta_dest = self.buscar_excel_en_carpeta(self.folder_destinatarios)
            df_dest = pd.read_excel(ruta_dest, dtype=object)
            df_dest.columns = df_dest.columns.str.strip()
        except Exception as e:
            print(f"❌ No se pudo cargar destinatarios: {e}")
            return

        # Mapear columnas
        df_dest_cols = {c.lower(): c for c in df_dest.columns}
        correo_col = df_dest_cols.get("correo")
        to_col = df_dest_cols.get("destinatarios directos",
                                  df_dest_cols.get("destinatarios_directos", df_dest_cols.get("to")))
        cc_col = df_dest_cols.get("destinatarios en copia",
                                  df_dest_cols.get("destinatarios_en_copia", df_dest_cols.get("cc")))

        if not correo_col:
            print("❌ Archivo destinatarios no tiene columna 'Correo'.")
            return

        outlook = win32.Dispatch("Outlook.Application")
        fecha_safe = self.sanitize_filename(self.fecha_reporte)

        escenarios_unicos = list({item[0] for item in archivos_generados})

        for esc in escenarios_unicos:
            fila = df_dest[df_dest[correo_col].astype(str).str.strip().str.upper() == esc.upper()]
            if fila.empty:
                print(f"⚠️ No hay destinatarios para el escenario {esc}")
                continue

            to = fila[to_col].values[0] if to_col and to_col in fila.columns else ""
            cc = fila[cc_col].values[0] if cc_col and cc_col in fila.columns else ""

            mail = outlook.CreateItem(0)

            # Asunto optimizado
            if esc == "GENERAL":
                asunto = f"Reporte de Solicitudes de Anulación - Bandeja de Anulaciones - {fecha_safe}"
            elif esc == "FORTEL":
                asunto = f"Reporte de Solicitudes de Anulación - Bandeja de Anulaciones - TIENDAS CÁLIDDA - {fecha_safe}"
            else:
                asunto = f"Reporte de Solicitudes de Anulación - Bandeja de Anulaciones - {esc} - {fecha_safe}"

            mail.Subject = asunto
            mail.To = to if pd.notna(to) else ""
            mail.CC = cc if pd.notna(cc) else ""

            # Cargar firma
            mail.Display()
            time.sleep(1.5)
            firma_actual = mail.HTMLBody

            # Cuerpo del correo
            if esc == "GENERAL":
                cuerpo = "Buenos días:<br><br>Se comparte el reporte de solicitudes de anulación para iniciar el proceso de atención.<br><br>"
            else:
                cuerpo = "Buenos días:<br><br>Se comparte el reporte de solicitudes de anulación derivadas a su buzón en la plataforma FNB, por favor su apoyo con la aprobación o rechazo correspondiente directamente en la <b>Bandeja de anulaciones</b>.<br><br>"

            # Adjuntar imágenes y archivos
            for categoria, etiqueta, ruta in imagenes_generadas:
                if categoria == esc:
                    cid = f"img_{self.sanitize_filename(categoria)}_{self.sanitize_filename(etiqueta)}"
                    try:
                        attachment = mail.Attachments.Add(ruta)
                        attachment.PropertyAccessor.SetProperty("http://schemas.microsoft.com/mapi/proptag/0x3712001F",
                                                                cid)

                        # Agregar imagen al cuerpo
                        if esc == "GENERAL":
                            cuerpo += f"<b>{etiqueta}</b><br>"
                        else:
                            cuerpo += f"<b>{etiqueta}</b><br>"
                        cuerpo += f'<br><img src="cid:{cid}"><br><br>'
                    except Exception as e:
                        print(f"⚠️ No se pudo adjuntar imagen {ruta}: {e}")

            # Adjuntar archivos Excel
            for categoria, etiqueta, ruta_excel in archivos_generados:
                if categoria == esc:
                    try:
                        mail.Attachments.Add(ruta_excel)
                    except Exception as e:
                        print(f"⚠️ No se pudo adjuntar Excel {ruta_excel}: {e}")

            # Finalizar cuerpo
            cuerpo += "Quedo atento a cualquier observación,"

            # Insertar cuerpo + firma
            mail.HTMLBody = f"<html><body style='font-family: Aptos, sans-serif; font-size:11pt;'>{cuerpo}{firma_actual}</body></html>"

            try:
                mail.Send()
                print(f"✅ Correo enviado: {esc}")
            except Exception as e:
                print(f"❌ Error enviando correo para {esc}: {e}")

    # -----------------------
    # Ejecutar todo
    # -----------------------
    def ejecutar(self):
        print("🚀 INICIANDO SISTEMA DE REPORTES - SOLICITUDES DE ANULACIÓN")
        print("=" * 60)
        archivos, imagenes = self.paso_1_generar()
        if not archivos:
            print("⚠️ No se generaron archivos. Fin de proceso.")
            return
        self.enviar_correos(archivos, imagenes)
        print("🎉 PROCESO COMPLETADO")


if __name__ == "__main__":
    sistema = SistemaSolicitudesAnulacion()
    sistema.ejecutar()
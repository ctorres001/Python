import pandas as pd
import numpy as np
import os
import matplotlib
matplotlib.use('Agg')  # Usar backend no interactivo
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, time as dt_time
import warnings
import asyncio
from playwright.async_api import async_playwright
from PIL import ImageGrab
import pyperclip
import win32clipboard
import io
from PIL import Image

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')


# ============================================================
# Clase WhatsAppSender (Playwright + Firefox)
# ============================================================
class WhatsAppSender:
    def __init__(self, profile_dir="D:/FNB/Proyectos/Python/Whatsapp_Firefox"):
        self.profile_dir = profile_dir
        self.browser = None
        self.page = None
        self.playwright = None
        self._launch_metrics = {}

    async def inicializar_driver(self):
        try:
            import time, pathlib, shutil
            t0 = time.time()
            print("[1/6] Iniciando Playwright...")
            self.playwright = await async_playwright().start()
            self._launch_metrics['playwright_start_s'] = round(time.time() - t0, 2)

            # Verificar carpeta de perfil y limpiar locks que pueden bloquear
            perfil_path = pathlib.Path(self.profile_dir)
            if perfil_path.exists():
                lock_files = list(perfil_path.glob('**/lock')) + list(perfil_path.glob('**/*.lock')) + list(perfil_path.glob('**/parent.lock'))
                for lf in lock_files:
                    try:
                        lf.unlink()
                    except:
                        pass
            else:
                perfil_path.mkdir(parents=True, exist_ok=True)

            print("[2/6] Lanzando Firefox persistente (perfil existente)...")
            t1 = time.time()
            try:
                self.browser = await self.playwright.firefox.launch_persistent_context(
                    user_data_dir=self.profile_dir,
                    headless=False,
                    viewport={'width': 1280, 'height': 800},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0'
                )
            except Exception as e_persist:
                print(f"   ⚠️ Falló perfil persistente: {e_persist}\n   Intentando perfil limpio temporal...")
                temp_profile = str(perfil_path.parent / (perfil_path.name + '_TEMP'))
                # Limpiar si existe
                try:
                    shutil.rmtree(temp_profile, ignore_errors=True)
                except:
                    pass
                self.browser = await self.playwright.firefox.launch_persistent_context(
                    user_data_dir=temp_profile,
                    headless=False,
                    viewport={'width': 1280, 'height': 800},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0'
                )
                print("   ✅ Perfil temporal iniciado")
            self._launch_metrics['firefox_launch_s'] = round(time.time() - t1, 2)

            print("[3/6] Creando nueva página...")
            t2 = time.time()
            self.page = await self.browser.new_page()
            self._launch_metrics['new_page_s'] = round(time.time() - t2, 2)

            print("[4/6] Navegando a WhatsApp Web...")
            t3 = time.time()
            await self.page.goto("https://web.whatsapp.com", timeout=60000)
            self._launch_metrics['goto_whatsapp_s'] = round(time.time() - t3, 2)

            print("[5/6] Esperando carga inicial selectores...")

            # Lista de posibles selectores para detectar que ya cargó
            posibles_selectores = [
                "[data-testid='chat-list']",
                "div[role='grid']",                # lista de chats
                "div[aria-label='Lista de chats']",# accesibilidad
                "#pane-side",                      # panel lateral
            ]

            loaded = False
            timeout_por_selector = 10000  # 10 segundos por selector
            
            for i, selector in enumerate(posibles_selectores, 1):
                try:
                    print(f"   Intentando selector {i}/{len(posibles_selectores)}: {selector}")
                    await self.page.wait_for_selector(selector, timeout=timeout_por_selector)
                    print(f"✅ WhatsApp cargado correctamente (detectado con: {selector})")
                    loaded = True
                    break
                except Exception as e_selector:
                    print(f"   ⏭️  Selector no encontrado, probando siguiente...")
                    continue

            if not loaded:
                print("[6/6] No se detectó lista de chats inicialmente, comprobando QR...")
                try:
                    qr_canvas = await self.page.query_selector("canvas")
                    if qr_canvas:
                        print("\n⚠️  Código QR visible. Esperando escaneo (hasta 60s)...")
                        for intento in range(12):
                            await asyncio.sleep(5)
                            print(f"   - Intento {intento+1}/12")
                            for selector in posibles_selectores:
                                try:
                                    await self.page.wait_for_selector(selector, timeout=2000)
                                    print("✅ Sesión iniciada tras escaneo QR")
                                    self._launch_metrics['qr_wait_s'] = (intento+1)*5
                                    print("⏱️ Métricas de lanzamiento:", self._launch_metrics)
                                    return True
                                except:
                                    continue
                        raise Exception("Timeout QR: no se escaneó en 60s")
                    else:
                        raise Exception("Interfaz no reconocida (sin chats ni QR)")
                except Exception as e_qr:
                    print(f"   ❌ {e_qr}")
                    print("⏱️ Métricas de lanzamiento:", self._launch_metrics)
                    return False

            print("✅ WhatsApp cargado. ⏱️ Métricas de lanzamiento:", self._launch_metrics)
            return True

        except Exception as e:
            print(f"\n❌ Error inicializando WhatsApp Web: {e}")
            print("\nSugerencias:")
            print("  1. Verifica que Firefox esté cerrado completamente")
            print("  2. Elimina el perfil corrupto: d:/FNB/Proyectos/Python/Whatsapp_Firefox")
            print("  3. Vuelve a escanear el código QR")
            print("  4. Ejecuta: python -m playwright install firefox")
            print("  5. Prueba sin perfil persistente (comentando user_data_dir)")
            return False

    async def buscar_contacto(self, numero: str):
        """Abre el chat del número usando URL directa"""
        try:
            url = f"https://web.whatsapp.com/send?phone={numero.replace('+','').replace(' ','')}"
            print(f"   🔍 Buscando contacto: {numero}")
            
            await self.page.goto(url, timeout=30000)
            await asyncio.sleep(2)  # Esperar que cargue el chat

            # Esperar a que cargue el chat: buscamos el input de mensajes
            print(f"   ⏳ Esperando que abra el chat...")
            await self.page.wait_for_selector("footer div[contenteditable='true']", timeout=15000)
            print(f"   ✅ Chat abierto correctamente")
            return True
        except Exception as e:
            print(f"   ❌ Error abriendo chat: {e}")
            print(f"   Posibles causas: número inválido, conexión lenta, o WhatsApp Web no respondió")
            return False


    async def enviar_mensaje(self, mensaje: str):
        """Envía un mensaje de texto"""
        try:
            box = await self.page.wait_for_selector("footer div[contenteditable='true']", timeout=10000)
            
            # Limpiar el campo antes de escribir
            await box.click()
            await self.page.keyboard.press("Control+A")
            await self.page.keyboard.press("Backspace")
            await asyncio.sleep(0.3)

            for line in mensaje.split("\n"):
                await box.type(line, delay=20)  # Delay para simular escritura natural
                await box.press("Shift+Enter")
            
            await asyncio.sleep(0.5)  # Pequeña pausa antes de enviar
            await box.press("Enter")
            await asyncio.sleep(1)  # Esperar confirmación de envío

            print("      ✅ Mensaje enviado")
            return True
        except Exception as e:
            print(f"      ❌ Error enviando mensaje: {e}")
            return False


    async def enviar_imagen(self, ruta_imagen: str):
        """Envía una imagen pegándola desde el portapapeles"""
        try:
            # Verificar que el archivo existe
            if not os.path.exists(ruta_imagen):
                print(f"      ❌ Archivo no encontrado: {ruta_imagen}")
                return False
            
            # Abrir la imagen con PIL
            image = Image.open(ruta_imagen)

            # Guardar en memoria como BMP
            output = io.BytesIO()
            image.convert("RGB").save(output, "BMP")
            data = output.getvalue()[14:]  # BMP necesita quitar header
            output.close()

            # Copiar al portapapeles
            win32clipboard.OpenClipboard()
            win32clipboard.EmptyClipboard()
            win32clipboard.SetClipboardData(win32clipboard.CF_DIB, data)
            win32clipboard.CloseClipboard()
            await asyncio.sleep(0.3)

            # Focar caja de texto
            box = await self.page.wait_for_selector("footer div[contenteditable='true']", timeout=10000)
            await box.click()
            await asyncio.sleep(0.3)

            # Pegar con Ctrl+V
            await self.page.keyboard.press("Control+V")
            await asyncio.sleep(3)  # esperar que cargue preview (aumentado)

            # Enter para enviar
            await self.page.keyboard.press("Enter")
            await asyncio.sleep(2)  # esperar confirmación de envío

            print(f"      ✅ Imagen enviada: {os.path.basename(ruta_imagen)}")
            return True
        except Exception as e:
            print(f"      ❌ Error enviando imagen: {e}")
            print(f"         Archivo: {os.path.basename(ruta_imagen)}")
            return False

    async def cerrar(self):
        """Cierra navegador y detiene Playwright"""
        try:
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
            print("✅ Sesión de WhatsApp Web cerrada correctamente")
        except Exception as e:
            print(f"⚠️ Error al cerrar WhatsApp Web: {e}")


# ============================================================
# Clase SalesImageGenerator (con formato condicional agregado)
# ============================================================
class SalesImageGenerator:
    def __init__(self):
        self.ruta_canal_fija = r"D:\FNB\Reportes\19. Reportes IBR\Archivos comunes\Canal\Canal.xlsx"
        self.ruta_imagenes = r"D:\FNB\Reportes\19. Reportes IBR\06. Avance de ventas cortes\Imagenes"
        self.columnas_producto = [
            "PRODUCTO", "SKU", "CANTIDAD", "PRECIO", "CATEGORIA", "MARCA", "SUBCANAL",
            "CATEGORIA REAL", "TIPO PRODUCTO", "MODELO PRODUCTO", "SKU2", "DESCRIPCION"
        ]

        # Cargar mapeos / sedes (esto ya lo tenías)
        self.mapeo_canales = self._cargar_mapeo_canales()
        self.sedes_registradas = self._cargar_sedes_registradas()

        # Rangos de 30 minutos (usado por _asignar_rango_hora)
        self.rangos_hora = [
            f"{h:02d}:{m:02d} - {(h + (m + 30) // 60) % 24:02d}:{(m + 30) % 60:02d}"
            for h in range(24) for m in range(0, 60, 30)
        ]

        # -- CORRECCIÓN: cache para acelerar y evitar recalcular rangos --
        self._cache_rango_hora = {}

        # Crear directorio de imágenes si no existe
        os.makedirs(self.ruta_imagenes, exist_ok=True)

        # Estilo matplotlib / seaborn
        plt.style.use('default')
        sns.set_palette("husl")

        plt.rcParams.update({
            'figure.autolayout': False,
            'figure.constrained_layout.use': False,
            'figure.constrained_layout.h_pad': 0,
            'figure.constrained_layout.w_pad': 0,
            'figure.constrained_layout.hspace': 0,
            'figure.constrained_layout.wspace': 0,
            'figure.subplot.hspace': 0,
            'figure.subplot.wspace': 0,
            'figure.subplot.left': 0,
            'figure.subplot.right': 1,
            'figure.subplot.top': 1,
            'figure.subplot.bottom': 0
        })

    def _cargar_mapeo_canales(self):
        try:
            df_canal = pd.read_excel(self.ruta_canal_fija, sheet_name='Hoja1')
            mapeo = pd.Series(df_canal.iloc[:, 2].values,
                              index=df_canal.iloc[:, 0].astype(str).str.strip().str.upper()).to_dict()
            return mapeo
        except Exception as e:
            print(f"Error cargando Canal.xlsx: {e}")
            return {}

    def _cargar_sedes_registradas(self):
        """Carga las sedes registradas desde Canal.xlsx"""
        try:
            df_canal = pd.read_excel(self.ruta_canal_fija, sheet_name='Hoja1')
            # Assuming column 0 has "Nombre Tienda de Venta" and column 1 has "SEDE"
            sedes_registradas = set(df_canal.iloc[:, 0].astype(str).str.strip().str.upper())
            return sedes_registradas
        except Exception as e:
            print(f"Error cargando sedes registradas: {e}")
            return set()

    def _aplicar_formato_condicional(self, tabla, data, col_variacion_idx, num_filas_datos, es_total_idx=None):
        """
        Aplica formato condicional a la columna de variación
        - Fondo rosado claro (#ffcccc) y texto rojo (#cc0000) para valores negativos
        - Fondo verde claro (#ccffcc) y texto verde (#006600) para valores positivos
        - Fondo blanco y texto negro para valores cero
        """
        # Recorrer las filas de datos (sin incluir headers)
        for i in range(num_filas_datos):
            fila_idx = i + 1  # +1 porque la fila 0 es el header
            
            # Obtener el valor de variación de los datos originales
            try:
                valor_texto = data[i][col_variacion_idx]
                # Extraer el valor numérico (remover S/, espacios, comas y +)
                valor_numerico = float(valor_texto.replace('S/ ', '').replace(',', '').replace('+', ''))
                
                if valor_numerico < 0:
                    # Formato para valores negativos: fondo rosado claro y texto rojo
                    tabla[(fila_idx, col_variacion_idx)].set_facecolor('#ffcccc')  # Rosado claro
                    tabla[(fila_idx, col_variacion_idx)].set_text_props(color='#cc0000')  # Rojo
                elif valor_numerico > 0:
                    # Formato para valores positivos: fondo verde claro y texto verde
                    if es_total_idx is not None and i == es_total_idx:
                        # Si es la fila total con valor positivo, usar verde pero mantener negrita
                        tabla[(fila_idx, col_variacion_idx)].set_facecolor('#ccffcc')  # Verde claro
                        tabla[(fila_idx, col_variacion_idx)].set_text_props(weight='bold', color='#006600')  # Verde oscuro
                    else:
                        # Fila normal con valor positivo: verde claro
                        tabla[(fila_idx, col_variacion_idx)].set_facecolor('#ccffcc')  # Verde claro
                        tabla[(fila_idx, col_variacion_idx)].set_text_props(color='#006600')  # Verde oscuro
                else:
                    # Formato para valores cero: mantener formato neutro
                    if es_total_idx is not None and i == es_total_idx:
                        # Si es la fila total con valor cero, mantener el color de fondo gris
                        tabla[(fila_idx, col_variacion_idx)].set_facecolor('#bdc3c7')
                        tabla[(fila_idx, col_variacion_idx)].set_text_props(weight='bold', color='black')
                    else:
                        # Fila normal con valor cero
                        tabla[(fila_idx, col_variacion_idx)].set_facecolor('white')
                        tabla[(fila_idx, col_variacion_idx)].set_text_props(color='black')
                        
            except (ValueError, IndexError) as e:
                # Si hay error al convertir, mantener formato por defecto
                print(f"Error al aplicar formato condicional en fila {i}: {e}")
                continue

    def validar_sedes_nuevas(self, df_anterior, df_nuevo):
        """Valida si hay sedes nuevas que no están registradas en Canal.xlsx"""
        print("\n=== VALIDACIÓN DE SEDES ===")
        
        # Obtener todas las sedes de ambos archivos
        sedes_anterior = set(df_anterior['SEDE'].astype(str).str.strip().str.upper()) if not df_anterior.empty else set()
        sedes_nuevo = set(df_nuevo['SEDE'].astype(str).str.strip().str.upper()) if not df_nuevo.empty else set()
        todas_las_sedes = sedes_anterior.union(sedes_nuevo)
        
        # Filtrar sedes vacías o NaN
        todas_las_sedes = {sede for sede in todas_las_sedes if sede and sede != 'NAN'}
        
        # Encontrar sedes no registradas
        sedes_no_registradas = todas_las_sedes - self.sedes_registradas
        
        if not sedes_no_registradas:
            print("✓ Todas las sedes están registradas correctamente")
            return True
        
        print(f"⚠️  Se encontraron {len(sedes_no_registradas)} sede(s) no registrada(s):")
        for i, sede in enumerate(sorted(sedes_no_registradas), 1):
            print(f"   {i}. {sede}")
        
        print(f"\nSedes registradas disponibles en Canal.xlsx: {len(self.sedes_registradas)}")
        print("Las primeras 10 sedes registradas:")
        for i, sede in enumerate(sorted(list(self.sedes_registradas))[:10], 1):
            print(f"   {i}. {sede}")
        
        while True:
            respuesta = input(f"\n¿Has registrado las nuevas sedes en Canal.xlsx? (s/n): ").strip().lower()
            
            if respuesta in ['s', 'si', 'sí', 'y', 'yes']:
                print("Recargando información de Canal.xlsx...")
                # Recargar mapeo y sedes
                self.mapeo_canales = self._cargar_mapeo_canales()
                self.sedes_registradas = self._cargar_sedes_registradas()
                
                # Verificar nuevamente
                sedes_aun_no_registradas = todas_las_sedes - self.sedes_registradas
                
                if not sedes_aun_no_registradas:
                    print("✓ Perfecto! Todas las sedes están ahora registradas")
                    return True
                else:
                    print(f"❌ Aún quedan {len(sedes_aun_no_registradas)} sede(s) sin registrar:")
                    for sede in sorted(sedes_aun_no_registradas):
                        print(f"   - {sede}")
                    print("Por favor registra estas sedes en Canal.xlsx antes de continuar")
                    return False
                    
            elif respuesta in ['n', 'no']:
                print("❌ Proceso cancelado. Por favor registra las nuevas sedes en Canal.xlsx antes de continuar")
                return False
            else:
                print("Por favor responde 's' para sí o 'n' para no")

    def _procesar_hora_venta(self, df):
        """Procesa la columna HORA VENTA con múltiples formatos"""
        print("Procesando horas...")

        formatos_hora = ['%H:%M:%S', '%H:%M', '%I:%M:%S %p', '%I:%M %p']
        hora_procesada = None

        for formato in formatos_hora:
            try:
                hora_temp = pd.to_datetime(df['HORA VENTA'], format=formato, errors='coerce')
                horas_validas = hora_temp.notna().sum()

                if horas_validas > 0:
                    hora_procesada = hora_temp
                    print(f"   Formato exitoso: {formato} ({horas_validas} horas válidas)")
                    break
            except:
                continue

        if hora_procesada is None or hora_procesada.notna().sum() == 0:
            print("   Asignando horas por defecto...")
            n_registros = len(df)
            horas_default = []

            for i in range(n_registros):
                minutos_del_dia = 480 + (i * 720 // n_registros)
                hora = minutos_del_dia // 60
                minuto = minutos_del_dia % 60
                horas_default.append(dt_time(hour=hora % 24, minute=minuto))

            df['HORA VENTA'] = horas_default
        else:
            df['HORA VENTA'] = hora_procesada.dt.time

        return df

    def _asignar_rango_hora(self, series_hora: pd.Series) -> pd.Series:
        def get_rango(hora):
            if pd.isna(hora) or hora is None:
                return "08:00 - 08:30"

            hora_str = str(hora)
            if hora_str in self._cache_rango_hora:
                return self._cache_rango_hora[hora_str]

            try:
                if isinstance(hora, str):
                    if ':' in hora:
                        hora_obj = datetime.strptime(hora, '%H:%M:%S').time()
                    else:
                        return "08:00 - 08:30"
                elif isinstance(hora, dt_time):
                    hora_obj = hora
                else:
                    return "08:00 - 08:30"

                minutos = hora_obj.hour * 60 + hora_obj.minute
                idx = min(minutos // 30, len(self.rangos_hora) - 1)
                resultado = self.rangos_hora[idx]
                self._cache_rango_hora[hora_str] = resultado
                return resultado
            except:
                return "08:00 - 08:30"

        return series_hora.apply(get_rango)

    def _determinar_canal_venta(self, df: pd.DataFrame) -> pd.Series:
        responsable = df['RESPONSABLE DE VENTA'].astype(str).str.strip().str.upper()
        aliado = df['ALIADO COMERCIAL'].astype(str).str.strip().str.upper()

        try:
            fecha_venta = pd.to_datetime(df['FECHA VENTA'], format='%d/%m/%Y', errors='coerce', dayfirst=True)
        except:
            fecha_venta = pd.to_datetime(df['FECHA VENTA'], errors='coerce', dayfirst=True)

        sede = df['SEDE'].astype(str).str.strip().str.upper()
        categoria = df.get('CATEGORIA_1', pd.Series([''] * len(df))).astype(str).str.strip().str.upper()

        canal = pd.Series([''] * len(df), index=df.index)
        fecha_limite = pd.to_datetime('2024-02-01', format='%Y-%m-%d')
        fecha_limite_1 = pd.to_datetime('2025-08-01', format='%Y-%m-%d')

        # ORDEN CORRECTO: Aplicar de menor a mayor prioridad (RETAIL sobrescribe todo al final)
        
        # 1. Materiales por categoría (EXCEPTO A&G, INCOSER, PROMART que van por SEDE)
        cond_materiales = (categoria == "MATERIALES Y ACABADOS DE CONSTRUCCIÓN") & (~responsable.isin(
            ["A & G INGENIERIA", "INCOSER GAS PERU S.A.C.", "PROMART"]))
        canal.loc[cond_materiales] = "MATERIALES Y ACABADOS DE CONSTRUCCIÓN"
        
        # 2. Grupo Merpes: MATERIALES (si categoría es MATERIALES) o MUEBLES → ambos van a MATERIALES
        cond_merpes = (aliado == "GRUPO MERPES") & (
            (categoria == "MATERIALES Y ACABADOS DE CONSTRUCCIÓN") | (categoria == "MUEBLES")
        )
        canal.loc[cond_merpes] = "MATERIALES Y ACABADOS DE CONSTRUCCIÓN"
        
        # 3. Motos (prioridad sobre MATERIALES pero no sobre RETAIL)
        cond_motos = categoria.isin(["MOTOS", "MOTOS ELECTRICAS", "ACCESORIOS MOTOS"])
        canal.loc[cond_motos] = "MOTOS"
        
        # 4. RETAIL tiene MÁXIMA PRIORIDAD - sobrescribe TODO (motos, materiales, etc.)
        cond_retail = (fecha_venta >= fecha_limite) & (responsable.isin(["CONECTA RETAIL", "INTEGRA RETAIL"]))
        canal.loc[cond_retail] = "RETAIL"
        
        cond_retail_1 = (fecha_venta >= fecha_limite_1) & (responsable.isin(["TOPITOP"]))
        canal.loc[cond_retail_1] = "RETAIL"

        # Identificar registros sin canal asignado que necesitan mapeo por SEDE
        mask_sin_canal = canal == ''
        sedes_sin_canal = sede.loc[mask_sin_canal]
        
        # NUEVA VALIDACIÓN: Verificar sedes no encontradas en el mapeo
        sedes_unicas_sin_canal = sedes_sin_canal.unique()
        sedes_unicas_sin_canal = [s for s in sedes_unicas_sin_canal if s and s != 'NAN']
        
        if len(sedes_unicas_sin_canal) > 0:
            sedes_no_encontradas = []
            for sede_nombre in sedes_unicas_sin_canal:
                if sede_nombre not in self.mapeo_canales:
                    sedes_no_encontradas.append(sede_nombre)
            
            if sedes_no_encontradas:
                print(f"\n❌ ERROR: Se encontraron {len(sedes_no_encontradas)} SEDE(S) no registrada(s) en Canal.xlsx:")
                for i, sede in enumerate(sorted(sedes_no_encontradas), 1):
                    print(f"   {i}. {sede}")
                
                print(f"\nSedes disponibles en Canal.xlsx: {len(self.mapeo_canales)}")
                print("Primeras 10 sedes disponibles:")
                for i, sede in enumerate(sorted(list(self.mapeo_canales.keys()))[:10], 1):
                    print(f"   {i}. {sede}")
                
                print("\n⚠️  PROCESO DETENIDO")
                print("Actualiza el archivo Canal.xlsx agregando las sedes faltantes antes de continuar.")
                print(f"Ruta del archivo: {self.ruta_canal_fija}")
                
                # Detener el proceso
                raise ValueError(f"Sedes no encontradas en mapeo: {', '.join(sedes_no_encontradas)}")

        # Si llegamos aquí, todas las sedes están en el mapeo
        canal.loc[mask_sin_canal] = sede.loc[mask_sin_canal].map(self.mapeo_canales).fillna('')
        canal = canal.replace('CHATBOT', 'DIGITAL')
        
        # Verificación final: identificar registros que aún no tienen canal asignado
        registros_sin_canal = (canal == '') | (canal.isna())
        if registros_sin_canal.any():
            print(f"\n⚠️  ADVERTENCIA: {registros_sin_canal.sum()} registro(s) sin canal asignado después del mapeo:")
            sedes_problema = df.loc[registros_sin_canal, 'SEDE'].unique()
            for sede_problema in sedes_problema[:5]:  # Mostrar máximo 5 ejemplos
                print(f"   - SEDE: {sede_problema}")
            if len(sedes_problema) > 5:
                print(f"   ... y {len(sedes_problema) - 5} más")
        
        return canal

    def procesar_archivo(self, ruta_archivo: str) -> pd.DataFrame:
        """Procesa un archivo Excel de ventas"""
        print(f"-> Cargando: {os.path.basename(ruta_archivo)}")
        df = pd.read_excel(ruta_archivo, engine='openpyxl')

        # --- Convertir fechas ---
        if 'FECHA VENTA' in df.columns:
            df['FECHA VENTA'] = pd.to_datetime(
                df['FECHA VENTA'],
                errors='coerce',
                dayfirst=True
            )

        # --- Filtro por estado ---
        estados_validos = ['PENDIENTE DE ENTREGA', 'ENTREGADO', 'PENDIENTE DE APROBACIÓN']
        if 'ESTADO' in df.columns:
            registros_antes = len(df)
            df = df[df['ESTADO'].isin(estados_validos)].copy()
            registros_despues = len(df)
            print(f"   Filtro ESTADO aplicado: {registros_antes} → {registros_despues} registros")
        else:
            print("   Columna ESTADO no encontrada, continuando sin filtro")

        # --- Procesar horas ---
        df = self._procesar_hora_venta(df)

        # --- Identificar transacciones únicas ---
        columnas_disponibles = df.columns.tolist()
        columnas_g2 = [col for col in self.columnas_producto if col in columnas_disponibles]
        columnas_g1 = [col for col in columnas_disponibles if col not in columnas_g2]

        df['codigo_unico'] = pd.util.hash_pandas_object(
            df[columnas_g1].astype(str).fillna(''),
            index=False
        )

        df_transacciones = df.drop_duplicates('codigo_unico').copy()
        df_transacciones['RANGO HORA'] = self._asignar_rango_hora(df_transacciones['HORA VENTA'])

        # --- Obtener categoría del primer producto ---
        df_productos = df[['codigo_unico'] + columnas_g2].copy()
        df_productos['producto_idx'] = df_productos.groupby('codigo_unico').cumcount() + 1
        df_producto_1 = df_productos[df_productos['producto_idx'] == 1].add_suffix('_1')

        df_final = pd.merge(
            df_transacciones,
            df_producto_1,
            left_on='codigo_unico',
            right_on='codigo_unico_1',
            how='left'
        )

        # --- Asignar canal ---
        df_final['CANAL_VENTA'] = self._determinar_canal_venta(df_final)
        df_final['CANAL_VENTA'] = df_final['CANAL_VENTA'].fillna("NO IDENTIFICADO")
        df_final.loc[df_final['CANAL_VENTA'] == '', 'CANAL_VENTA'] = "NO IDENTIFICADO"

        print(f"   {len(df_final)} transacciones procesadas")
        return df_final

    def recortar_franjas_blancas(self, ruta_imagen):
        """Recortar automáticamente las franjas blancas de una imagen"""
        from PIL import Image
        import numpy as np
        
        try:
            # Abrir imagen
            img = Image.open(ruta_imagen)
            img_array = np.array(img)
            
            # Encontrar los límites no blancos
            def encontrar_limites_no_blancos(array):
                # Convertir a escala de grises si es RGB
                if len(array.shape) == 3:
                    gray = np.mean(array, axis=2)
                else:
                    gray = array
                
                # Encontrar píxeles no blancos (diferentes de 255)
                no_blancos = gray < 255
                
                if not np.any(no_blancos):
                    return 0, 0, array.shape[1], array.shape[0]  # Imagen completamente blanca
                
                # Encontrar límites
                filas_no_blancas = np.any(no_blancos, axis=1)
                columnas_no_blancas = np.any(no_blancos, axis=0)
                
                top = np.argmax(filas_no_blancas)
                bottom = len(filas_no_blancas) - np.argmax(filas_no_blancas[::-1])
                left = np.argmax(columnas_no_blancas)
                right = len(columnas_no_blancas) - np.argmax(columnas_no_blancas[::-1])
                
                return left, top, right, bottom
            
            # Recortar la imagen
            left, top, right, bottom = encontrar_limites_no_blancos(img_array)
            img_recortada = img.crop((left, top, right, bottom))
            
            # Guardar imagen recortada (sobrescribir la original)
            img_recortada.save(ruta_imagen, 'PNG', dpi=(300, 300))
            
            return True
            
        except Exception as e:
            print(f"Error al recortar franjas blancas de {os.path.basename(ruta_imagen)}: {e}")
            return False

    def crear_imagen_resumen_general(self, df_comparativo, fecha_anterior, fecha_nueva, hora_corte):
        """Crear imagen del resumen general SIN DECIMALES y CON FORMATO CONDICIONAL"""
        # Crear figura con configuración específica para eliminar franjas blancas
        fig = plt.figure(figsize=(14, 8), facecolor='white', dpi=300)
        
        # Crear axes que ocupen toda la figura sin márgenes
        ax = fig.add_axes([0, 0, 1, 1])
        ax.set_facecolor('white')
        
        # Preparar datos para la tabla
        data = []
        colores_fila = []
        
        for canal, row in df_comparativo.iterrows():
            # Extraer valores numéricos para comparación
            importe_ant = float(str(row['Importe_Anterior']).replace('S/ ', '').replace(',', ''))
            importe_nue = float(str(row['Importe_Nuevo']).replace('S/ ', '').replace(',', ''))
            variacion = float(str(row['Variación Importe']).replace('S/ ', '').replace(',', ''))
            
            # CAMBIO: Formatear SIN DECIMALES usando :.0f
            data.append([
                canal,
                f"S/ {importe_ant:,.0f}",  # Sin decimales
                f"{int(row['Transacciones_Anterior']):,}",  # Transacciones como entero
                f"S/ {importe_nue:,.0f}",  # Sin decimales
                f"{int(row['Transacciones_Nuevo']):,}",  # Transacciones como entero
                f"S/ {variacion:+,.0f}"  # Sin decimales
            ])
            
            # Determinar color según variación - solo para filas TOTAL
            if canal == 'TOTAL':
                colores_fila.append('#bdc3c7')  # Gris para total
            else:
                colores_fila.append('white')  # Fondo blanco para datos
        
        # Crear tabla
        tabla = ax.table(cellText=data,
                        colLabels=[
                            'Canal',
                            f'Importe {fecha_anterior}',
                            f'# Trx {fecha_anterior}',
                            f'Importe {fecha_nueva}',
                            f'# Trx {fecha_nueva}',
                            'Variación Importe'
                        ],
                        cellLoc='center',
                        loc='center')
        
        # Configurar estilo de la tabla
        tabla.auto_set_font_size(False)
        tabla.set_fontsize(9)
        tabla.scale(1.2, 1.8)
        tabla.set_fontsize(9)
        tabla.auto_set_column_width(col=list(range(6)))
        
        # Ajustar ancho de columnas
        cellDict = tabla.get_celld()
        for i in range(len(data) + 1):  # +1 por el header
            cellDict[(i, 0)].set_width(0.25)  # Canal más ancho
            for j in range(1, 6):
                cellDict[(i, j)].set_width(0.15)
        
        # Aplicar colores
        for i, color in enumerate(colores_fila):
            for j in range(6):
                if i == len(colores_fila) - 1:  # Fila TOTAL
                    tabla[(i+1, j)].set_facecolor('#bdc3c7')
                    tabla[(i+1, j)].set_text_props(weight='bold')
                else:
                    tabla[(i+1, j)].set_facecolor('white')
                    tabla[(i+1, j)].set_text_props(weight='normal')
        
        # NUEVO: Aplicar formato condicional a la columna de variación (índice 5)
        self._aplicar_formato_condicional(tabla, data, 5, len(data), len(data) - 1)
        
        # Estilo de headers
        for j in range(6):
            tabla[(0, j)].set_facecolor('#3498db')
            tabla[(0, j)].set_text_props(weight='bold', color='white')
        
        # Configurar el plot sin título y sin ejes
        ax.axis('off')
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        
        # Guardar imagen sin franjas blancas
        nombre_archivo = f"01_resumen_general_{fecha_anterior.replace('/', '-')}_vs_{fecha_nueva.replace('/', '-')}.png"
        ruta_completa = os.path.join(self.ruta_imagenes, nombre_archivo)
        
        # Configuración específica para eliminar franjas blancas
        plt.savefig(ruta_completa, 
                   dpi=300, 
                   bbox_inches='tight',
                   facecolor='white', 
                   pad_inches=0, 
                   edgecolor='none', 
                   transparent=False, 
                   format='png')
        plt.close()
        
        # Recortar franjas blancas automáticamente
        self.recortar_franjas_blancas(ruta_completa)
        
        return ruta_completa

    def crear_imagen_canal_simple(self, df_anterior, df_nuevo, canal, col_importe, fecha_anterior, fecha_nueva):
        """Crear imagen para canales simples (una vista) CON FORMATO CONDICIONAL"""
        # Crear figura con configuración específica
        fig = plt.figure(figsize=(12, 6), facecolor='white', dpi=300)
        
        # Crear axes que ocupen toda la figura sin márgenes
        ax = fig.add_axes([0, 0, 1, 1])
        ax.set_facecolor('white')
        
        # Configuración de columnas por canal
        if canal == 'ALO CÁLIDDA':
            columna_grupo = 'ASESOR DE VENTAS'
        else:
            columna_grupo = 'SEDE'
        
        # Procesar datos
        def procesar_dataframe(df, sufijo):
            if df.empty:
                return pd.DataFrame()
            
            grupo = df.groupby(columna_grupo).agg(
                Importe=(col_importe, 'sum'),
                Transacciones=('codigo_unico', 'nunique')
            )
            grupo.columns = [f'Importe_{sufijo}', f'Transacciones_{sufijo}']
            return grupo
        
        tabla_anterior = procesar_dataframe(df_anterior, 'Anterior')
        tabla_nuevo = procesar_dataframe(df_nuevo, 'Nuevo')
        
        if tabla_anterior.empty and tabla_nuevo.empty:
            return None
        
        # Combinar tablas
        if tabla_anterior.empty:
            tabla_combinada = tabla_nuevo.copy()
            tabla_combinada['Importe_Anterior'] = 0
            tabla_combinada['Transacciones_Anterior'] = 0
        elif tabla_nuevo.empty:
            tabla_combinada = tabla_anterior.copy()
            tabla_combinada['Importe_Nuevo'] = 0
            tabla_combinada['Transacciones_Nuevo'] = 0
        else:
            tabla_combinada = pd.merge(tabla_anterior, tabla_nuevo, left_index=True, right_index=True, how='outer').fillna(0)
        
        # Calcular variación
        tabla_combinada['Variacion_Importe'] = tabla_combinada['Importe_Nuevo'] - tabla_combinada['Importe_Anterior']
        
        # Preparar datos para la tabla
        data = []
        
        for idx, row in tabla_combinada.iterrows():
            data.append([
                idx,
                f"S/ {row['Importe_Anterior']:,.0f}",
                f"{row['Transacciones_Anterior']:,.0f}",
                f"S/ {row['Importe_Nuevo']:,.0f}",
                f"{row['Transacciones_Nuevo']:,.0f}",
                f"S/ {row['Variacion_Importe']:+,.0f}"
            ])
        
        # Agregar fila de total
        total_importe_ant = tabla_combinada['Importe_Anterior'].sum()
        total_trans_ant = tabla_combinada['Transacciones_Anterior'].sum()
        total_importe_nue = tabla_combinada['Importe_Nuevo'].sum()
        total_trans_nue = tabla_combinada['Transacciones_Nuevo'].sum()
        total_variacion = tabla_combinada['Variacion_Importe'].sum()
        
        data.append([
            f'TOTAL {canal}',
            f"S/ {total_importe_ant:,.0f}",
            f"{total_trans_ant:,.0f}",
            f"S/ {total_importe_nue:,.0f}",
            f"{total_trans_nue:,.0f}",
            f"S/ {total_variacion:+,.0f}"
        ])
        
        # Crear tabla
        tabla = ax.table(cellText=data,
                        colLabels=[
                            columna_grupo,
                            f'Importe {fecha_anterior}',
                            f'Trans. {fecha_anterior}',
                            f'Importe {fecha_nueva}',
                            f'Trans. {fecha_nueva}',
                            'Variación'
                        ],
                        cellLoc='center',
                        loc='center')
        
        # Configurar estilo
        tabla.auto_set_font_size(False)
        tabla.set_fontsize(8)
        tabla.scale(1.2, 1.6)
        
        # Ajustar ancho de columnas
        cellDict = tabla.get_celld()
        for i in range(len(data) + 1):
            cellDict[(i, 0)].set_width(0.3)  # Primera columna más ancha
            for j in range(1, 6):
                cellDict[(i, j)].set_width(0.14)
        
        # Aplicar colores - solo headers y total con fondo
        num_filas = len(data)
        for i in range(num_filas):
            for j in range(6):
                if i == num_filas - 1:  # Fila total
                    tabla[(i+1, j)].set_facecolor('#bdc3c7')
                    tabla[(i+1, j)].set_text_props(weight='bold')
                else:  # Filas de datos
                    tabla[(i+1, j)].set_facecolor('white')
                    tabla[(i+1, j)].set_text_props(weight='normal')
        
        # NUEVO: Aplicar formato condicional a la columna de variación (índice 5)
        self._aplicar_formato_condicional(tabla, data, 5, num_filas, num_filas - 1)
        
        # Headers
        for j in range(6):
            tabla[(0, j)].set_facecolor('#3498db')
            tabla[(0, j)].set_text_props(weight='bold', color='white')
        
        ax.axis('off')
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        
        # Guardar imagen sin franjas blancas
        nombre_archivo = f"{canal.replace(' ', '_').replace('Á', 'A').lower()}_{fecha_anterior.replace('/', '-')}_vs_{fecha_nueva.replace('/', '-')}.png"
        ruta_completa = os.path.join(self.ruta_imagenes, nombre_archivo)
        
        plt.savefig(ruta_completa, 
                   dpi=300, 
                   bbox_inches='tight',  # Usar 'tight' pero con pad_inches=0
                   facecolor='white', 
                   pad_inches=0, 
                   edgecolor='none', 
                   transparent=False, 
                   format='png')
        plt.close()
        
        # Recortar franjas blancas automáticamente
        self.recortar_franjas_blancas(ruta_completa)
        
        return ruta_completa

    def crear_imagen_canal_doble(self, df_anterior, df_nuevo, canal, col_importe, fecha_anterior, fecha_nueva, vista='resumen'):
        """Crear imágenes para canales con doble vista (resumen y detalle) CON FORMATO CONDICIONAL"""
        # Configurar tamaño según vista
        if vista == 'resumen':
            figsize = (12, 6)
        else:
            figsize = (15, 8)
            
        # Crear figura con configuración específica
        fig = plt.figure(figsize=figsize, facecolor='white', dpi=300)
        
        # Crear axes que ocupen toda la figura sin márgenes
        ax = fig.add_axes([0, 0, 1, 1])
        ax.set_facecolor('white')
        
        # Configuración de columnas por canal
        config_canales = {
            'RETAIL': ['ALIADO COMERCIAL', 'SEDE'],
            'MOTOS': ['ALIADO COMERCIAL', 'SEDE'],
            'GRANDES SUPERFICIES': ['ALIADO COMERCIAL', 'SEDE'],
            'MATERIALES Y ACABADOS DE CONSTRUCCIÓN': ['ALIADO COMERCIAL', 'SEDE'],
            'CANAL PROVEEDOR': ['ALIADO COMERCIAL', 'SEDE'],
            'FFVV - PUERTA A PUERTA': ['ALIADO COMERCIAL', 'SEDE']
        }
        
        columnas_grupo = config_canales.get(canal, ['SEDE'])
        
        # Procesar datos
        def procesar_dataframe(df, sufijo):
            if df.empty:
                return pd.DataFrame()
                
            if len(columnas_grupo) == 1:
                grupo = df.groupby(columnas_grupo[0]).agg(
                    Importe=(col_importe, 'sum'),
                    Transacciones=('codigo_unico', 'nunique')
                )
            else:
                grupo = df.groupby(columnas_grupo).agg(
                    Importe=(col_importe, 'sum'),
                    Transacciones=('codigo_unico', 'nunique')
                )
            
            grupo.columns = [f'Importe_{sufijo}', f'Transacciones_{sufijo}']
            return grupo
        
        tabla_anterior = procesar_dataframe(df_anterior, 'Anterior')
        tabla_nuevo = procesar_dataframe(df_nuevo, 'Nuevo')
        
        if tabla_anterior.empty and tabla_nuevo.empty:
            return None
            
        # Combinar tablas
        if tabla_anterior.empty:
            tabla_combinada = tabla_nuevo.copy()
            tabla_combinada['Importe_Anterior'] = 0
            tabla_combinada['Transacciones_Anterior'] = 0
        elif tabla_nuevo.empty:
            tabla_combinada = tabla_anterior.copy()
            tabla_combinada['Importe_Nuevo'] = 0
            tabla_combinada['Transacciones_Nuevo'] = 0
        else:
            tabla_combinada = pd.merge(tabla_anterior, tabla_nuevo, left_index=True, right_index=True, how='outer').fillna(0)
        
        # Calcular variación
        tabla_combinada['Variacion_Importe'] = tabla_combinada['Importe_Nuevo'] - tabla_combinada['Importe_Anterior']
        
        # Preparar datos según la vista
        data = []
        subtotales_info = []  # Para rastrear qué filas son subtotales
        
        if vista == 'resumen' and len(columnas_grupo) > 1:
            # Vista resumen: agrupar por primera columna (ALIADO COMERCIAL) - SIN COLUMNA SEDE
            primer_nivel = tabla_combinada.groupby(level=0)
            
            for grupo_nombre, grupo_data in primer_nivel:
                subtotal_importe_ant = grupo_data['Importe_Anterior'].sum()
                subtotal_trans_ant = grupo_data['Transacciones_Anterior'].sum()
                subtotal_importe_nue = grupo_data['Importe_Nuevo'].sum()
                subtotal_trans_nue = grupo_data['Transacciones_Nuevo'].sum()
                subtotal_variacion = subtotal_importe_nue - subtotal_importe_ant
                
                data.append([
                    grupo_nombre,
                    f"S/ {subtotal_importe_ant:,.0f}",
                    f"{subtotal_trans_ant:,.0f}",
                    f"S/ {subtotal_importe_nue:,.0f}",
                    f"{subtotal_trans_nue:,.0f}",
                    f"S/ {subtotal_variacion:+,.0f}"
                ])
                subtotales_info.append(False)  # En resumen, las filas de datos son blancas
            
            headers = ['Aliado Comercial', f'Importe {fecha_anterior}', f'Trans. {fecha_anterior}',
                      f'Importe {fecha_nueva}', f'Trans. {fecha_nueva}', 'Variación']
            
        else:
            # Vista detallada: mostrar todos los datos con subtotales por ALIADO COMERCIAL
            if len(columnas_grupo) > 1:
                # Agrupar por ALIADO COMERCIAL para crear subtotales
                primer_nivel = tabla_combinada.groupby(level=0)
                
                for grupo_nombre, grupo_data in primer_nivel:
                    # Agregar fila de subtotal del ALIADO COMERCIAL
                    subtotal_importe_ant = grupo_data['Importe_Anterior'].sum()
                    subtotal_trans_ant = grupo_data['Transacciones_Anterior'].sum()
                    subtotal_importe_nue = grupo_data['Importe_Nuevo'].sum()
                    subtotal_trans_nue = grupo_data['Transacciones_Nuevo'].sum()
                    subtotal_variacion = subtotal_importe_nue - subtotal_importe_ant
                    
                    data.append([
                        grupo_nombre,
                        '',  # Sede vacía para subtotal
                        f"S/ {subtotal_importe_ant:,.0f}",
                        f"{subtotal_trans_ant:,.0f}",
                        f"S/ {subtotal_importe_nue:,.0f}",
                        f"{subtotal_trans_nue:,.0f}",
                        f"S/ {subtotal_variacion:+,.0f}"
                    ])
                    subtotales_info.append(True)  # Marcar como subtotal
                    
                    # Agregar filas detalladas por sede (excepto CARDIF)
                    for idx, row in grupo_data.iterrows():
                        aliado = str(idx[0])
                        sede = str(idx[1])
                        
                        # Comprimir CARDIF: no mostrar filas individuales, solo el subtotal
                        if aliado.upper() == 'CARDIF':
                            continue  # Saltar filas individuales de CARDIF
                        else:
                            # Para otros aliados, mostrar aliado y sede separados
                            data.append([
                                aliado,
                                sede,
                                f"S/ {row['Importe_Anterior']:,.0f}",
                                f"{row['Transacciones_Anterior']:,.0f}",
                                f"S/ {row['Importe_Nuevo']:,.0f}",
                                f"{row['Transacciones_Nuevo']:,.0f}",
                                f"S/ {row['Variacion_Importe']:+,.0f}"
                            ])
                            subtotales_info.append(False)  # No es subtotal
            else:
                # Para canales con una sola columna
                for idx, row in tabla_combinada.iterrows():
                    aliado = str(idx)
                    sede = ''
                    data.append([
                        aliado,
                        sede,
                        f"S/ {row['Importe_Anterior']:,.0f}",
                        f"{row['Transacciones_Anterior']:,.0f}",
                        f"S/ {row['Importe_Nuevo']:,.0f}",
                        f"{row['Transacciones_Nuevo']:,.0f}",
                        f"S/ {row['Variacion_Importe']:+,.0f}"
                    ])
                    subtotales_info.append(False)  # No es subtotal
            
            headers = ['Aliado Comercial', 'Sede', f'Importe {fecha_anterior}', f'Trans. {fecha_anterior}',
                      f'Importe {fecha_nueva}', f'Trans. {fecha_nueva}', 'Variación']
        
        # Agregar fila de total
        total_importe_ant = tabla_combinada['Importe_Anterior'].sum()
        total_trans_ant = tabla_combinada['Transacciones_Anterior'].sum()
        total_importe_nue = tabla_combinada['Importe_Nuevo'].sum()
        total_trans_nue = tabla_combinada['Transacciones_Nuevo'].sum()
        total_variacion = tabla_combinada['Variacion_Importe'].sum()
        
        if vista == 'resumen':
            data.append([
                f'TOTAL {canal}',
                f"S/ {total_importe_ant:,.0f}",
                f"{total_trans_ant:,.0f}",
                f"S/ {total_importe_nue:,.0f}",
                f"{total_trans_nue:,.0f}",
                f"S/ {total_variacion:+,.0f}"
            ])
        else:
            data.append([
                f'TOTAL {canal}',
                '',
                f"S/ {total_importe_ant:,.0f}",
                f"{total_trans_ant:,.0f}",
                f"S/ {total_importe_nue:,.0f}",
                f"{total_trans_nue:,.0f}",
                f"S/ {total_variacion:+,.0f}"
            ])
        subtotales_info.append(False)  # El total no es subtotal, es total
        
        # Crear tabla
        tabla = ax.table(cellText=data,
                        colLabels=headers,
                        cellLoc='center',
                        loc='center')
        
        # Configurar estilo
        tabla.auto_set_font_size(False)
        tabla.set_fontsize(7)
        tabla.scale(1.1, 1.4)
        
        # Ajustar ancho de columnas según vista
        cellDict = tabla.get_celld()
        num_columnas = len(headers)
        tabla.auto_set_column_width(col=list(range(num_columnas)))
        
        if vista == 'resumen':
            # Vista resumen: 6 columnas (sin SEDE)
            for i in range(len(data) + 1):
                cellDict[(i, 0)].set_width(0.3)  # Aliado Comercial más ancho
                for j in range(1, 6):
                    cellDict[(i, j)].set_width(0.14)  # Resto de columnas
        else:
            # Vista detalle: 7 columnas (con SEDE)
            for i in range(len(data) + 1):
                cellDict[(i, 0)].set_width(0.25)  # Aliado Comercial
                cellDict[(i, 1)].set_width(0.20)  # Sede
                for j in range(2, 7):
                    cellDict[(i, j)].set_width(0.11)  # Resto de columnas
        
        # Aplicar colores
        num_filas = len(data)
        for i in range(num_filas):
            for j in range(num_columnas):
                if i == num_filas - 1:  # Fila total
                    tabla[(i+1, j)].set_facecolor('#bdc3c7')
                    tabla[(i+1, j)].set_text_props(weight='bold')
                elif vista == 'resumen':  # En resumen, todas las filas de datos son blancas
                    tabla[(i+1, j)].set_facecolor('white')
                    tabla[(i+1, j)].set_text_props(weight='normal')
                elif subtotales_info[i]:  # Filas de subtotal en detalle (mismo color que total)
                    tabla[(i+1, j)].set_facecolor('#bdc3c7')
                    tabla[(i+1, j)].set_text_props(weight='bold')
                else:  # Filas de datos en detalle
                    tabla[(i+1, j)].set_facecolor('white')
                    tabla[(i+1, j)].set_text_props(weight='normal')
        
        # NUEVO: Aplicar formato condicional a la columna de variación
        if vista == 'resumen':
            col_variacion_idx = 5  # Columna de variación en vista resumen
        else:
            col_variacion_idx = 6  # Columna de variación en vista detalle
            
        self._aplicar_formato_condicional(tabla, data, col_variacion_idx, num_filas, num_filas - 1)
        
        # Headers
        for j in range(num_columnas):
            tabla[(0, j)].set_facecolor('#3498db')
            tabla[(0, j)].set_text_props(weight='bold', color='white')
        
        ax.axis('off')
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        
        # Guardar imagen sin franjas blancas
        sufijo_vista = '_resumen' if vista == 'resumen' else '_detalle'
        nombre_archivo = f"{canal.replace(' ', '_').replace('Á', 'A').lower()}{sufijo_vista}_{fecha_anterior.replace('/', '-')}_vs_{fecha_nueva.replace('/', '-')}.png"
        ruta_completa = os.path.join(self.ruta_imagenes, nombre_archivo)
        
        plt.savefig(ruta_completa, 
                   dpi=300, 
                   bbox_inches='tight',  # Usar 'tight' pero con pad_inches=0
                   facecolor='white', 
                   pad_inches=0, 
                   edgecolor='none', 
                   transparent=False, 
                   format='png')
        
        plt.close()
        
        # Recortar franjas blancas automáticamente
        self.recortar_franjas_blancas(ruta_completa)
        
        return ruta_completa

    def generar_todas_las_imagenes(self, df_anterior_filtrado, df_nuevo_filtrado, df_comparativo, 
                                  col_importe, fecha_anterior, fecha_nueva, hora_corte):
        """Generar todas las imágenes según la configuración especificada"""
        imagenes_generadas = []
        
        print("\nGenerando imágenes...")
        
        # 1. Resumen General
        print("1. Creando imagen de Resumen General...")
        ruta_resumen = self.crear_imagen_resumen_general(df_comparativo, fecha_anterior, fecha_nueva, hora_corte)
        if ruta_resumen:
            imagenes_generadas.append(ruta_resumen)
            print(f"   ✓ {os.path.basename(ruta_resumen)}")
        
        # Canales con una sola vista
        canales_simples = ['ALO CÁLIDDA', 'CSC', 'TIENDAS CÁLIDDA', 'DIGITAL']
        
        # 2-5. Canales simples
        for i, canal in enumerate(canales_simples, 2):
            print(f"{i}. Creando imagen para {canal}...")
            
            # Filtrar datos por canal
            df_ant_canal = df_anterior_filtrado[df_anterior_filtrado['CANAL_VENTA'] == canal] if not df_anterior_filtrado.empty else pd.DataFrame()
            df_nue_canal = df_nuevo_filtrado[df_nuevo_filtrado['CANAL_VENTA'] == canal] if not df_nuevo_filtrado.empty else pd.DataFrame()
            
            if not df_ant_canal.empty or not df_nue_canal.empty:
                ruta_imagen = self.crear_imagen_canal_simple(df_ant_canal, df_nue_canal, canal, col_importe, fecha_anterior, fecha_nueva)
                if ruta_imagen:
                    imagenes_generadas.append(ruta_imagen)
                    print(f"   ✓ {os.path.basename(ruta_imagen)}")
                else:
                    print(f"   ✗ No se pudo crear imagen para {canal}")
            else:
                print(f"   ✗ Sin datos para {canal}")
        
        # Canales con doble vista
        canales_dobles = ['RETAIL', 'MOTOS', 'GRANDES SUPERFICIES', 'MATERIALES Y ACABADOS DE CONSTRUCCIÓN', 'CANAL PROVEEDOR', 'FFVV - PUERTA A PUERTA']
        
        # 6-17. Canales dobles (resumen + detalle)
        contador = len(canales_simples) + 2
        for canal in canales_dobles:
            print(f"{contador}. Creando imágenes para {canal}...")
            
            # Filtrar datos por canal
            df_ant_canal = df_anterior_filtrado[df_anterior_filtrado['CANAL_VENTA'] == canal] if not df_anterior_filtrado.empty else pd.DataFrame()
            df_nue_canal = df_nuevo_filtrado[df_nuevo_filtrado['CANAL_VENTA'] == canal] if not df_nuevo_filtrado.empty else pd.DataFrame()
            
            if not df_ant_canal.empty or not df_nue_canal.empty:
                # Vista resumen
                ruta_resumen = self.crear_imagen_canal_doble(df_ant_canal, df_nue_canal, canal, col_importe, fecha_anterior, fecha_nueva, 'resumen')
                if ruta_resumen:
                    imagenes_generadas.append(ruta_resumen)
                    print(f"   ✓ Resumen: {os.path.basename(ruta_resumen)}")
                
                # Vista detalle
                contador += 1
                print(f"{contador}. Creando vista detallada para {canal}...")
                ruta_detalle = self.crear_imagen_canal_doble(df_ant_canal, df_nue_canal, canal, col_importe, fecha_anterior, fecha_nueva, 'detalle')
                if ruta_detalle:
                    imagenes_generadas.append(ruta_detalle)
                    print(f"   ✓ Detalle: {os.path.basename(ruta_detalle)}")
            else:
                print(f"   ✗ Sin datos para {canal}")
                contador += 1  # Incrementar contador aunque no haya datos
            
            contador += 1
        
        return imagenes_generadas

    # ========================================================
    # Envío de reporte WhatsApp (CORREGIDO A ASYNC/AWAIT)
    # ========================================================
    async def enviar_reporte_whatsapp(self, imagenes_generadas, fecha_anterior, fecha_nueva):
        print("\n=== ENVIANDO REPORTE POR WHATSAPP ===")
        print(f"Total de imágenes a enviar: {len(imagenes_generadas)}")
        
        # Modificar para agregar más numeros
        numeros_destino = [
            '51976650091',
            '51940193512'
        ]

        #numeros_destino = ['51962344604']

        whatsapp = WhatsAppSender()
        
        print("\n🔄 Inicializando WhatsApp Web...")
        inicializado = await whatsapp.inicializar_driver()
        
        if not inicializado:
            print("\n❌ No se pudo inicializar WhatsApp Web")
            print("\nAcciones recomendadas:")
            print("  1. Cierra todos los navegadores Firefox abiertos")
            print("  2. Elimina el perfil: D:/FNB/Proyectos/Python/Whatsapp_Firefox")
            print("  3. Ejecuta el script nuevamente y escanea el código QR")
            return False
        
        print("✅ WhatsApp Web inicializado correctamente\n")
        
        try:
            # NUEVO: Determinar saludo según la hora actual
            hora_actual = datetime.now().time()
            if hora_actual < dt_time(12, 0):  # Antes de las 12:00:00
                saludo = "Buenos días, se brinda el comparativo de ventas:"
            else:  # A partir de las 12:00:00
                saludo = "Buenas tardes, se brinda el comparativo de ventas:"
            
            # Estructura de envío con saludo dinámico
            estructura_envio = [
                (saludo, None),  # Usar el saludo dinámico
                ("Resumen General", "01_resumen_general"),
                ("Canal Aló Cálidda", "alo_calidda"),
                ("Canal CSC", "csc"),
                ("Canal Digital", "digital"),
                ("Canal Tiendas Cálidda", "tiendas_calidda"),
                ("Canal Retail", "retail_resumen"),
                ("Detalle Retail", "retail_detalle"),
                ("Canal Motos", "motos_resumen"),
                ("Detalle Motos", "motos_detalle"),
                ("Canal Materiales", "materiales_y_acabados_de_construcción_resumen"),
                ("Detalle Materiales", "materiales_y_acabados_de_construcción_detalle"),
                ("Canal GGSS", "grandes_superficies_resumen"),
                ("Detalle GGSS", "grandes_superficies_detalle"),
                ("Canal Proveedor", "canal_proveedor_resumen"),
                ("Detalle Proveedor", "canal_proveedor_detalle"),
                ("Canal FFVV PaP", "ffvv_-_puerta_a_puerta_resumen"),
                ("Detalle FFVV PaP", "ffvv_-_puerta_a_puerta_detalle")
            ]
            
            imagenes_disponibles = {}
            for ruta in imagenes_generadas:
                nombre_base = os.path.basename(ruta)
                for mensaje, patron_imagen in estructura_envio:
                    if patron_imagen and patron_imagen in nombre_base:
                        imagenes_disponibles[patron_imagen] = ruta
                        break
            
            for idx_numero, numero in enumerate(numeros_destino, 1):
                print(f"\n📱 [{idx_numero}/{len(numeros_destino)}] Enviando reporte al número: {numero}")
                
                if not await whatsapp.buscar_contacto(numero):
                    print(f"   ❌ No se pudo abrir chat con {numero}, continuando con el siguiente...")
                    continue
                
                envios_exitosos = 0
                envios_fallidos = 0
                
                for i, (mensaje, patron_imagen) in enumerate(estructura_envio, 1):
                    try:
                        if patron_imagen is None:
                            # Solo mensaje de texto
                            print(f"   [{i:2d}/{len(estructura_envio)}] {mensaje}")
                            exito = await whatsapp.enviar_mensaje(mensaje)
                            if exito:
                                envios_exitosos += 1
                            else:
                                envios_fallidos += 1
                        else:
                            # Mensaje + imagen
                            if patron_imagen in imagenes_disponibles:
                                ruta_imagen = imagenes_disponibles[patron_imagen]
                                print(f"   [{i:2d}/{len(estructura_envio)}] {mensaje}")
                                
                                # Enviar mensaje
                                exito_msg = await whatsapp.enviar_mensaje(mensaje)
                                if not exito_msg:
                                    envios_fallidos += 1
                                    continue
                                
                                # Enviar imagen
                                exito_img = await whatsapp.enviar_imagen(ruta_imagen)
                                if exito_img:
                                    envios_exitosos += 1
                                else:
                                    envios_fallidos += 1
                            else:
                                print(f"   [{i:2d}/{len(estructura_envio)}] ⚠️ Imagen no disponible: {mensaje}")
                        
                        # Pausa entre envíos (reducido de 4 a 2 segundos)
                        await asyncio.sleep(2)
                        
                    except Exception as e:
                        print(f"      ❌ Error en envío #{i}: {e}")
                        envios_fallidos += 1
                        # Continuar con el siguiente envío
                        continue
                
                print(f"\n   ✅ Reporte completado para {numero}")
                print(f"   📊 Exitosos: {envios_exitosos} | Fallidos: {envios_fallidos}")
            
            return True
        except asyncio.TimeoutError:
            print("\n❌ TIMEOUT: El proceso de envío excedió el tiempo límite")
            print("   Verifica tu conexión a internet y el estado de WhatsApp Web")
            return False
        except Exception as e:
            print(f"\n❌ Error durante el envío: {e}")
            import traceback
            print("\nDetalles del error:")
            traceback.print_exc()
            return False
        finally:
            print("\n🔒 Cerrando WhatsApp Web...")
            await whatsapp.cerrar()


# ============================================================
# Funciones auxiliares
# ============================================================
def determinar_hora_corte():
    hora_actual = datetime.now().time()
    return "nuevo" if hora_actual >= dt_time(12, 0) else "mayor"


def extraer_fecha_nombre(ruta_archivo):
    import re
    nombre = os.path.basename(ruta_archivo)
    patrones = [r'(\d{1,2}[-/]\d{1,2}[-/]\d{4})',
                r'(\d{4}[-/]\d{1,2}[-/]\d{1,2})',
                r'(\d{1,2}[-/]\d{1,2}[-/]\d{2})']
    for patron in patrones:
        match = re.search(patron, nombre)
        if match:
            return match.group(1).replace('-', '/')
    try:
        ts = os.path.getmtime(ruta_archivo)
        return datetime.fromtimestamp(ts).strftime('%d/%m/%Y')
    except:
        return "Sin fecha"


# ============================================================
# Generación de reporte e imágenes
# ============================================================
async def generar_reporte_imagenes():
    ruta_fechas = r"D:\FNB\Reportes\19. Reportes IBR\06. Avance de ventas cortes\Fechas"
    ruta_anterior = os.path.join(ruta_fechas, "Fecha Anterior.xlsx")
    ruta_nueva = os.path.join(ruta_fechas, "Fecha Nueva.xlsx")
    col_importe = "IMPORTE (S./)"

    for ruta in [ruta_anterior, ruta_nueva]:
        if not os.path.exists(ruta):
            print(f"Error: No se encontró {os.path.basename(ruta)}")
            return

    print("Iniciando procesamiento de archivos...")
    generator = SalesImageGenerator()

    df_anterior = generator.procesar_archivo(ruta_anterior)
    df_nuevo = generator.procesar_archivo(ruta_nueva)

    fecha_anterior = df_anterior['FECHA VENTA'].min().strftime('%d/%m/%Y')
    fecha_nueva = df_nuevo['FECHA VENTA'].min().strftime('%d/%m/%Y')

    hora_max_anterior = df_anterior['HORA VENTA'].max()
    hora_max_nueva = df_nuevo['HORA VENTA'].max()
    tipo_corte = determinar_hora_corte()
    hora_corte = hora_max_nueva if tipo_corte == "nuevo" else max(hora_max_anterior, hora_max_nueva)

    df_anterior_filtrado = df_anterior[df_anterior['HORA VENTA'] <= hora_corte].copy()
    df_nuevo_filtrado = df_nuevo[df_nuevo['HORA VENTA'] <= hora_corte].copy()

    def crear_pivot(df, sufijo):
        if df.empty:
            return pd.DataFrame(columns=[f'Importe_{sufijo}', f'Transacciones_{sufijo}'])
        pivot = df.groupby('CANAL_VENTA').agg(
            Importe=(col_importe, 'sum'),
            Transacciones=('codigo_unico', 'nunique')
        )
        pivot.columns = [f'Importe_{sufijo}', f'Transacciones_{sufijo}']
        return pivot

    pivot_anterior = crear_pivot(df_anterior_filtrado, 'Anterior')
    pivot_nuevo = crear_pivot(df_nuevo_filtrado, 'Nuevo')

    if pivot_anterior.empty and pivot_nuevo.empty:
        print("No hay datos para comparar")
        return

    df_comparativo = pd.merge(pivot_anterior, pivot_nuevo, left_index=True, right_index=True, how='outer').fillna(0)
    df_comparativo['Variación Importe'] = df_comparativo['Importe_Nuevo'] - df_comparativo['Importe_Anterior']
    df_comparativo.loc['TOTAL'] = df_comparativo.sum()

    imagenes_generadas = generator.generar_todas_las_imagenes(
        df_anterior_filtrado, df_nuevo_filtrado, df_comparativo,
        col_importe, fecha_anterior, fecha_nueva, hora_corte
    )

    # Enviar reporte con timeout de 10 minutos (600 segundos)
    print(f"\n{'='*60}")
    print("Iniciando envío de reporte por WhatsApp...")
    print(f"Timeout máximo: 10 minutos")
    print(f"{'='*60}")
    
    try:
        await asyncio.wait_for(
            generator.enviar_reporte_whatsapp(imagenes_generadas, fecha_anterior, fecha_nueva),
            timeout=600  # 10 minutos
        )
    except asyncio.TimeoutError:
        print("\n⏱️ TIMEOUT GLOBAL: El proceso completo excedió los 10 minutos")
        print("   El reporte puede estar incompleto")
    except Exception as e:
        print(f"\n❌ Error inesperado: {e}")


# ============================================================
# Main
# ============================================================
async def main():
    print("✅ Dependencias verificadas (Playwright + Firefox)")
    await generar_reporte_imagenes()


if __name__ == "__main__":
    asyncio.run(main())
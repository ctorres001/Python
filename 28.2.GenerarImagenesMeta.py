import asyncio
import io
import os
import time
import tkinter as tk
from datetime import datetime, time as dt_time
from tkinter import filedialog

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import win32clipboard
from PIL import Image
from playwright.async_api import async_playwright

matplotlib.use("Agg")


class CanalMapper:
    def __init__(self, ruta_canal_fija):
        self.ruta_canal_fija = ruta_canal_fija
        self.mapeo_canales = self._cargar_mapeo_canales()

    def _cargar_mapeo_canales(self):
        try:
            df_canal = pd.read_excel(self.ruta_canal_fija, sheet_name="Hoja1")
            mapeo = pd.Series(
                df_canal.iloc[:, 2].values,
                index=df_canal.iloc[:, 0].astype(str).str.strip().str.upper(),
            ).to_dict()
            return mapeo
        except Exception as e:
            print(f"Error cargando Canal.xlsx: {e}")
            return {}

    def _serie_str(self, df, col_name):
        if col_name in df.columns:
            return df[col_name].astype(str).str.strip().str.upper()
        return pd.Series([""] * len(df), index=df.index)

    def _fecha_venta(self, df):
        if "FECHA VENTA" not in df.columns:
            return pd.Series([pd.NaT] * len(df), index=df.index)
        try:
            return pd.to_datetime(df["FECHA VENTA"], format="%d/%m/%Y", errors="coerce", dayfirst=True)
        except Exception:
            return pd.to_datetime(df["FECHA VENTA"], errors="coerce", dayfirst=True)

    def determinar_canal_venta(self, df):
        responsable = self._serie_str(df, "RESPONSABLE DE VENTA")
        aliado = self._serie_str(df, "ALIADO COMERCIAL")
        sede = self._serie_str(df, "SEDE")
        categoria = self._serie_str(df, "CATEGORIA_1")
        fecha_venta = self._fecha_venta(df)

        canal = pd.Series([""] * len(df), index=df.index)
        fecha_limite = pd.to_datetime("2024-02-01", format="%Y-%m-%d")
        fecha_limite_1 = pd.to_datetime("2025-08-01", format="%Y-%m-%d")

        cond_materiales = (categoria == "MATERIALES Y ACABADOS DE CONSTRUCCIÓN") & (~responsable.isin(
            ["A & G INGENIERIA", "INCOSER GAS PERU S.A.C.", "PROMART"]
        ))
        canal.loc[cond_materiales] = "MATERIALES Y ACABADOS DE CONSTRUCCIÓN"

        cond_merpes = (aliado == "GRUPO MERPES") & (
            (categoria == "MATERIALES Y ACABADOS DE CONSTRUCCIÓN") | (categoria == "MUEBLES")
        )
        canal.loc[cond_merpes] = "MATERIALES Y ACABADOS DE CONSTRUCCIÓN"

        cond_motos = categoria.isin(["MOTOS", "MOTOS ELECTRICAS", "ACCESORIOS MOTOS"])
        canal.loc[cond_motos] = "MOTOS"

        cond_retail = (fecha_venta >= fecha_limite) & (responsable.isin(["CONECTA RETAIL", "INTEGRA RETAIL"]))
        canal.loc[cond_retail] = "RETAIL"

        cond_retail_1 = (fecha_venta >= fecha_limite_1) & (responsable.isin(["TOPITOP"]))
        canal.loc[cond_retail_1] = "RETAIL"

        mask_sin_canal = canal == ""
        sedes_sin_canal = sede.loc[mask_sin_canal]
        sedes_unicas_sin_canal = [s for s in sedes_sin_canal.unique() if s and s != "NAN"]

        if sedes_unicas_sin_canal:
            sedes_no_encontradas = [s for s in sedes_unicas_sin_canal if s not in self.mapeo_canales]
            if sedes_no_encontradas:
                print(f"\nERROR: Se encontraron {len(sedes_no_encontradas)} SEDE(S) no registrada(s) en Canal.xlsx:")
                for i, sede_nombre in enumerate(sorted(sedes_no_encontradas), 1):
                    print(f"   {i}. {sede_nombre}")
                print("\nActualiza el archivo Canal.xlsx agregando las sedes faltantes antes de continuar.")
                print(f"Ruta del archivo: {self.ruta_canal_fija}")
                raise ValueError(f"Sedes no encontradas en mapeo: {', '.join(sedes_no_encontradas)}")

        canal.loc[mask_sin_canal] = sede.loc[mask_sin_canal].map(self.mapeo_canales).fillna("")
        canal = canal.replace("CHATBOT", "DIGITAL")
        canal = canal.fillna("NO IDENTIFICADO")
        canal.loc[canal == ""] = "NO IDENTIFICADO"

        return canal


class WhatsAppSender:
    def __init__(self, profile_dir="D:/FNB/Proyectos/Python/Whatsapp_Firefox"):
        self.profile_dir = profile_dir
        self.browser = None
        self.page = None
        self.playwright = None
        self._launch_metrics = {}
        self.use_chromium = False

    async def _aplicar_stealth(self, page):
        await page.add_init_script("""
            delete Object.getPrototypeOf(navigator).webdriver;
            Object.defineProperty(navigator, 'webdriver', {
                get: () => false,
                configurable: true
            });

            if (window.chrome) {
                window.chrome = { runtime: {} };
            }

            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );

            Object.defineProperty(navigator, 'plugins', {
                get: () => [
                    {name: 'PDF Viewer', description: 'Portable Document Format', filename: 'internal-pdf-viewer'},
                    {name: 'Chrome PDF Viewer', description: 'Portable Document Format', filename: 'internal-pdf-viewer'},
                    {name: 'Chromium PDF Viewer', description: 'Portable Document Format', filename: 'internal-pdf-viewer'},
                    {name: 'Microsoft Edge PDF Viewer', description: 'Portable Document Format', filename: 'internal-pdf-viewer'},
                    {name: 'WebKit built-in PDF', description: 'Portable Document Format', filename: 'internal-pdf-viewer'}
                ]
            });

            Object.defineProperty(navigator, 'languages', {
                get: () => ['es-ES', 'es', 'en-US', 'en']
            });
        """)

    async def inicializar_driver(self):
        try:
            import pathlib
            import shutil

            t0 = time.time()
            print("[1/6] Iniciando Playwright...")
            self.playwright = await async_playwright().start()
            self._launch_metrics["playwright_start_s"] = round(time.time() - t0, 2)

            perfil_path = pathlib.Path(self.profile_dir)
            if not perfil_path.exists():
                perfil_path.mkdir(parents=True, exist_ok=True)

            if self.use_chromium:
                print("[2/6] Lanzando Chromium persistente (perfil existente)...")
                t1 = time.time()
                try:
                    self.browser = await self.playwright.chromium.launch_persistent_context(
                        user_data_dir=self.profile_dir,
                        headless=False,
                        viewport={"width": 1280, "height": 800},
                        args=["--disable-blink-features=AutomationControlled"],
                        ignore_https_errors=True,
                        ignore_default_args=["--enable-automation"],
                        user_agent=(
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/120.0.0.0 Safari/537.36"
                        ),
                        bypass_csp=True,
                    )
                    print("   ✅ Chromium iniciado correctamente")
                except Exception as e_persist:
                    print(f"   ⚠️ Falló perfil persistente: {e_persist}\n   Intentando perfil limpio temporal...")
                    temp_profile = str(perfil_path.parent / (perfil_path.name + "_TEMP"))
                    try:
                        shutil.rmtree(temp_profile, ignore_errors=True)
                    except Exception:
                        pass
                    self.browser = await self.playwright.chromium.launch_persistent_context(
                        user_data_dir=temp_profile,
                        headless=False,
                        viewport={"width": 1280, "height": 800},
                        args=["--disable-blink-features=AutomationControlled"],
                        ignore_https_errors=True,
                    )
                    print("   ✅ Perfil temporal iniciado")
                self._launch_metrics["browser_launch_s"] = round(time.time() - t1, 2)
            else:
                if perfil_path.exists():
                    lock_files = list(perfil_path.glob("**/lock")) + list(perfil_path.glob("**/*.lock"))
                    for lf in lock_files:
                        try:
                            lf.unlink()
                        except Exception:
                            pass

                print("[2/6] Lanzando Firefox persistente (perfil existente)...")
                t1 = time.time()

                firefox_prefs = {
                    "dom.webdriver.enabled": False,
                    "useAutomationExtension": False,
                    "dom.indexedDB.experimental": True,
                    "dom.indexedDB.logging.enabled": False,
                    "browser.cache.disk.enable": True,
                    "browser.cache.memory.enable": True,
                }

                try:
                    self.browser = await self.playwright.firefox.launch_persistent_context(
                        user_data_dir=self.profile_dir,
                        headless=False,
                        viewport={"width": 1280, "height": 800},
                        user_agent=(
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) "
                            "Gecko/20100101 Firefox/121.0"
                        ),
                        ignore_https_errors=True,
                        accept_downloads=True,
                        locale="es-ES",
                        timezone_id="America/Lima",
                        firefox_user_prefs=firefox_prefs,
                    )
                    print("   ✅ Firefox iniciado con perfil existente")
                except Exception as e_persist:
                    print(f"   ⚠️ Falló perfil persistente: {e_persist}")
                    print("   🔄 Intentando con perfil limpio...")
                    temp_profile = str(perfil_path.parent / (perfil_path.name + "_TEMP"))
                    try:
                        shutil.rmtree(temp_profile, ignore_errors=True)
                    except Exception:
                        pass
                    self.browser = await self.playwright.firefox.launch_persistent_context(
                        user_data_dir=temp_profile,
                        headless=False,
                        viewport={"width": 1280, "height": 800},
                        user_agent=(
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) "
                            "Gecko/20100101 Firefox/121.0"
                        ),
                        ignore_https_errors=True,
                        firefox_user_prefs=firefox_prefs,
                    )
                    print("   ✅ Perfil temporal iniciado")
                self._launch_metrics["browser_launch_s"] = round(time.time() - t1, 2)

            print("[3/6] Creando nueva página...")
            t2 = time.time()
            self.page = await self.browser.new_page()
            self._launch_metrics["new_page_s"] = round(time.time() - t2, 2)

            print("[3.5/6] Aplicando técnicas anti-detección...")
            await self._aplicar_stealth(self.page)

            print("[4/6] Navegando a WhatsApp Web...")
            t3 = time.time()

            max_reintentos = 3
            for intento in range(max_reintentos):
                try:
                    if intento > 0:
                        print(f"   🔄 Reintento {intento}/{max_reintentos-1}...")
                        await self.page.evaluate("""() => {
                            localStorage.clear();
                            sessionStorage.clear();
                        }""")
                        await asyncio.sleep(2)

                    await self.page.goto("https://web.whatsapp.com", timeout=60000, wait_until="domcontentloaded")
                    await asyncio.sleep(5)

                    error_db = await self.page.query_selector_all("text=/error en la base de datos/i")
                    if error_db:
                        print("   ⚠️  Error de base de datos detectado, limpiando y recargando...")
                        await self.page.evaluate("""() => {
                            indexedDB.databases().then(dbs => {
                                dbs.forEach(db => indexedDB.deleteDatabase(db.name));
                            });
                            localStorage.clear();
                            sessionStorage.clear();
                        }""")
                        await asyncio.sleep(2)
                        await self.page.reload(timeout=60000, wait_until="domcontentloaded")
                        await asyncio.sleep(5)

                    error_elementos = await self.page.query_selector_all("text=/error inesperado/i")
                    if error_elementos:
                        print("   ⚠️  Detectado error en WhatsApp Web, recargando...")
                        await self.page.reload(timeout=60000, wait_until="domcontentloaded")
                        await asyncio.sleep(3)

                    break
                except Exception as e_goto:
                    if intento < max_reintentos - 1:
                        print(f"   ⚠️  Error al cargar: {e_goto}, reintentando...")
                        await asyncio.sleep(5)
                    else:
                        raise

            self._launch_metrics["goto_whatsapp_s"] = round(time.time() - t3, 2)

            print("[5/6] Esperando carga inicial selectores...")

            await asyncio.sleep(5)
            posibles_selectores = [
                "[data-testid='chat-list']",
                "div[role='grid']",
                "div[aria-label='Lista de chats']",
                "#pane-side",
            ]

            loaded = False
            timeout_por_selector = 10000

            for selector in posibles_selectores:
                try:
                    await self.page.wait_for_selector(selector, timeout=timeout_por_selector)
                    print(f"✅ Sesión detectada con: {selector}")
                    loaded = True
                    break
                except Exception:
                    continue

            print("\n" + "=" * 70)
            print("📱 VERIFICACIÓN DE VINCULACIÓN")
            print("=" * 70)

            if loaded:
                print("\n✅ Se detectó una sesión activa de WhatsApp Web")
                print("\n🔍 ACCIÓN REQUERIDA:")
                print("   1. Revisa la ventana de Firefox que se abrió")
                print("   2. VERIFICA que puedes ver tus chats de WhatsApp")
                print("   3. Si NO ves tus chats:")
                print("      • Escanea el código QR con tu teléfono")
                print("      • Marca 'Mantener sesión iniciada'")
                print("\n⏳ Esperando 10 segundos para que verifiques...")
                print("   (Si ves tus chats, el script continuará automáticamente)\n")
                await asyncio.sleep(10)

                sesion_verificada = False
                for selector in posibles_selectores:
                    try:
                        await self.page.wait_for_selector(selector, timeout=3000)
                        sesion_verificada = True
                        break
                    except Exception:
                        continue

                if sesion_verificada:
                    print("✅ Sesión verificada correctamente\n")
                else:
                    print("⚠️ Se perdió la sesión, esperando nueva vinculación...\n")
                    loaded = False

            if not loaded:
                print("\n🔍 Verifica la ventana de Firefox que se abrió:")
                print("   1. Si ves un código QR:")
                print("      • Abre WhatsApp en tu teléfono")
                print("      • Ve a: Configuración → Dispositivos vinculados")
                print("      • Toca 'Vincular un dispositivo'")
                print("      • Escanea el código QR")
                print("      • ✅ MARCA la casilla 'Mantener sesión iniciada'")
                print("\n   2. Si ves un mensaje de error:")
                print("      • Intenta recargar la página (F5)")
                print("      • Espera a que aparezca el código QR")
                print("\n   3. Si ya ves tus chats de WhatsApp:")
                print("      • Perfecto, ya estás vinculado")
                print("\n" + "=" * 70)
                print("\n⏳ Esperando vinculación...")
                print("   El script verificará automáticamente cada 5 segundos.")
                print("   Presiona Ctrl+C si deseas cancelar.\n")

                for intento in range(60):
                    await asyncio.sleep(5)
                    for selector in posibles_selectores:
                        try:
                            await self.page.wait_for_selector(selector, timeout=2000)
                            print("\n✅ ¡Sesión activa detectada!")
                            print(f"   Tiempo de espera: {(intento + 1) * 5} segundos")
                            loaded = True
                            break
                        except Exception:
                            continue

                    if loaded:
                        break

                    if (intento + 1) % 2 == 0:
                        tiempo_transcurrido = (intento + 1) * 5
                        print(f"   ⏱️  Esperando... ({tiempo_transcurrido}s / 300s)")

                if not loaded:
                    print("\n❌ Timeout: No se detectó sesión activa después de 5 minutos")
                    print("   Ejecuta el script nuevamente cuando estés listo.")
                    return False

            print("✅ WhatsApp cargado. ⏱️ Métricas de lanzamiento:", self._launch_metrics)
            return True

        except Exception as e:
            print(f"\n❌ Error inicializando WhatsApp Web: {e}")
            print("\n🔧 Acciones recomendadas para resolver el error:")
            print("  1. Cierra TODOS los navegadores Firefox abiertos")
            print("  2. Elimina el perfil (puede estar corrupto):")
            print("     rmdir /s /q D:\\FNB\\Proyectos\\Python\\Whatsapp_Firefox")
            print("  3. Si WhatsApp Web muestra 'error inesperado':")
            print("     - Verifica tu conexión a internet")
            print("     - Intenta abrir https://web.whatsapp.com manualmente en Firefox")
            print("     - Espera unos minutos y vuelve a intentar")
            print("  4. Asegúrate de que Playwright y Firefox estén instalados:")
            print("     python -m playwright install firefox")
            print("  5. Si el problema persiste, prueba desconectar otros dispositivos de WhatsApp")
            return False

    async def buscar_contacto(self, numero):
        try:
            url_actual = self.page.url
            if "post_logout" in url_actual or "logout_reason" in url_actual:
                print("   ⚠️ La sesión de WhatsApp fue cerrada, no se puede continuar")
                print("   💡 Solución: Ejecuta el script nuevamente y escanea el código QR")
                return False

            url = f"https://web.whatsapp.com/send?phone={numero.replace('+', '').replace(' ', '')}"
            print(f"   🔍 Buscando contacto: {numero}")

            await self.page.goto(url, timeout=40000, wait_until="domcontentloaded")
            await asyncio.sleep(5)

            url_actual = self.page.url
            if "post_logout" in url_actual or "logout_reason" in url_actual:
                print("   ⚠️ WhatsApp cerró la sesión al intentar abrir el chat")
                print("   💡 Esto indica que la sesión no está vinculada correctamente")
                return False

            print("   ⏳ Esperando que abra el chat...")
            selectores_chat = [
                "footer div[contenteditable='true']",
                "[data-testid='conversation-compose-box-input']",
                "div[contenteditable='true'][data-tab='10']",
                "div[role='textbox']",
            ]

            for selector in selectores_chat:
                try:
                    await self.page.wait_for_selector(selector, timeout=8000)
                    print("   ✅ Chat abierto correctamente")
                    return True
                except Exception:
                    continue

            numero_invalido = await self.page.query_selector_all("text=/número de teléfono/i")
            if numero_invalido:
                print(f"   ❌ El número {numero} no existe o es inválido")
            else:
                print("   ⚠️ No se pudo detectar el cuadro de texto del chat")
            return False
        except Exception as e:
            print(f"   ❌ Error abriendo chat: {e}")
            url_actual = self.page.url
            if "post_logout" in url_actual:
                print(f"   🔍 URL actual: {url_actual}")
                print("   ⚠️ La sesión fue cerrada por WhatsApp")
                print("   💡 Necesitas vincular el dispositivo escaneando el código QR")
            else:
                print("   Posibles causas: número inválido, conexión lenta, o WhatsApp Web no respondió")
            return False

    async def enviar_mensaje(self, mensaje):
        try:
            box = await self.page.wait_for_selector("footer div[contenteditable='true']", timeout=10000)

            await box.click()
            await self.page.keyboard.press("Control+A")
            await self.page.keyboard.press("Backspace")
            await asyncio.sleep(0.3)

            for line in mensaje.split("\n"):
                await box.type(line, delay=20)
                await box.press("Shift+Enter")

            await asyncio.sleep(0.5)
            await box.press("Enter")
            await asyncio.sleep(1)

            print("      ✅ Mensaje enviado")
            return True
        except Exception as e:
            print(f"      ❌ Error enviando mensaje: {e}")
            return False

    async def enviar_imagen(self, ruta_imagen):
        try:
            if not os.path.exists(ruta_imagen):
                print(f"      ❌ Archivo no encontrado: {ruta_imagen}")
                return False

            image = Image.open(ruta_imagen)
            output = io.BytesIO()
            image.convert("RGB").save(output, "BMP")
            data = output.getvalue()[14:]
            output.close()

            win32clipboard.OpenClipboard()
            win32clipboard.EmptyClipboard()
            win32clipboard.SetClipboardData(win32clipboard.CF_DIB, data)
            win32clipboard.CloseClipboard()
            await asyncio.sleep(0.3)

            box = await self.page.wait_for_selector("footer div[contenteditable='true']", timeout=10000)
            await box.click()
            await asyncio.sleep(0.3)

            await self.page.keyboard.press("Control+V")
            await asyncio.sleep(3)

            await self.page.keyboard.press("Enter")
            await asyncio.sleep(2)

            print(f"      ✅ Imagen enviada: {os.path.basename(ruta_imagen)}")
            return True
        except Exception as e:
            print(f"      ❌ Error enviando imagen: {e}")
            print(f"         Archivo: {os.path.basename(ruta_imagen)}")
            return False

    async def cerrar(self):
        try:
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
            print("✅ Sesión de WhatsApp Web cerrada correctamente")
        except Exception as e:
            print(f"⚠️ Error al cerrar WhatsApp Web: {e}")


class SalesImageGenerator:
    def __init__(self):
        self.ruta_canal_fija = r"D:\FNB\Reportes\19. Reportes IBR\Archivos comunes\Canal\Canal.xlsx"
        self.ruta_imagenes = r"D:\FNB\Reportes\28. Avance ventas por cortes\Imagenes"
        self.ruta_metas = r"D:\FNB\Reportes\28. Avance ventas por cortes\Metas_diarias_v1.xlsx"
        self.col_importe = "IMPORTE (S./)"
        self.columnas_producto = [
            "PRODUCTO",
            "SKU",
            "CANTIDAD",
            "PRECIO",
            "CATEGORIA",
            "MARCA",
            "SUBCANAL",
            "CATEGORIA REAL",
            "TIPO PRODUCTO",
            "MODELO PRODUCTO",
            "SKU2",
            "DESCRIPCION",
        ]

        os.makedirs(self.ruta_imagenes, exist_ok=True)

        plt.style.use("default")
        sns.set_palette("husl")

        plt.rcParams.update({
            "figure.autolayout": False,
            "figure.constrained_layout.use": False,
            "figure.constrained_layout.h_pad": 0,
            "figure.constrained_layout.w_pad": 0,
            "figure.constrained_layout.hspace": 0,
            "figure.constrained_layout.wspace": 0,
            "figure.subplot.hspace": 0,
            "figure.subplot.wspace": 0,
            "figure.subplot.left": 0,
            "figure.subplot.right": 1,
            "figure.subplot.top": 1,
            "figure.subplot.bottom": 0,
        })

    def seleccionar_archivo_excel(self):
        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        archivo = filedialog.askopenfilename(
            title="Seleccionar archivo de ventas",
            filetypes=[("Excel files", "*.xlsx;*.xls")],
        )
        root.destroy()
        return archivo

    def limpiar_importe(self, serie):
        return pd.to_numeric(
            serie.astype(str)
            .str.replace("S/", "", regex=False)
            .str.replace(",", "", regex=False)
            .str.strip(),
            errors="coerce",
        ).fillna(0)

    def preparar_transacciones(self, df):
        columnas_disponibles = df.columns.tolist()
        columnas_g2 = [col for col in self.columnas_producto if col in columnas_disponibles]
        columnas_g1 = [col for col in columnas_disponibles if col not in columnas_g2]

        df["codigo_unico"] = pd.util.hash_pandas_object(
            df[columnas_g1].astype(str).fillna(""),
            index=False,
        )

        df_transacciones = df.drop_duplicates("codigo_unico").copy()

        df_productos = df[["codigo_unico"] + columnas_g2].copy()
        df_productos["producto_idx"] = df_productos.groupby("codigo_unico").cumcount() + 1
        df_producto_1 = df_productos[df_productos["producto_idx"] == 1].add_suffix("_1")

        df_final = pd.merge(
            df_transacciones,
            df_producto_1,
            left_on="codigo_unico",
            right_on="codigo_unico_1",
            how="left",
        )

        return df_final

    def cargar_metas_diarias(self):
        def parse_fecha(valor):
            if pd.isna(valor):
                return pd.NaT
            if isinstance(valor, (int, float)):
                try:
                    return pd.to_datetime(valor, origin="1899-12-30", unit="D", errors="coerce")
                except Exception:
                    return pd.NaT
            texto = str(valor).strip()
            if not texto:
                return pd.NaT
            if "-" in texto and texto[:4].isdigit():
                return pd.to_datetime(texto, errors="coerce", dayfirst=False)
            return pd.to_datetime(texto, errors="coerce", dayfirst=True)

        df_header = pd.read_excel(self.ruta_metas, sheet_name="META", usecols="A:AG", header=0)
        if not df_header.empty:
            canal_col = df_header.columns[0]
            meta_cols = list(df_header.columns[1:])
            col_dates = {}
            valid_meta_cols = []
            for col in meta_cols:
                fecha_col = parse_fecha(col)
                if pd.notna(fecha_col):
                    col_dates[col] = fecha_col.date()
                    valid_meta_cols.append(col)

            if valid_meta_cols:
                df_header = df_header.dropna(subset=[canal_col]).copy()
                df_long = df_header[[canal_col] + valid_meta_cols].melt(
                    id_vars=[canal_col],
                    var_name="FECHA_META",
                    value_name="META",
                )
                df_long["FECHA_META"] = df_long["FECHA_META"].map(col_dates)
                df_long["CANAL_META"] = df_long[canal_col].astype(str).str.strip().str.upper()
                return df_long

        df_raw = pd.read_excel(self.ruta_metas, sheet_name="META", usecols="A:AG", header=None)
        if df_raw.empty:
            raise ValueError("La hoja META esta vacia")

        def contar_fechas(row):
            valores = row.iloc[1:]
            fechas = valores.apply(parse_fecha)
            return fechas.notna().sum()

        conteos = df_raw.apply(contar_fechas, axis=1)
        idx_fecha = conteos.idxmax()
        if conteos.loc[idx_fecha] == 0:
            raise ValueError("No se encontraron encabezados de fecha en la hoja META")

        fila_fechas = df_raw.loc[idx_fecha]
        fechas_cols = fila_fechas.iloc[1:]
        fechas_parseadas = fechas_cols.apply(parse_fecha)

        if fechas_parseadas.notna().sum() == 0:
            raise ValueError("No se pudieron interpretar fechas en la hoja META")

        col_dates = {}
        valid_col_idx = []
        for i, fecha_col in enumerate(fechas_parseadas, start=1):
            if pd.notna(fecha_col):
                col_dates[i] = fecha_col.date()
                valid_col_idx.append(i)

        df_meta = df_raw.iloc[idx_fecha + 1 :].copy()
        df_meta = df_meta.dropna(subset=[0])

        columnas = ["CANAL"] + [col_dates[i] for i in valid_col_idx]
        df_meta = df_meta[[0] + valid_col_idx]
        df_meta.columns = columnas

        df_long = df_meta.melt(
            id_vars=["CANAL"],
            var_name="FECHA_META",
            value_name="META",
        )
        df_long["CANAL_META"] = df_long["CANAL"].astype(str).str.strip().str.upper()

        return df_long

    def recortar_franjas_blancas(self, ruta_imagen):
        try:
            img = Image.open(ruta_imagen)
            img_array = np.array(img)

            def encontrar_limites_no_blancos(array):
                if len(array.shape) == 3:
                    gray = np.mean(array, axis=2)
                else:
                    gray = array

                no_blancos = gray < 255

                if not np.any(no_blancos):
                    return 0, 0, array.shape[1], array.shape[0]

                filas_no_blancas = np.any(no_blancos, axis=1)
                columnas_no_blancas = np.any(no_blancos, axis=0)

                top = np.argmax(filas_no_blancas)
                bottom = len(filas_no_blancas) - np.argmax(filas_no_blancas[::-1])
                left = np.argmax(columnas_no_blancas)
                right = len(columnas_no_blancas) - np.argmax(columnas_no_blancas[::-1])

                return left, top, right, bottom

            left, top, right, bottom = encontrar_limites_no_blancos(img_array)
            img_recortada = img.crop((left, top, right, bottom))
            img_recortada.save(ruta_imagen, "PNG", dpi=(300, 300))

            return True
        except Exception as e:
            print(f"Error al recortar franjas blancas de {os.path.basename(ruta_imagen)}: {e}")
            return False

    def _aplicar_formato_condicional_alcance(self, tabla, data, col_idx, num_filas, total_idx=None):
        for i in range(num_filas):
            fila_idx = i + 1
            try:
                valor_texto = data[i][col_idx]
                valor_numerico = float(valor_texto.replace("%", "")) / 100

                if valor_numerico < 1:
                    tabla[(fila_idx, col_idx)].set_facecolor("#ffcccc")
                    tabla[(fila_idx, col_idx)].set_text_props(color="#cc0000")
                elif valor_numerico > 1:
                    if total_idx is not None and i == total_idx:
                        tabla[(fila_idx, col_idx)].set_facecolor("#ccffcc")
                        tabla[(fila_idx, col_idx)].set_text_props(weight="bold", color="#006600")
                    else:
                        tabla[(fila_idx, col_idx)].set_facecolor("#ccffcc")
                        tabla[(fila_idx, col_idx)].set_text_props(color="#006600")
                else:
                    if total_idx is not None and i == total_idx:
                        tabla[(fila_idx, col_idx)].set_facecolor("#bdc3c7")
                        tabla[(fila_idx, col_idx)].set_text_props(weight="bold", color="black")
                    else:
                        tabla[(fila_idx, col_idx)].set_facecolor("white")
                        tabla[(fila_idx, col_idx)].set_text_props(color="black")
            except (ValueError, IndexError):
                continue

    def crear_imagen_resumen_general(self, resumen, fecha_objetivo):
        fig = plt.figure(figsize=(14, 8), facecolor="white", dpi=300)
        ax = fig.add_axes([0, 0, 1, 1])
        ax.set_facecolor("white")

        data = []
        colores_fila = []

        for _, row in resumen.iterrows():
            avance = float(row["AVANCE"])
            meta = float(row["META"]) if pd.notna(row["META"]) else 0
            alcance = (avance / meta) if meta else 0

            data.append([
                row["CANAL_VENTA"],
                f"S/ {avance:,.0f}",
                f"S/ {meta:,.0f}",
                f"{alcance:.0%}",
            ])

            if row["CANAL_VENTA"] == "TOTAL":
                colores_fila.append("#bdc3c7")
            else:
                colores_fila.append("white")

        tabla = ax.table(
            cellText=data,
            colLabels=[
                "Canal de Venta",
                "Avance",
                "Meta",
                "Alcance %",
            ],
            cellLoc="center",
            loc="center",
        )

        tabla.auto_set_font_size(False)
        tabla.set_fontsize(9)
        tabla.scale(1.2, 1.8)
        tabla.auto_set_column_width(col=list(range(4)))

        cell_dict = tabla.get_celld()
        for i in range(len(data) + 1):
            cell_dict[(i, 0)].set_width(0.35)
            for j in range(1, 4):
                cell_dict[(i, j)].set_width(0.16)

        for i, color in enumerate(colores_fila):
            for j in range(4):
                if i == len(colores_fila) - 1:
                    tabla[(i + 1, j)].set_facecolor("#bdc3c7")
                    tabla[(i + 1, j)].set_text_props(weight="bold")
                else:
                    tabla[(i + 1, j)].set_facecolor(color)
                    tabla[(i + 1, j)].set_text_props(weight="normal")

        self._aplicar_formato_condicional_alcance(tabla, data, 3, len(data), len(data) - 1)

        for j in range(4):
            tabla[(0, j)].set_facecolor("#3498db")
            tabla[(0, j)].set_text_props(weight="bold", color="white")

        ax.axis("off")
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)

        fecha_txt = fecha_objetivo.strftime("%d-%m-%Y")
        nombre_archivo = f"01_resumen_general_meta_{fecha_txt}.png"
        ruta_completa = os.path.join(self.ruta_imagenes, nombre_archivo)

        plt.savefig(
            ruta_completa,
            dpi=300,
            bbox_inches="tight",
            facecolor="white",
            pad_inches=0,
            edgecolor="none",
            transparent=False,
            format="png",
        )
        plt.close()

        self.recortar_franjas_blancas(ruta_completa)

        return ruta_completa

    def _normalizar_nombre_archivo(self, texto):
        reemplazos = {
            "Á": "A",
            "É": "E",
            "Í": "I",
            "Ó": "O",
            "Ú": "U",
            "Ü": "U",
            "Ñ": "N",
        }
        for origen, destino in reemplazos.items():
            texto = texto.replace(origen, destino)
        return texto.replace(" ", "_").replace("/", "_").lower()

    def crear_imagen_canal_simple(self, df_canal, canal):
        fig = plt.figure(figsize=(12, 6), facecolor="white", dpi=300)
        ax = fig.add_axes([0, 0, 1, 1])
        ax.set_facecolor("white")

        if df_canal.empty:
            return None

        if canal == "ALO CÁLIDDA":
            columna_grupo = "ASESOR DE VENTAS"
        else:
            columna_grupo = "SEDE"

        tabla_base = df_canal.groupby(columna_grupo).agg(
            Importe=("IMPORTE_NUM", "sum"),
            Transacciones=("codigo_unico", "nunique"),
        )
        tabla_base = tabla_base.reset_index()

        data = []
        for _, row in tabla_base.iterrows():
            data.append([
                row[columna_grupo],
                f"S/ {row['Importe']:,.0f}",
                f"{row['Transacciones']:,.0f}",
            ])

        total_importe = tabla_base["Importe"].sum()
        total_trans = tabla_base["Transacciones"].sum()
        data.append([
            f"TOTAL {canal}",
            f"S/ {total_importe:,.0f}",
            f"{total_trans:,.0f}",
        ])

        tabla = ax.table(
            cellText=data,
            colLabels=[columna_grupo, "Importe", "Transacciones"],
            cellLoc="center",
            loc="center",
        )

        tabla.auto_set_font_size(False)
        tabla.set_fontsize(8)
        tabla.scale(1.2, 1.6)

        cell_dict = tabla.get_celld()
        for i in range(len(data) + 1):
            cell_dict[(i, 0)].set_width(0.45)
            cell_dict[(i, 1)].set_width(0.2)
            cell_dict[(i, 2)].set_width(0.2)

        num_filas = len(data)
        for i in range(num_filas):
            for j in range(3):
                if i == num_filas - 1:
                    tabla[(i + 1, j)].set_facecolor("#bdc3c7")
                    tabla[(i + 1, j)].set_text_props(weight="bold")
                else:
                    tabla[(i + 1, j)].set_facecolor("white")
                    tabla[(i + 1, j)].set_text_props(weight="normal")

        for j in range(3):
            tabla[(0, j)].set_facecolor("#3498db")
            tabla[(0, j)].set_text_props(weight="bold", color="white")

        ax.axis("off")
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)

        canal_archivo = self._normalizar_nombre_archivo(canal)
        nombre_archivo = f"{canal_archivo}.png"
        ruta_completa = os.path.join(self.ruta_imagenes, nombre_archivo)

        plt.savefig(
            ruta_completa,
            dpi=300,
            bbox_inches="tight",
            facecolor="white",
            pad_inches=0,
            edgecolor="none",
            transparent=False,
            format="png",
        )
        plt.close()

        self.recortar_franjas_blancas(ruta_completa)

        return ruta_completa

    def crear_imagen_canal_resumen(self, df_canal, canal):
        fig = plt.figure(figsize=(12, 6), facecolor="white", dpi=300)
        ax = fig.add_axes([0, 0, 1, 1])
        ax.set_facecolor("white")

        if df_canal.empty:
            return None

        tabla_base = df_canal.groupby("ALIADO COMERCIAL").agg(
            Importe=("IMPORTE_NUM", "sum"),
            Transacciones=("codigo_unico", "nunique"),
        )
        tabla_base = tabla_base.reset_index()

        data = []
        for _, row in tabla_base.iterrows():
            data.append([
                row["ALIADO COMERCIAL"],
                f"S/ {row['Importe']:,.0f}",
                f"{row['Transacciones']:,.0f}",
            ])

        total_importe = tabla_base["Importe"].sum()
        total_trans = tabla_base["Transacciones"].sum()
        data.append([
            f"TOTAL {canal}",
            f"S/ {total_importe:,.0f}",
            f"{total_trans:,.0f}",
        ])

        tabla = ax.table(
            cellText=data,
            colLabels=["Aliado Comercial", "Importe", "Transacciones"],
            cellLoc="center",
            loc="center",
        )

        tabla.auto_set_font_size(False)
        tabla.set_fontsize(7)
        tabla.scale(1.1, 1.4)

        cell_dict = tabla.get_celld()
        for i in range(len(data) + 1):
            cell_dict[(i, 0)].set_width(0.4)
            cell_dict[(i, 1)].set_width(0.2)
            cell_dict[(i, 2)].set_width(0.2)

        num_filas = len(data)
        for i in range(num_filas):
            for j in range(3):
                if i == num_filas - 1:
                    tabla[(i + 1, j)].set_facecolor("#bdc3c7")
                    tabla[(i + 1, j)].set_text_props(weight="bold")
                else:
                    tabla[(i + 1, j)].set_facecolor("white")
                    tabla[(i + 1, j)].set_text_props(weight="normal")

        for j in range(3):
            tabla[(0, j)].set_facecolor("#3498db")
            tabla[(0, j)].set_text_props(weight="bold", color="white")

        ax.axis("off")
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)

        canal_archivo = self._normalizar_nombre_archivo(canal)
        nombre_archivo = f"{canal_archivo}.png"
        ruta_completa = os.path.join(self.ruta_imagenes, nombre_archivo)

        plt.savefig(
            ruta_completa,
            dpi=300,
            bbox_inches="tight",
            facecolor="white",
            pad_inches=0,
            edgecolor="none",
            transparent=False,
            format="png",
        )
        plt.close()

        self.recortar_franjas_blancas(ruta_completa)

        return ruta_completa

    def crear_imagen_canal(self, df_canal, canal):
        fig = plt.figure(figsize=(12, 6), facecolor="white", dpi=300)
        ax = fig.add_axes([0, 0, 1, 1])
        ax.set_facecolor("white")

        if df_canal.empty:
            return None

        tabla_base = df_canal.groupby("ALIADO COMERCIAL").agg(
            Importe=("IMPORTE_NUM", "sum"),
            Transacciones=("codigo_unico", "nunique"),
        )
        tabla_base = tabla_base.reset_index()

        data = []
        for _, row in tabla_base.iterrows():
            data.append([
                row["ALIADO COMERCIAL"],
                f"S/ {row['Importe']:,.0f}",
                f"{row['Transacciones']:,.0f}",
            ])

        tabla = ax.table(
            cellText=data,
            colLabels=["Aliado Comercial", "Importe", "Transacciones"],
            cellLoc="center",
            loc="center",
        )

        tabla.auto_set_font_size(False)
        tabla.set_fontsize(8)
        tabla.scale(1.2, 1.6)

        cell_dict = tabla.get_celld()
        for i in range(len(data) + 1):
            cell_dict[(i, 0)].set_width(0.4)
            cell_dict[(i, 1)].set_width(0.2)
            cell_dict[(i, 2)].set_width(0.2)

        for i in range(len(data)):
            for j in range(3):
                tabla[(i + 1, j)].set_facecolor("white")
                tabla[(i + 1, j)].set_text_props(weight="normal")

        for j in range(3):
            tabla[(0, j)].set_facecolor("#3498db")
            tabla[(0, j)].set_text_props(weight="bold", color="white")

        ax.axis("off")
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)

        canal_archivo = self._normalizar_nombre_archivo(canal)
        nombre_archivo = f"{canal_archivo}.png"
        ruta_completa = os.path.join(self.ruta_imagenes, nombre_archivo)

        plt.savefig(
            ruta_completa,
            dpi=300,
            bbox_inches="tight",
            facecolor="white",
            pad_inches=0,
            edgecolor="none",
            transparent=False,
            format="png",
        )
        plt.close()

        self.recortar_franjas_blancas(ruta_completa)

        return ruta_completa

    def generar_imagenes(self, df_final, fecha_objetivo, resumen):
        imagenes = []

        print("Generando imagen de resumen general...")
        ruta_resumen = self.crear_imagen_resumen_general(resumen, fecha_objetivo)
        if ruta_resumen:
            imagenes.append(ruta_resumen)
            print(f"   OK: {os.path.basename(ruta_resumen)}")

        canales = sorted(df_final["CANAL_VENTA"].dropna().unique().tolist())
        canales_simple = {"ALO CÁLIDDA", "CSC", "DIGITAL", "TIENDAS CÁLIDDA"}
        for canal in canales:
            df_canal = df_final[df_final["CANAL_VENTA"] == canal].copy()
            print(f"Generando imagen para canal: {canal}")
            if canal in canales_simple:
                ruta_canal = self.crear_imagen_canal_simple(df_canal, canal)
            else:
                ruta_canal = self.crear_imagen_canal_resumen(df_canal, canal)
            if ruta_canal:
                imagenes.append(ruta_canal)
                print(f"   OK: {os.path.basename(ruta_canal)}")
            else:
                print(f"   Sin datos para canal: {canal}")

        return imagenes

    async def enviar_reporte_whatsapp(self, imagenes_generadas, fecha_objetivo):
        print("\n=== ENVIANDO REPORTE POR WHATSAPP ===")
        print(f"Total de imágenes a enviar: {len(imagenes_generadas)}")

        numeros_destino = [
            '51976650091', #Stefany
            '51940193512' #Chema
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

        print("🔍 Verificando que WhatsApp Web esté listo...")
        await asyncio.sleep(3)

        try:
            await whatsapp.page.wait_for_selector("[data-testid='chat-list'], canvas", timeout=5000)
            print("✅ WhatsApp Web listo para enviar mensajes\n")
        except Exception:
            print("⚠️ No se pudo verificar el estado de WhatsApp Web")
            print("   Intentando continuar de todas formas...\n")

        try:
            hora_actual = datetime.now().time()
            if hora_actual < dt_time(12, 0):
                saludo = "Buenos días, se brinda el avance de ventas con meta:" 
            else:
                saludo = "Buenas tardes, se brinda el avance de ventas con meta:" 

            estructura_envio = [
                (saludo, None),
                ("Resumen General", "01_resumen_general_meta"),
                ("Canal Aló Cálidda", "alo_calidda"),
                ("Canal CSC", "csc"),
                ("Canal Digital", "digital"),
                ("Canal Tiendas Cálidda", "tiendas_calidda"),
                ("Canal Retail", "retail"),
                ("Canal Motos", "motos"),
                ("Canal Materiales", "materiales_y_acabados_de_construccion"),
                ("Canal GGSS", "grandes_superficies"),
                ("Canal Proveedor", "canal_proveedor"),
                ("Canal FFVV PaP", "ffvv_-_puerta_a_puerta"),
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
                            print(f"   [{i:2d}/{len(estructura_envio)}] {mensaje}")
                            exito = await whatsapp.enviar_mensaje(mensaje)
                            if exito:
                                envios_exitosos += 1
                            else:
                                envios_fallidos += 1
                        else:
                            if patron_imagen in imagenes_disponibles:
                                ruta_imagen = imagenes_disponibles[patron_imagen]
                                print(f"   [{i:2d}/{len(estructura_envio)}] {mensaje}")

                                exito_msg = await whatsapp.enviar_mensaje(mensaje)
                                if not exito_msg:
                                    envios_fallidos += 1
                                    continue

                                exito_img = await whatsapp.enviar_imagen(ruta_imagen)
                                if exito_img:
                                    envios_exitosos += 1
                                else:
                                    envios_fallidos += 1
                            else:
                                print(f"   [{i:2d}/{len(estructura_envio)}] ⚠️ Imagen no disponible: {mensaje}")

                        await asyncio.sleep(2)
                    except Exception as e:
                        print(f"      ❌ Error en envío #{i}: {e}")
                        envios_fallidos += 1
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

    def ejecutar(self):
        ruta_archivo = self.seleccionar_archivo_excel()
        if not ruta_archivo:
            print("No se selecciono ningun archivo. Proceso cancelado.")
            return

        print(f"Cargando archivo: {os.path.basename(ruta_archivo)}")
        df = pd.read_excel(ruta_archivo, engine="openpyxl")

        if self.col_importe not in df.columns:
            raise ValueError(f"No se encontro la columna requerida: {self.col_importe}")
        if "FECHA VENTA" not in df.columns:
            raise ValueError("No se encontro la columna requerida: FECHA VENTA")

        df["FECHA VENTA"] = pd.to_datetime(df["FECHA VENTA"], errors="coerce", dayfirst=True)

        estados_validos = ["PENDIENTE DE ENTREGA", "ENTREGADO", "PENDIENTE DE APROBACIÓN"]
        if "ESTADO" in df.columns:
            registros_antes = len(df)
            df = df[df["ESTADO"].isin(estados_validos)].copy()
            registros_despues = len(df)
            print(f"Filtro ESTADO aplicado: {registros_antes} -> {registros_despues} registros")
        else:
            print("Columna ESTADO no encontrada, continuando sin filtro")

        df_final = self.preparar_transacciones(df)

        mapper = CanalMapper(self.ruta_canal_fija)
        df_final["CANAL_VENTA"] = mapper.determinar_canal_venta(df_final)
        df_final["IMPORTE_NUM"] = self.limpiar_importe(df_final[self.col_importe])
        df_final["FECHA VENTA"] = pd.to_datetime(df_final["FECHA VENTA"], errors="coerce", dayfirst=True)

        fecha_objetivo = df_final["FECHA VENTA"].dropna().dt.date.min()
        if pd.isna(fecha_objetivo):
            raise ValueError("No se pudo determinar la fecha de venta")

        metas_long = self.cargar_metas_diarias()
        metas_fecha = metas_long[metas_long["FECHA_META"] == fecha_objetivo].copy()
        if metas_fecha.empty:
            fechas_disponibles = metas_long["FECHA_META"].dropna().unique().tolist()
            fechas_disponibles = sorted(fechas_disponibles)[:10]
            raise ValueError(
                f"No se encontraron metas para la fecha {fecha_objetivo}. "
                f"Fechas disponibles (ejemplos): {fechas_disponibles}"
            )

        resumen_avance = (
            df_final.groupby("CANAL_VENTA")["IMPORTE_NUM"]
            .sum()
            .reset_index()
            .rename(columns={"IMPORTE_NUM": "AVANCE"})
        )

        resumen_avance["CANAL_KEY"] = resumen_avance["CANAL_VENTA"].astype(str).str.strip().str.upper()
        metas_fecha["CANAL_KEY"] = metas_fecha["CANAL_META"].astype(str).str.strip().str.upper()
        metas_fecha_no_total = metas_fecha[metas_fecha["CANAL_KEY"] != "TOTAL"].copy()

        resumen = pd.merge(
            resumen_avance,
            metas_fecha_no_total[["CANAL_KEY", "META"]],
            on="CANAL_KEY",
            how="left",
        )

        meta_total = metas_fecha_no_total["META"].sum()
        avance_total = resumen_avance["AVANCE"].sum()
        resumen_total = pd.DataFrame([
            {"CANAL_VENTA": "TOTAL", "AVANCE": avance_total, "META": meta_total}
        ])
        resumen = pd.concat([resumen, resumen_total], ignore_index=True)

        faltan_metas = resumen["META"].isna() & (resumen["CANAL_VENTA"] != "TOTAL")
        if faltan_metas.any():
            canales_faltantes = resumen.loc[faltan_metas, "CANAL_VENTA"].unique().tolist()
            print(f"Advertencia: faltan metas para {len(canales_faltantes)} canal(es): {canales_faltantes}")
            resumen["META"] = resumen["META"].fillna(0)

        imagenes_generadas = self.generar_imagenes(df_final, fecha_objetivo, resumen)

        print(f"\n{'=' * 60}")
        print("Iniciando envío de reporte por WhatsApp...")
        print("Timeout máximo: 10 minutos")
        print(f"{'=' * 60}")

        try:
            asyncio.run(
                asyncio.wait_for(
                    self.enviar_reporte_whatsapp(imagenes_generadas, fecha_objetivo),
                    timeout=600,
                )
            )
        except asyncio.TimeoutError:
            print("\n⏱️ TIMEOUT GLOBAL: El proceso completo excedió los 10 minutos")
            print("   El reporte puede estar incompleto")
        except Exception as e:
            print(f"\n❌ Error inesperado: {e}")


def main():
    generator = SalesImageGenerator()
    generator.ejecutar()


if __name__ == "__main__":
    main()

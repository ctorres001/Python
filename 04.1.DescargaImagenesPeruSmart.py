import os
import pandas as pd
import requests
import re
from urllib.parse import urlparse
from PIL import Image
import io

# Ruta del archivo Excel
archivo_excel = r"D:\FNB\Productos\Productos\Descargar Peru Smart\Base.xlsx"
ruta_guardado = os.path.dirname(archivo_excel)

# Leer el archivo Excel
df = pd.read_excel(archivo_excel)

# Validar columnas necesarias
if 'IMAGENES' not in df.columns or 'CODIGO SKU' not in df.columns:
    raise ValueError("El archivo debe tener las columnas 'IMAGENES' y 'CODIGO SKU'.")


# Función para extraer la primera URL válida (jpg, png o URLs dinámicas)
def extraer_primer_url(texto):
    if pd.isna(texto):
        return None

    # Buscar URLs que terminen en .jpg o .png
    urls_estaticas = re.findall(r'https?://[^\s]+?\.(?:jpg|png)', texto, flags=re.IGNORECASE)

    # Buscar URLs dinámicas (como las de Vtex, con parámetros de imagen)
    urls_dinamicas = re.findall(
        r'https?://[^\s]+?(?:vtexassets\.com|vteximg\.com\.br)[^\s]*?(?:arquivos/ids/|unsafe/)[^\s]*', texto,
        flags=re.IGNORECASE)

    # Buscar otros patrones comunes de URLs de imágenes dinámicas
    urls_con_parametros = re.findall(r'https?://[^\s]+?[?&](?:width|height|w|h|size)=[^\s]*', texto,
                                     flags=re.IGNORECASE)

    # Priorizar URLs estáticas, luego dinámicas
    if urls_estaticas:
        return urls_estaticas[0]
    elif urls_dinamicas:
        return urls_dinamicas[0]
    elif urls_con_parametros:
        return urls_con_parametros[0]

    return None


# Función para determinar extensión de archivo
def obtener_extension(url, content_type=None):
    # Intentar obtener extensión de la URL
    parsed_url = urlparse(url)
    path = parsed_url.path

    # Buscar extensión en la URL
    if '.' in path:
        ext = os.path.splitext(path)[-1].lower()
        if ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']:
            return ext

    # Si no hay extensión en la URL, usar Content-Type
    if content_type:
        if 'jpeg' in content_type or 'jpg' in content_type:
            return '.jpg'
        elif 'png' in content_type:
            return '.png'
        elif 'gif' in content_type:
            return '.gif'
        elif 'webp' in content_type:
            return '.webp'

    # Por defecto, usar .jpg
    return '.jpg'


# Función para convertir PNG a JPG
def convertir_png_a_jpg(imagen_bytes):
    try:
        # Abrir imagen desde bytes
        imagen = Image.open(io.BytesIO(imagen_bytes))

        # Si es PNG, convertir a JPG
        if imagen.format == 'PNG':
            # Crear fondo blanco para manejar transparencia
            fondo_blanco = Image.new('RGB', imagen.size, (255, 255, 255))
            if imagen.mode == 'RGBA':
                fondo_blanco.paste(imagen, mask=imagen.split()[3])  # Usar canal alpha
            else:
                fondo_blanco.paste(imagen)

            # Guardar como JPG en memoria
            buffer = io.BytesIO()
            fondo_blanco.save(buffer, format='JPEG', quality=95)
            return buffer.getvalue(), '.jpg'
        else:
            # Si no es PNG, retornar imagen original
            return imagen_bytes, obtener_extension('', f'image/{imagen.format.lower()}')
    except Exception as e:
        print(f"⚠️ Error al convertir imagen: {e}")
        return imagen_bytes, '.jpg'


# Procesar cada fila
errores_404 = []
exitosos = 0
total_procesados = 0

for index, row in df.iterrows():
    codigo_sku = str(row['CODIGO SKU']).strip()
    url = extraer_primer_url(str(row['IMAGENES']))

    if not url or not codigo_sku:
        continue  # Saltar si no hay URL o código válido

    total_procesados += 1

    try:
        # Agregar headers para evitar bloqueos
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        print(f"🔍 Procesando SKU: {codigo_sku}")
        print(f"📍 URL: {url}")

        response = requests.get(url, timeout=10, headers=headers)
        if response.status_code == 200:
            # Obtener Content-Type del header
            content_type = response.headers.get('Content-Type', '')

            # Procesar imagen (convertir PNG a JPG si es necesario)
            imagen_procesada, extension_final = convertir_png_a_jpg(response.content)

            nombre_archivo = f"{codigo_sku}{extension_final}"
            ruta_completa = os.path.join(ruta_guardado, nombre_archivo)

            with open(ruta_completa, 'wb') as f:
                f.write(imagen_procesada)
            print(f"✅ Imagen guardada: {ruta_completa}")
            exitosos += 1
        elif response.status_code == 404:
            error_info = f"SKU: {codigo_sku} - URL: {url}"
            errores_404.append(error_info)
            print(f"⚠️ Error 404 - Imagen no encontrada para SKU: {codigo_sku}")
        else:
            print(f"⚠️ No se pudo descargar la imagen (status {response.status_code}) para SKU: {codigo_sku}")
            print(f"📍 URL problemática: {url}")
    except Exception as e:
        print(f"❌ Error al descargar imagen para SKU {codigo_sku}: {e}")
        print(f"📍 URL problemática: {url}")

print(f"\n🎯 Resumen del proceso:")
print(f"✅ Imágenes descargadas exitosamente: {exitosos}")
print(f"📊 Total procesadas: {total_procesados}")

if errores_404:
    print(f"\n⚠️ SKUs con errores 404 ({len(errores_404)} total):")
    for error in errores_404:
        print(f"   - {error}")

    # Guardar errores 404 en archivo
    with open(os.path.join(ruta_guardado, "errores_404.txt"), "w", encoding="utf-8") as f:
        f.write("SKUs con errores 404:\n")
        for error in errores_404:
            f.write(f"{error}\n")
    print(f"📝 Lista de errores 404 guardada en: {os.path.join(ruta_guardado, 'errores_404.txt')}")

print("Proceso completado.")
import os
from PIL import Image

# Ubicación de los archivos
directorio = r"D:\FNB\Productos\Productos\Descargar Peru Smart"

# Recorrer todos los archivos en el directorio
for archivo in os.listdir(directorio):
    # Verificar si es un archivo .png
    if archivo.lower().endswith('.png'):
        # Obtener rutas completas
        ruta_completa = os.path.join(directorio, archivo)

        try:
            # Abrir la imagen
            with Image.open(ruta_completa) as img:
                # Convertir a RGB si es necesario (para PNG con transparencia)
                if img.mode in ('RGBA', 'P'):
                    img = img.convert('RGB')

                # Crear nuevo nombre con extensión .jpg
                nombre_sin_ext = os.path.splitext(archivo)[0]
                nueva_ruta = os.path.join(directorio, nombre_sin_ext + '.jpg')

                # Guardar como JPG con calidad máxima (ajustable)
                img.save(nueva_ruta, 'JPEG', quality=95)

                print(f"Convertido: {archivo} -> {nombre_sin_ext}.jpg")

        except Exception as e:
            print(f"Error procesando {archivo}: {str(e)}")

print("Proceso completado")
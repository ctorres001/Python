import pandas as pd
import tkinter as tk
from tkinter import filedialog

# --- Función para seleccionar el archivo Excel ---
def seleccionar_archivo():
    root = tk.Tk()
    root.withdraw()  # Ocultar la ventana principal de Tkinter
    archivo = filedialog.askopenfilename(title="Seleccionar archivo Excel", filetypes=[("Archivos Excel", "*.xlsx;*.xlsm")])
    return archivo

# --- Función para seleccionar dónde guardar el archivo TXT ---
def seleccionar_ruta_guardado():
    root = tk.Tk()
    root.withdraw()
    ruta_guardado = filedialog.asksaveasfilename(title="Seleccionar dónde guardar el archivo", defaultextension=".txt", filetypes=[("Archivos TXT", "*.txt")])
    return ruta_guardado

# --- Seleccionar archivo Excel ---
archivo_excel = seleccionar_archivo()
if not archivo_excel:
    print("No se seleccionó ningún archivo.")
    exit()

# --- Cargar hojas disponibles ---
xls = pd.ExcelFile(archivo_excel)
print("\nHojas disponibles en el archivo:")
for hoja in xls.sheet_names:
    print("-", hoja)

# --- Solicitar al usuario qué hojas combinar ---
hojas_a_unir = input("\nEscribe los nombres de las hojas que quieres unir, separados por comas: ").split(",")

# --- Seleccionar ruta de guardado ---
ruta_guardado = seleccionar_ruta_guardado()
if not ruta_guardado:
    print("No se seleccionó una ruta de guardado.")
    exit()

# --- Unir datos de las hojas seleccionadas ---
with open(ruta_guardado, "w", encoding="utf-8") as txt_file:
    for hoja in hojas_a_unir:
        hoja = hoja.strip()  # Limpiar espacios en los nombres
        if hoja in xls.sheet_names:
            df = pd.read_excel(archivo_excel, sheet_name=hoja)
            df.to_csv(txt_file, sep="\t", index=False, header=True, mode="a")  # Guardar datos con tabulación
            txt_file.write("\n")  # Espacio entre hojas
        else:
            print(f"La hoja '{hoja}' no existe en el archivo.")

print(f"\nLas hojas seleccionadas han sido guardadas en: {ruta_guardado}")

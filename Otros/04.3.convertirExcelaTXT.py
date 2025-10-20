import pandas as pd
import tkinter as tk
from tkinter import filedialog

def seleccionar_archivo():
    """Abre un cuadro de diálogo para seleccionar un archivo Excel."""
    root = tk.Tk()
    root.withdraw()  # Ocultar ventana
    archivo = filedialog.askopenfilename(title="Seleccionar archivo Excel", filetypes=[("Excel files", "*.xlsx *.xls")])
    return archivo

def listar_hojas(archivo):
    """Lista las hojas disponibles en el archivo Excel."""
    xl = pd.ExcelFile(archivo)
    return xl.sheet_names

def procesar_excel(archivo, hoja, output_txt):
    """Lee un archivo Excel, reemplaza '¬' por '-', y guarda en formato TXT con delimitador '¬'."""
    df = pd.read_excel(archivo, sheet_name=hoja, engine="openpyxl")

    # Convertir todo a texto y reemplazar caracteres conflictivos
    df = df.astype(str).map(lambda x: x.replace("¬", "-"))

    # Convertir a formato delimitado
    df_txt = df.apply(lambda row: "¬".join(row), axis=1)

    # Guardar en TXT
    with open(output_txt, 'w', encoding='utf-8') as f:
        f.write("\n".join(df_txt) + "\n")

if __name__ == "__main__":
    archivo_excel = seleccionar_archivo()
    hojas = listar_hojas(archivo_excel)

    print("\n📜 Hojas disponibles en el archivo:")
    for i, hoja in enumerate(hojas):
        print(f"{i + 1}. {hoja}")

    while True:
        try:
            indice = int(input("\nIngrese el número de la hoja a procesar: ")) - 1
            if 0 <= indice < len(hojas):
                break
            else:
                print("❌ Número fuera de rango. Inténtalo de nuevo.")
        except ValueError:
            print("❌ Entrada inválida. Debe ser un número.")

    hoja_seleccionada = hojas[indice]
    output_txt = archivo_excel.replace(".xlsx", ".txt").replace(".xls", ".txt")

    procesar_excel(archivo_excel, hoja_seleccionada, output_txt)
    print(f"\n✅ Archivo TXT generado exitosamente: {output_txt}")

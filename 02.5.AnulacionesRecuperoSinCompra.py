import pandas as pd
import os
from datetime import datetime
import locale
import win32com.client as win32
from xlsxwriter import Workbook
from datetime import timedelta

# === CONFIGURACIONES ===
locale.setlocale(locale.LC_TIME, 'es_PE.utf8' if os.name != 'nt' else 'Spanish_Peru')
ruta_base = r"D:\FNB\Reportes\19. Reportes IBR"
ruta_procesado = os.path.join(ruta_base, r"00. Estructura Reporte\Procesado\Archivo_Procesado.xlsx")
ruta_destinatarios = os.path.join(ruta_base, r"03. Base anulaciones para recupero\Destinatarios\Listado de correos recupero.xlsx")
ruta_salida = os.path.join(ruta_base, r"03. Base anulaciones para recupero\Reportes")
firma_path = os.path.join(ruta_base, r"01. Pendientes de Entrega\Firma\Firma_resized.jpg")

columnas_exportar = [
    'RESPONSABLE DE VENTA', 'SEDE', 'ALIADO COMERCIAL', 'CUENTA CONTRATO',
    'CLIENTE', 'DNI', 'TELÉFONO', 'Nro. PEDIDO VENTA', 'Nro. DE CONTRATO', 'IMPORTE (S./)',
    'CRÉDITO UTILIZADO', 'Nro. DE CUOTAS', 'FECHA VENTA', 'HORA VENTA',
    'FECHA ENTREGA', 'TIPO DE VALIDACION RENIEC', 'TIPO DESPACHO', 'ESTADO',
    'BOLETA', 'CANAL_VENTA', 'MOTIVO ANULACIÓN'
]

os.makedirs(ruta_salida, exist_ok=True)

# === CARGAR Y LIMPIAR DATOS ===
df = pd.read_excel(ruta_procesado, dtype=str)
df = df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x) if col.dtype == 'object' else col)

# === PROCESAMIENTO MEJORADO DE FECHA Y HORA ===
# Convertir FECHA VENTA a datetime
df['FECHA VENTA'] = pd.to_datetime(df['FECHA VENTA'], errors='coerce')

# Procesar HORA VENTA de manera más robusta
def procesar_hora(hora_str):
    if pd.isna(hora_str) or hora_str == '':
        return '00:00:00'
    
    hora_str = str(hora_str).strip()
    
    # Si ya tiene formato HH:MM:SS, mantenerlo
    if len(hora_str.split(':')) == 3:
        return hora_str
    # Si tiene formato HH:MM, agregar segundos
    elif len(hora_str.split(':')) == 2:
        return f"{hora_str}:00"
    # Si es solo un número (como 1430 para 14:30), convertir
    elif hora_str.isdigit() and len(hora_str) in [3, 4]:
        if len(hora_str) == 3:
            hora_str = '0' + hora_str
        return f"{hora_str[:2]}:{hora_str[2:]}:00"
    else:
        return '00:00:00'

df['HORA_PROCESADA'] = df['HORA VENTA'].apply(procesar_hora)

# Crear columna DATETIME que combine fecha y hora
def crear_datetime(row):
    if pd.isna(row['FECHA VENTA']):
        return pd.NaT
    
    try:
        fecha_str = row['FECHA VENTA'].strftime('%Y-%m-%d')
        hora_str = row['HORA_PROCESADA']
        datetime_str = f"{fecha_str} {hora_str}"
        return pd.to_datetime(datetime_str)
    except:
        return pd.NaT

df['FECHA_HORA_VENTA'] = df.apply(crear_datetime, axis=1)

# === CONSULTA: FILTRAR A PARTIR DE CIERTO MES/AÑO ===
while True:
    fecha_input = input("📅 Ingrese el mes y año desde el cual desea incluir anulaciones (formato MM/YYYY, ej: 04/2025): ")
    try:
        fecha_inicio = datetime.strptime(fecha_input, "%m/%Y")
        break
    except ValueError:
        print("❌ Formato incorrecto. Use MM/YYYY (ej: 04/2025)")

df = df[df['FECHA VENTA'] >= fecha_inicio]
print(f"✅ Se filtraron las anulaciones a partir de {fecha_inicio.strftime('%B %Y')}.")

# Guardamos copia original para validación cruzada
df_original = df.copy()

# === FILTRO ANULACIONES ===
filtro = (
    (df['ESTADO'] == 'ANULADO') &
    (df['CANAL_VENTA'].isin(['DIGITAL', 'ALO CÁLIDDA', 'CANAL PROVEEDOR', 'CSC', 'FFVV - PUERTA A PUERTA'])) &
    (df['ALIADO COMERCIAL'].str.upper() != 'CARDIF') &
    (df['MOTIVO ANULACIÓN'].str.upper() != 'PRUEBAS')
)
df_anuladas = df[filtro].copy()

# === VALIDACIÓN MEJORADA: ¿TIENE COMPRAS POSTERIORES? ===
def tiene_compra_posterior(fila):
    cuenta = fila['CUENTA CONTRATO']
    fecha_hora_anulacion = fila['FECHA_HORA_VENTA']
    
    # Si no se pudo procesar la fecha/hora de la anulación, usar solo fecha
    if pd.isna(fecha_hora_anulacion):
        fecha_anulacion = fila['FECHA VENTA']
        posteriores = df_original[
            (df_original['CUENTA CONTRATO'] == cuenta) &
            (pd.to_datetime(df_original['FECHA VENTA'], errors='coerce') > fecha_anulacion) &
            (df_original['ESTADO'].isin(['ENTREGADO', 'PENDIENTE DE ENTREGA']))
        ]
    else:
        # Buscar compras posteriores usando fecha y hora completa
        posteriores = df_original[
            (df_original['CUENTA CONTRATO'] == cuenta) &
            (df_original['FECHA_HORA_VENTA'] > fecha_hora_anulacion) &
            (df_original['ESTADO'].isin(['ENTREGADO', 'PENDIENTE DE ENTREGA']))
        ]
    
    return not posteriores.empty

# Aplicar la validación mejorada
print("🔍 Analizando compras posteriores (considerando fecha y hora)...")
df_anuladas['TIENE_COMPRA_POSTERIOR'] = df_anuladas.apply(tiene_compra_posterior, axis=1)

# Mostrar estadísticas para debugging
total_anuladas = len(df_anuladas)
con_compra_posterior = df_anuladas['TIENE_COMPRA_POSTERIOR'].sum()
sin_compra_posterior = total_anuladas - con_compra_posterior

print(f"📊 Estadísticas de análisis:")
print(f"   • Total de anulaciones encontradas: {total_anuladas}")
print(f"   • Anulaciones CON compra posterior: {con_compra_posterior}")
print(f"   • Anulaciones SIN compra posterior: {sin_compra_posterior}")

# Solo conservar las anuladas SIN compras posteriores
df_anuladas = df_anuladas[~df_anuladas['TIENE_COMPRA_POSTERIOR']].copy()

# === VALIDACIÓN SI HAY DATOS ===
if df_anuladas.empty:
    print("⚠️ No se encontraron anulaciones sin compras posteriores desde la fecha indicada.")
    exit()

# === DEBUGGING: MOSTRAR CASOS DEL MISMO DÍA ===
print("\n🔍 Verificando casos del mismo día...")
for idx, fila in df_anuladas.iterrows():
    cuenta = fila['CUENTA CONTRATO']
    fecha_anulacion = fila['FECHA VENTA']
    
    # Buscar otras transacciones de la misma cuenta el mismo día
    mismo_dia = df_original[
        (df_original['CUENTA CONTRATO'] == cuenta) &
        (df_original['FECHA VENTA'] == fecha_anulacion) &
        (df_original.index != idx)
    ]
    
    if not mismo_dia.empty:
        print(f"   📅 Cuenta {cuenta}: {len(mismo_dia)} transacciones adicionales el {fecha_anulacion.strftime('%d/%m/%Y')}")
        for _, transaccion in mismo_dia.iterrows():
            print(f"      • {transaccion['ESTADO']} a las {transaccion['HORA VENTA']} vs Anulación a las {fila['HORA VENTA']}")

# === FECHAS PARA NOMBRAR ARCHIVOS Y TEXTO ===
fecha_min = df_anuladas['FECHA VENTA'].min().strftime('%d/%m/%Y')
fecha_ayer = (datetime.now() - timedelta(days=1)).strftime('%d/%m/%Y')  # Día anterior
fecha_nombre = (datetime.now() - timedelta(days=1)).strftime('%d-%m-%Y')  # Para nombre archivo

# === EXPORTAR A EXCEL ===
def exportar_excel(df_filtrado, ruta_archivo):
    with pd.ExcelWriter(ruta_archivo, engine='xlsxwriter') as writer:
        df_filtrado.to_excel(writer, index=False, sheet_name='Anulaciones')
        workbook = writer.book
        worksheet = writer.sheets['Anulaciones']

        header_format = workbook.add_format({
            'bold': True, 'font_name': 'Aptos', 'font_size': 8,
            'bg_color': '#000000', 'font_color': '#FFFFFF',
            'align': 'center', 'valign': 'vcenter'
        })
        cell_format = workbook.add_format({
            'font_name': 'Aptos', 'font_size': 8,
            'align': 'left', 'valign': 'vcenter'
        })
        number_format = workbook.add_format({
            'font_name': 'Aptos', 'font_size': 8,
            'align': 'right', 'valign': 'vcenter',
            'num_format': '#,##0.00'
        })

        worksheet.set_default_row(11.25)
        for col_num, value in enumerate(df_filtrado.columns):
            worksheet.write(0, col_num, value, header_format)
            max_len = max(df_filtrado[value].astype(str).map(len).max(), len(value))
            worksheet.set_column(col_num, col_num, min(max_len + 2, 50))

        for row in range(1, len(df_filtrado) + 1):
            for col in range(len(df_filtrado.columns)):
                value = df_filtrado.iloc[row - 1, col]
                col_name = df_filtrado.columns[col]
                if pd.isna(value):
                    worksheet.write(row, col, "", cell_format)
                elif col_name in ['IMPORTE (S./)', 'CRÉDITO UTILIZADO']:
                    try:
                        num_value = float(str(value).replace(',', ''))
                    except:
                        num_value = 0.0
                    worksheet.write(row, col, num_value, number_format)
                else:
                    worksheet.write(row, col, value, cell_format)

# Preparar datos para exportar
df_exportar = df_anuladas[columnas_exportar].copy()
df_exportar['HORA VENTA'] = pd.to_datetime(df_exportar['HORA VENTA'], errors='coerce').dt.strftime('%H:%M')
df_exportar['FECHA VENTA'] = df_exportar['FECHA VENTA'].dt.strftime('%d/%m/%Y')

# Guardar archivo
archivo_salida = f"Ventas anuladas sin compra posterior al {fecha_nombre}.xlsx"
ruta_archivo = os.path.join(ruta_salida, archivo_salida)
exportar_excel(df_exportar, ruta_archivo)

print(f"📁 Archivo guardado: {archivo_salida}")

# === RESUMEN PARA CORREO ===
resumen_html = f"<b>Se detectaron {len(df_exportar)} anulaciones sin compras posteriores.</b><br><br>"

df_anuladas['MES'] = df_anuladas['FECHA VENTA'].dt.month
df_anuladas['MES_AÑO'] = df_anuladas['FECHA VENTA'].dt.strftime('%B %Y').str.capitalize()
df_anuladas['MOTIVO TITULO'] = df_anuladas['MOTIVO ANULACIÓN'].str.title()
for mes in df_anuladas.sort_values('MES', ascending=False)['MES_AÑO'].unique():
    df_mes = df_anuladas[df_anuladas['MES_AÑO'] == mes]
    resumen_html += f"<b>Mes {mes}:</b> {len(df_mes)} anulaciones detectadas<br><ul>"
    for motivo, count in df_mes['MOTIVO TITULO'].value_counts().items():
        resumen_html += f"<li>{motivo}: {count}</li>"
    resumen_html += "</ul>"

# === ENVIAR CORREO ===
correos = pd.read_excel(ruta_destinatarios)
correos = correos.dropna(subset=['Destinatarios directos'])

asunto = f"Reporte de ventas anuladas sin compras posteriores - {fecha_nombre}"
cuerpo = f"""<html><body style='font-family:Aptos, sans-serif; font-size:11pt;'>
Buenos días:<br><br>
Linda, se comparte el reporte de ventas anuladas sin compras posteriores. Corresponde al periodo del <b>{fecha_min}</b> al <b>{fecha_ayer}</b>.<br><br>
{resumen_html}
Quedo atento a cualquier observación.<br><br>

Atentamente,<br><br>

<img src="cid:firmaimg">
</body></html>"""

outlook = win32.Dispatch("Outlook.Application")
for idx, row in correos.iterrows():
    mail = outlook.CreateItem(0)
    mail.To = row['Destinatarios directos']
    if pd.notna(row.get('Destinatarios en copia')):
        mail.CC = row['Destinatarios en copia']
    mail.Subject = asunto
    mail.Attachments.Add(ruta_archivo)
    attach = mail.Attachments.Add(firma_path)
    attach.PropertyAccessor.SetProperty("http://schemas.microsoft.com/mapi/proptag/0x3712001F", "firmaimg")
    mail.HTMLBody = cuerpo
    mail.Display()
    print(f"📧 Correo enviado a: {row['Destinatarios directos']}")

print("🎉 Proceso de envío completado.")
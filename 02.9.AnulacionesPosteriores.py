import pandas as pd
import os
from datetime import datetime
import locale
import win32com.client as win32
from xlsxwriter import Workbook
import calendar

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
    'BOLETA', 'CANAL_VENTA', 'MOTIVO ANULACIÓN', 'RECUPERACION_STATUS', 
    'CANAL_RECUPERACION', 'FECHA_PRIMERA_COMPRA_POSTERIOR', 'TOTAL_COMPRAS_POSTERIORES'
]

os.makedirs(ruta_salida, exist_ok=True)

# === CARGAR Y LIMPIAR DATOS ===
print("🔄 Cargando datos...")
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

# === CONSULTA: MES ESPECÍFICO A ANALIZAR ===
while True:
    fecha_input = input("📅 Ingrese el mes y año a analizar (formato MM/YYYY, ej: 04/2025): ")
    try:
        fecha_analisis = datetime.strptime(fecha_input, "%m/%Y")
        break
    except ValueError:
        print("❌ Formato incorrecto. Use MM/YYYY (ej: 04/2025)")

# Filtrar solo el mes específico para anulaciones
mes_inicio = fecha_analisis.replace(day=1)
if fecha_analisis.month == 12:
    mes_fin = fecha_analisis.replace(year=fecha_analisis.year + 1, month=1, day=1)
else:
    mes_fin = fecha_analisis.replace(month=fecha_analisis.month + 1, day=1)

print(f"✅ Analizando anulaciones del mes de {fecha_analisis.strftime('%B %Y')}...")

# Guardar copia original para análisis completo
df_original = df.copy()

# === FILTRO ANULACIONES DEL MES ESPECÍFICO ===
filtro_anulaciones_mes = (
    (df['ESTADO'] == 'ANULADO') &
    (df['FECHA VENTA'] >= mes_inicio) &
    (df['FECHA VENTA'] < mes_fin) &
    (df['CANAL_VENTA'].isin(['DIGITAL', 'ALO CÁLIDDA', 'CANAL PROVEEDOR', 'CSC', 'FFVV - PUERTA A PUERTA'])) &
    (df['ALIADO COMERCIAL'].str.upper() != 'CARDIF') &
    (df['MOTIVO ANULACIÓN'].str.upper() != 'PRUEBAS')
)

df_anuladas_mes = df[filtro_anulaciones_mes].copy()

if df_anuladas_mes.empty:
    print(f"⚠️ No se encontraron anulaciones en {fecha_analisis.strftime('%B %Y')} que cumplan los criterios.")
    exit()

print(f"📊 Se encontraron {len(df_anuladas_mes)} anulaciones en {fecha_analisis.strftime('%B %Y')}")

# === ANÁLISIS MEJORADO DE COMPRAS POSTERIORES POR CANAL ===
def analizar_compra_posterior(fila):
    """
    Analiza si una cuenta anulada tuvo compras posteriores y en qué canales
    Considera fecha y hora para mayor precisión
    Retorna: (status, canales_info, fecha_primera, total_compras)
    """
    cuenta = fila['CUENTA CONTRATO']
    fecha_anulacion = fila['FECHA VENTA']
    fecha_hora_anulacion = fila['FECHA_HORA_VENTA']
    canal_original = fila['CANAL_VENTA']
    
    # Buscar compras posteriores exitosas considerando fecha y hora
    if pd.isna(fecha_hora_anulacion):
        # Si no se pudo procesar la fecha/hora de la anulación, usar solo fecha
        compras_posteriores = df_original[
            (df_original['CUENTA CONTRATO'] == cuenta) &
            (pd.to_datetime(df_original['FECHA VENTA'], errors='coerce') > fecha_anulacion) &
            (df_original['ESTADO'].isin(['ENTREGADO', 'PENDIENTE DE ENTREGA']))
        ].copy()
    else:
        # Buscar compras posteriores usando fecha y hora completa
        compras_posteriores = df_original[
            (df_original['CUENTA CONTRATO'] == cuenta) &
            (df_original['FECHA_HORA_VENTA'] > fecha_hora_anulacion) &
            (df_original['ESTADO'].isin(['ENTREGADO', 'PENDIENTE DE ENTREGA']))
        ].copy()
    
    if compras_posteriores.empty:
        return 'SIN_RECUPERACION', 'N/A', None, 0
    
    # Analizar canales de las compras posteriores
    canales_posteriores = compras_posteriores['CANAL_VENTA'].unique()
    fecha_primera_compra = compras_posteriores['FECHA VENTA'].min()
    total_compras = len(compras_posteriores)
    
    # Determinar el tipo de recuperación
    if len(canales_posteriores) == 1:
        canal_posterior = canales_posteriores[0]
        if canal_posterior == canal_original:
            status = 'MISMO_CANAL'
            canales_info = canal_posterior
        else:
            status = 'CANAL_DIFERENTE'
            canales_info = f"{canal_original} → {canal_posterior}"
    else:
        status = 'DIVERSOS_CANALES'
        canales_info = f"{canal_original} → {', '.join(canales_posteriores)}"
    
    return status, canales_info, fecha_primera_compra, total_compras

print("🔍 Analizando compras posteriores por canal (considerando fecha y hora)...")

# Aplicar análisis a cada anulación
resultados_analisis = []
for idx, fila in df_anuladas_mes.iterrows():
    status, canales_info, fecha_primera, total_compras = analizar_compra_posterior(fila)
    resultados_analisis.append({
        'RECUPERACION_STATUS': status,
        'CANAL_RECUPERACION': canales_info,
        'FECHA_PRIMERA_COMPRA_POSTERIOR': fecha_primera.strftime('%d/%m/%Y') if fecha_primera else 'N/A',
        'TOTAL_COMPRAS_POSTERIORES': total_compras
    })

# Agregar resultados al DataFrame
df_resultado = pd.DataFrame(resultados_analisis)
df_anuladas_mes = pd.concat([df_anuladas_mes.reset_index(drop=True), df_resultado], axis=1)

# === DEBUGGING: MOSTRAR CASOS DEL MISMO DÍA ===
print("\n🔍 Verificando casos del mismo día...")
casos_mismo_dia = 0
for idx, fila in df_anuladas_mes.iterrows():
    cuenta = fila['CUENTA CONTRATO']
    fecha_anulacion = fila['FECHA VENTA']
    
    # Buscar otras transacciones de la misma cuenta el mismo día
    mismo_dia = df_original[
        (df_original['CUENTA CONTRATO'] == cuenta) &
        (df_original['FECHA VENTA'] == fecha_anulacion) &
        (df_original.index != idx)
    ]
    
    if not mismo_dia.empty:
        casos_mismo_dia += 1
        print(f"   📅 Cuenta {cuenta}: {len(mismo_dia)} transacciones adicionales el {fecha_anulacion.strftime('%d/%m/%Y')}")
        for _, transaccion in mismo_dia.iterrows():
            print(f"      • {transaccion['ESTADO']} a las {transaccion.get('HORA VENTA', 'N/A')} vs Anulación a las {fila.get('HORA VENTA', 'N/A')}")

if casos_mismo_dia == 0:
    print("   ✅ No se encontraron casos con múltiples transacciones el mismo día")

# === GENERAR ESTADÍSTICAS PARA RESUMEN ===
total_anulaciones = len(df_anuladas_mes)
sin_recuperacion = len(df_anuladas_mes[df_anuladas_mes['RECUPERACION_STATUS'] == 'SIN_RECUPERACION'])
con_recuperacion = total_anulaciones - sin_recuperacion
mismo_canal = len(df_anuladas_mes[df_anuladas_mes['RECUPERACION_STATUS'] == 'MISMO_CANAL'])
canal_diferente = len(df_anuladas_mes[df_anuladas_mes['RECUPERACION_STATUS'] == 'CANAL_DIFERENTE'])
diversos_canales = len(df_anuladas_mes[df_anuladas_mes['RECUPERACION_STATUS'] == 'DIVERSOS_CANALES'])

print(f"\n📈 Estadísticas del análisis (con procesamiento mejorado fecha/hora):")
print(f"   • Total anulaciones: {total_anulaciones}")
print(f"   • Sin recuperación: {sin_recuperacion}")
print(f"   • Con recuperación: {con_recuperacion}")
print(f"   • Mismo canal: {mismo_canal}")
print(f"   • Canal diferente: {canal_diferente}")
print(f"   • Diversos canales: {diversos_canales}")
print(f"   • Precisión mejorada: Análisis considera fecha Y hora de transacciones")

# === EXPORTAR A EXCEL ===
def exportar_excel_completo(df_filtrado, ruta_archivo):
    with pd.ExcelWriter(ruta_archivo, engine='xlsxwriter') as writer:
        # Hoja principal con todos los datos
        df_filtrado.to_excel(writer, index=False, sheet_name='Anulaciones_Detalle')
        
        # Hoja de resumen estadístico
        resumen_data = {
            'Categoría': [
                'Total Anulaciones',
                'Sin Recuperación',
                'Con Recuperación',
                '- Mismo Canal',
                '- Canal Diferente', 
                '- Diversos Canales'
            ],
            'Cantidad': [
                total_anulaciones,
                sin_recuperacion,
                con_recuperacion,
                mismo_canal,
                canal_diferente,
                diversos_canales
            ],
            'Porcentaje': [
                '100.0%',
                f'{(sin_recuperacion/total_anulaciones*100):.1f}%',
                f'{(con_recuperacion/total_anulaciones*100):.1f}%',
                f'{(mismo_canal/total_anulaciones*100):.1f}%',
                f'{(canal_diferente/total_anulaciones*100):.1f}%',
                f'{(diversos_canales/total_anulaciones*100):.1f}%'
            ]
        }
        
        df_resumen = pd.DataFrame(resumen_data)
        df_resumen.to_excel(writer, index=False, sheet_name='Resumen_Estadistico')
        
        workbook = writer.book
        
        # Formatos
        header_format = workbook.add_format({
            'bold': True, 'font_name': 'Aptos', 'font_size': 10,
            'bg_color': '#000000', 'font_color': '#FFFFFF',
            'align': 'center', 'valign': 'vcenter'
        })
        cell_format = workbook.add_format({
            'font_name': 'Aptos', 'font_size': 9,
            'align': 'left', 'valign': 'vcenter'
        })
        number_format = workbook.add_format({
            'font_name': 'Aptos', 'font_size': 9,
            'align': 'right', 'valign': 'vcenter',
            'num_format': '#,##0.00'
        })
        resumen_format = workbook.add_format({
            'font_name': 'Aptos', 'font_size': 10,
            'align': 'center', 'valign': 'vcenter'
        })
        
        # Formatear hoja de detalle
        worksheet_detalle = writer.sheets['Anulaciones_Detalle']
        worksheet_detalle.set_default_row(12)
        
        for col_num, value in enumerate(df_filtrado.columns):
            worksheet_detalle.write(0, col_num, value, header_format)
            max_len = max(df_filtrado[value].astype(str).map(len).max(), len(value))
            worksheet_detalle.set_column(col_num, col_num, min(max_len + 2, 50))
        
        for row in range(1, len(df_filtrado) + 1):
            for col in range(len(df_filtrado.columns)):
                value = df_filtrado.iloc[row - 1, col]
                col_name = df_filtrado.columns[col]
                if pd.isna(value):
                    worksheet_detalle.write(row, col, "", cell_format)
                elif col_name in ['IMPORTE (S./)', 'CRÉDITO UTILIZADO']:
                    try:
                        num_value = float(str(value).replace(',', ''))
                    except:
                        num_value = 0.0
                    worksheet_detalle.write(row, col, num_value, number_format)
                else:
                    worksheet_detalle.write(row, col, value, cell_format)
        
        # Formatear hoja de resumen
        worksheet_resumen = writer.sheets['Resumen_Estadistico']
        worksheet_resumen.set_default_row(15)
        
        for col_num, value in enumerate(df_resumen.columns):
            worksheet_resumen.write(0, col_num, value, header_format)
            worksheet_resumen.set_column(col_num, col_num, 25)
        
        for row in range(1, len(df_resumen) + 1):
            for col in range(len(df_resumen.columns)):
                value = df_resumen.iloc[row - 1, col]
                worksheet_resumen.write(row, col, value, resumen_format)

# Preparar datos para exportación
df_exportar = df_anuladas_mes[columnas_exportar].copy()

# Formateo mejorado de hora considerando el procesamiento previo
df_exportar['HORA VENTA'] = df_anuladas_mes['HORA_PROCESADA'].apply(
    lambda x: x[:5] if len(x) >= 5 else x  # Mostrar solo HH:MM
)
df_exportar['FECHA VENTA'] = df_exportar['FECHA VENTA'].dt.strftime('%d/%m/%Y')

# Generar nombre del archivo
mes_nombre = calendar.month_name[fecha_analisis.month].capitalize()
año = fecha_analisis.year
archivo_salida = f"Análisis Anulaciones por Canal - {mes_nombre} {año}.xlsx"
ruta_archivo = os.path.join(ruta_salida, archivo_salida)

print("💾 Generando archivo Excel...")
exportar_excel_completo(df_exportar, ruta_archivo)

# === GENERAR RESUMEN HTML PARA CORREO ===
mes_texto = fecha_analisis.strftime('%B %Y').capitalize()

# Análisis por motivos de anulación
motivos_stats = df_anuladas_mes.groupby(['MOTIVO ANULACIÓN', 'RECUPERACION_STATUS']).size().unstack(fill_value=0)

resumen_html = f"""
<div style='font-family: Aptos, sans-serif; font-size: 11pt;'>
<h3>📊 ANÁLISIS DE ANULACIONES - {mes_texto.upper()}</h3>

<table border='1' style='border-collapse: collapse; margin: 10px 0;'>
<tr style='background-color: #2E75B6; color: white; font-weight: bold;'>
    <td style='padding: 8px;'>CATEGORÍA</td>
    <td style='padding: 8px; text-align: center;'>CANTIDAD</td>
    <td style='padding: 8px; text-align: center;'>PORCENTAJE</td>
</tr>
<tr>
    <td style='padding: 5px; font-weight: bold;'>Total Anulaciones</td>
    <td style='padding: 5px; text-align: center;'>{total_anulaciones}</td>
    <td style='padding: 5px; text-align: center;'>100.0%</td>
</tr>
<tr style='background-color: #FFE6E6;'>
    <td style='padding: 5px;'>• Sin Recuperación</td>
    <td style='padding: 5px; text-align: center;'>{sin_recuperacion}</td>
    <td style='padding: 5px; text-align: center;'>{(sin_recuperacion/total_anulaciones*100):.1f}%</td>
</tr>
<tr style='background-color: #E6F3FF;'>
    <td style='padding: 5px; font-weight: bold;'>• Con Recuperación</td>
    <td style='padding: 5px; text-align: center;'>{con_recuperacion}</td>
    <td style='padding: 5px; text-align: center;'>{(con_recuperacion/total_anulaciones*100):.1f}%</td>
</tr>
<tr>
    <td style='padding: 5px; padding-left: 20px;'>- Mismo Canal</td>
    <td style='padding: 5px; text-align: center;'>{mismo_canal}</td>
    <td style='padding: 5px; text-align: center;'>{(mismo_canal/total_anulaciones*100):.1f}%</td>
</tr>
<tr>
    <td style='padding: 5px; padding-left: 20px;'>- Canal Diferente</td>
    <td style='padding: 5px; text-align: center;'>{canal_diferente}</td>
    <td style='padding: 5px; text-align: center;'>{(canal_diferente/total_anulaciones*100):.1f}%</td>
</tr>
<tr>
    <td style='padding: 5px; padding-left: 20px;'>- Diversos Canales</td>
    <td style='padding: 5px; text-align: center;'>{diversos_canales}</td>
    <td style='padding: 5px; text-align: center;'>{(diversos_canales/total_anulaciones*100):.1f}%</td>
</tr>
</table>

<br>
<h4>🔍 INSIGHTS CLAVE:</h4>
<ul>
<li><b>Tasa de Recuperación:</b> {(con_recuperacion/total_anulaciones*100):.1f}% de los clientes anulados realizaron compras posteriores</li>
<li><b>Fidelidad al Canal:</b> {(mismo_canal/con_recuperacion*100 if con_recuperacion > 0 else 0):.1f}% de las recuperaciones fueron en el mismo canal</li>
<li><b>Migración de Canal:</b> {((canal_diferente + diversos_canales)/con_recuperacion*100 if con_recuperacion > 0 else 0):.1f}% cambiaron de canal para su recuperación</li>
<li><b>Precisión de Análisis:</b> Considera fecha y hora exacta de transacciones para mayor exactitud</li>
</ul>
</div>
"""

# === ENVIAR CORREO ===
print("📧 Preparando envío de correos...")

correos = pd.read_excel(ruta_destinatarios)
correos = correos.dropna(subset=['Destinatarios directos'])

asunto = f"Análisis de Anulaciones por Canal de Recuperación - {mes_texto}"
cuerpo = f"""<html><body style='font-family:Aptos, sans-serif; font-size:11pt;'>
Buenos días:<br><br>
Se comparte el análisis detallado de anulaciones y su recuperación por canales correspondiente al mes de <b>{mes_texto}</b>.<br><br>
Este reporte utiliza <b>procesamiento mejorado de fecha y hora</b> para mayor precisión en la identificación de compras posteriores, 
permitiendo distinguir transacciones realizadas el mismo día según su horario específico.<br><br>

{resumen_html}

<br>
El archivo adjunto contiene el detalle completo de cada anulación y su estado de recuperación, así como un resumen estadístico en hoja separada.<br><br>

<i><b>Mejora técnica:</b> El análisis ahora considera la hora exacta de las transacciones, no solo la fecha, proporcionando mayor exactitud en la clasificación de compras posteriores.</i><br><br>

Quedo atento a cualquier consulta o análisis adicional que requieran.<br><br>

Atentamente,<br><br>

<img src="cid:firmaimg">
</body></html>"""

try:
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
    
    print("🎉 Proceso completado exitosamente.")
    print(f"📄 Archivo generado: {archivo_salida}")
    print(f"📊 Total anulaciones analizadas: {total_anulaciones}")
    print(f"📈 Tasa de recuperación: {(con_recuperacion/total_anulaciones*100):.1f}%")
    print(f"🕐 Precisión mejorada: Análisis considera fecha y hora exacta")
    
except Exception as e:
    print(f"❌ Error en el envío de correos: {str(e)}")
    print(f"📄 Archivo Excel generado correctamente en: {ruta_archivo}")
    print(f"🕐 Con procesamiento mejorado de fecha y hora")
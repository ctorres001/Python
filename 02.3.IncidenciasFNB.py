import pandas as pd
import os
from datetime import datetime
import win32com.client as win32
from xlsxwriter import Workbook

ruta_base = r"D:\FNB\Reportes\19. Reportes IBR"
ruta_procesado = os.path.join(ruta_base, r"00. Estructura Reporte\Procesado\Archivo_Procesado.xlsx")
ruta_destinatarios = os.path.join(ruta_base,
                                  r"04. Reporte incidencias\Destinatarios\Listado de correos incidencias.xlsx")
ruta_salida = os.path.join(ruta_base, r"04. Reporte incidencias\Incidencias")
firma_path = os.path.join(ruta_base, r"01. Pendientes de Entrega\Firma\Firma_resized.jpg")

columnas_exportar = [
    'RESPONSABLE DE VENTA', 'SEDE', 'ALIADO COMERCIAL', 'CUENTA CONTRATO',
    'CLIENTE', 'DNI', 'Nro. PEDIDO VENTA', 'Nro. PEDIDO CARDIF',
    'ESTADO', 'ESTADO PEDIDO CARDIF',
    'Nro. DE CONTRATO', 'IMPORTE (S./)', 'CRÉDITO UTILIZADO', 'Nro. DE CUOTAS',
    'FECHA VENTA', 'HORA VENTA', 'FECHA ENTREGA', 'TIPO DE VALIDACION RENIEC',
    'TIPO DESPACHO', 'BOLETA', 'CANAL_VENTA', 'VALIDACIÓN MOTORIZADO'
]

# Estados de validación fallida CORREGIDOS con mayúsculas y minúsculas exactas
ESTADOS_VALIDACION_FALLIDA = [
    'Validación fallida – No ingreso al link',
    'Validación fallida - tiempo expirado',
    'Validación fallida - error técnico de celular',
    'Validación fallida - sin conexión'
]

# También incluir posibles variaciones en mayúsculas para mayor robustez
ESTADOS_VALIDACION_FALLIDA_VARIACIONES = [
    'Validación fallida – No ingreso al link',
    'Validación fallida - tiempo expirado',
    'Validación fallida - error técnico de celular',
    'Validación fallida - sin conexión',
    'VALIDACIÓN FALLIDA – NO INGRESO AL LINK',
    'VALIDACIÓN FALLIDA - TIEMPO EXPIRADO',
    'VALIDACIÓN FALLIDA - ERROR TÉCNICO DE CELULAR',
    'VALIDACIÓN FALLIDA - SIN CONEXIÓN'
]

# Debug: Mostrar estados configurados
print("🔧 Estados de validación fallida configurados:")
for estado in ESTADOS_VALIDACION_FALLIDA:
    print(f"   - '{estado}'")

os.makedirs(ruta_salida, exist_ok=True)

df = pd.read_excel(ruta_procesado, dtype=str)
df = df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x) if col.dtype == 'object' else col)

# Debug: Análisis de estados presentes en los datos
print("📊 Análisis de estados en los datos:")
if 'ESTADO' in df.columns:
    estados_unicos = df['ESTADO'].value_counts().head(30)  # Aumentar para ver más estados
    print("Estados más frecuentes:")
    for estado, count in estados_unicos.items():
        print(f"   - '{estado}': {count}")

    # Verificar si los nuevos estados están presentes (búsqueda más flexible)
    print("\n🔍 Verificación de nuevos estados de validación fallida:")

    # Buscar por coincidencia parcial para detectar variaciones
    estados_validacion_encontrados = []
    for estado in df['ESTADO'].unique():
        if pd.notna(estado) and 'validación fallida' in str(estado).lower():
            estados_validacion_encontrados.append(estado)

    if estados_validacion_encontrados:
        print("   Estados de validación fallida encontrados en los datos:")
        for estado in estados_validacion_encontrados:
            count = df[df['ESTADO'] == estado].shape[0]
            print(f"      - '{estado}': {count} registros")
    else:
        print("   ⚠️  No se encontraron estados de validación fallida en los datos")

    # Verificar coincidencias exactas con nuestros estados configurados
    print("\n🎯 Verificación exacta de estados configurados:")
    for estado in ESTADOS_VALIDACION_FALLIDA:
        count = df[df['ESTADO'] == estado].shape[0]
        print(f"   - '{estado}': {count} registros")

    # Verificar si aún existe INVALIDO en los datos
    invalido_count = df[df['ESTADO'] == 'INVALIDO'].shape[0]
    if invalido_count > 0:
        print(f"\n⚠️  ATENCIÓN: Aún existen {invalido_count} registros con estado 'INVALIDO'")
else:
    print("⚠️  Columna 'ESTADO' no encontrada en los datos")

df['FECHA VENTA'] = pd.to_datetime(df['FECHA VENTA'], errors='coerce')
df['FECHA ENTREGA'] = pd.to_datetime(df['FECHA ENTREGA'], errors='coerce')
fecha_min = df['FECHA VENTA'].min().strftime('%d/%m/%Y') if not df[
    'FECHA VENTA'].isna().all() else datetime.now().strftime('%d/%m/%Y')
fecha_max = df['FECHA VENTA'].max().strftime('%d/%m/%Y') if not df[
    'FECHA VENTA'].isna().all() else datetime.now().strftime('%d/%m/%Y')
fecha_nombre = df['FECHA VENTA'].max().strftime('%d-%m-%Y') if not df[
    'FECHA VENTA'].isna().all() else datetime.now().strftime('%d-%m-%Y')


def formatear_df(df_filtrado):
    df_filtrado = df_filtrado.copy()
    for col in ['FECHA VENTA', 'FECHA ENTREGA']:
        if col in df_filtrado.columns:
            df_filtrado[col] = pd.to_datetime(df_filtrado[col], errors='coerce').dt.strftime('%d/%m/%Y').fillna("")
    if 'HORA VENTA' in df_filtrado.columns:
        df_filtrado['HORA VENTA'] = pd.to_datetime(df_filtrado['HORA VENTA'], format='%H:%M',
                                                   errors='coerce').dt.strftime('%H:%M').fillna("")
    columnas_presentes = [col for col in columnas_exportar if col in df_filtrado.columns]
    return df_filtrado[columnas_presentes]


def exportar_excel(df_filtrado, ruta):
    with pd.ExcelWriter(ruta, engine='xlsxwriter') as writer:
        df_filtrado.to_excel(writer, index=False, sheet_name='Incidencias')
        workbook: Workbook = writer.book
        worksheet = writer.sheets['Incidencias']
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
                if pd.isna(value) or (isinstance(value, float) and (pd.isna(value) or pd.isinf(value))):
                    worksheet.write(row, col, "", cell_format)
                elif col_name in ['IMPORTE (S./)', 'CRÉDITO UTILIZADO']:
                    try:
                        num_value = float(str(value).replace(',', ''))
                    except:
                        num_value = 0.0
                    worksheet.write(row, col, num_value, number_format)
                else:
                    worksheet.write(row, col, str(value), cell_format)


def detectar_diferencias_estado(df):
    """Devuelve un DataFrame con las diferencias de estado detectadas"""
    df = df.copy()
    df = df[df['Nro. PEDIDO CARDIF'].notna() & df['Nro. PEDIDO VENTA'].notna()]
    df_estado_ref = df[['Nro. PEDIDO VENTA', 'ESTADO']].dropna().drop_duplicates()
    df_estado_ref = df_estado_ref.rename(columns={
        'Nro. PEDIDO VENTA': 'PEDIDO_REFERENCIA',
        'ESTADO': 'ESTADO PEDIDO CARDIF'
    })
    df = df.merge(df_estado_ref, left_on='Nro. PEDIDO CARDIF', right_on='PEDIDO_REFERENCIA', how='left')
    df = df[df['ESTADO PEDIDO CARDIF'].notna()]
    df_diferencias = df[df['ESTADO'] != df['ESTADO PEDIDO CARDIF']].copy()
    return df_diferencias


def filtro_ventas_rechazadas_sin_venta_posterior(df):
    """
    ESCENARIO 7: Ventas con validación fallida con sustento de entrega y sin venta posterior igual

    FILTROS:
    - ESTADO debe ser uno de: Validación fallida (todos los tipos), Rechazado por biometría, Error de integración
    - Nro. PEDIDO VENTA debe estar vacío
    - ESTADO DE ARCHIVOS(SUSTENTO DE VENTA) debe ser "SI"
    - ESTADO DE ARCHIVOS(SUSTENTO DE ENTREGA) debe ser "SI"
    - ALIADO COMERCIAL no debe ser "CARDIF"
    - Verificación de que no exista venta posterior exitosa con mismas características
    - NUEVO: Exoneración por archivo Exonerados.xlsx basado en Nro. DE CONTRATO
    """

    # Crear copia del DataFrame para no modificar el original
    df = df.copy()

    # NUEVO: Cargar archivo de exonerados
    ruta_exonerados = os.path.join(ruta_base, r"04. Reporte incidencias\Exonerados\Exonerados.xlsx")
    contratos_exonerados = set()

    try:
        if os.path.exists(ruta_exonerados):
            df_exonerados = pd.read_excel(ruta_exonerados)
            if 'Nro. DE CONTRATO' in df_exonerados.columns:
                # Obtener lista de contratos exonerados, limpiando espacios y valores nulos
                contratos_exonerados = set(
                    str(contrato).strip()
                    for contrato in df_exonerados['Nro. DE CONTRATO'].dropna()
                    if str(contrato).strip() != '' and str(contrato).strip() != 'nan'
                )
                print(f"📋 Contratos exonerados cargados: {len(contratos_exonerados)}")
                if contratos_exonerados:
                    print(f"   Primeros 5 contratos: {list(contratos_exonerados)[:5]}")
            else:
                print("⚠️  Columna 'Nro. DE CONTRATO' no encontrada en archivo de exonerados")
        else:
            print(f"⚠️  Archivo de exonerados no encontrado: {ruta_exonerados}")
    except Exception as e:
        print(f"❌ Error cargando archivo de exonerados: {str(e)}")

    # Convertir fechas y horas a datetime para comparaciones precisas
    df['FECHA VENTA'] = pd.to_datetime(df['FECHA VENTA'], errors='coerce')

    # Crear datetime completo combinando fecha y hora para comparación precisa
    df['FECHA_HORA_VENTA'] = df['FECHA VENTA'].copy()

    # Si existe columna HORA VENTA, combinar fecha y hora
    if 'HORA VENTA' in df.columns:
        # Convertir hora a string si no lo es
        df['HORA VENTA'] = df['HORA VENTA'].astype(str)

        # Para filas que tienen tanto fecha como hora válidas
        mask_hora_valida = df['FECHA VENTA'].notna() & df['HORA VENTA'].notna() & (df['HORA VENTA'] != 'nan') & (
                df['HORA VENTA'] != '')

        # Combinar fecha y hora
        df.loc[mask_hora_valida, 'FECHA_HORA_VENTA'] = pd.to_datetime(
            df.loc[mask_hora_valida, 'FECHA VENTA'].dt.strftime('%Y-%m-%d') + ' ' +
            df.loc[mask_hora_valida, 'HORA VENTA'],
            errors='coerce'
        )

    # FILTROS INICIALES ACTUALIZADOS - Usar los estados corregidos
    estados_rechazados = ESTADOS_VALIDACION_FALLIDA_VARIACIONES + ['RECHAZADO POR BIOMETRÍA', 'ERROR DE INTEGRACIÓN']

    df_validos = df[
        # Estado de la venta debe ser uno de estos (venta rechazada o validación fallida)
        df['ESTADO'].isin(estados_rechazados) &

        # NUEVO - Nro. PEDIDO VENTA debe estar vacío
        (df['Nro. PEDIDO VENTA'].isna() | (df['Nro. PEDIDO VENTA'] == '') | (df['Nro. PEDIDO VENTA'] == 'nan')) &

        # CORREGIDO - Debe tener sustento de VENTA = "SI" (no entrega)
        (df['ESTADO DE ARCHIVOS(SUSTENTO DE VENTA)'].str.upper() == 'SI') &

        # CORREGIDO - Debe tener sustento de VENTA = "SI" (no entrega)
        (df['ESTADO DE ARCHIVOS(SUSTENTO DE ENTREGA)'].str.upper() == 'SI') &

        # NUEVO - ALIADO COMERCIAL no debe ser CARDIF
        (df['ALIADO COMERCIAL'].str.upper() != 'CARDIF')
        ].copy()

    # NUEVO: Aplicar exoneración por Nro. DE CONTRATO
    if contratos_exonerados and 'Nro. DE CONTRATO' in df_validos.columns:
        # Crear una serie con los contratos como string para comparación
        contratos_df = df_validos['Nro. DE CONTRATO'].astype(str).str.strip()

        # Contar registros antes de la exoneración
        registros_antes = len(df_validos)

        # Filtrar excluyendo los contratos exonerados
        df_validos = df_validos[~contratos_df.isin(contratos_exonerados)]

        # Mostrar información de exoneración
        registros_exonerados = registros_antes - len(df_validos)
        if registros_exonerados > 0:
            print(f"🚫 Registros exonerados: {registros_exonerados}")
            print(f"📊 Registros restantes después de exoneración: {len(df_validos)}")
        else:
            print("✅ No se encontraron registros para exonerar en este escenario")

    if df_validos.empty:
        return df_validos

    print(f"🔍 Ventas con validación fallida y sustento encontradas: {len(df_validos)}")
    print(f"📋 Estados considerados: {', '.join(estados_rechazados)}")

    # Crear clave única para identificar ventas equivalentes
    df_validos['clave'] = df_validos[[
        'RESPONSABLE DE VENTA', 'SEDE', 'ALIADO COMERCIAL',
        'CUENTA CONTRATO', 'CLIENTE', 'IMPORTE (S./)', 'Nro. DE CUOTAS'
    ]].astype(str).agg('|'.join, axis=1)

    df_validos['id'] = df_validos.index

    # Preparar datos para búsqueda de ventas posteriores
    claves_fecha = df[[
        'RESPONSABLE DE VENTA', 'SEDE', 'ALIADO COMERCIAL',
        'CUENTA CONTRATO', 'CLIENTE', 'IMPORTE (S./)', 'Nro. DE CUOTAS',
        'FECHA_HORA_VENTA', 'ESTADO'
    ]].dropna(subset=['FECHA_HORA_VENTA'])

    claves_fecha['clave'] = claves_fecha[[
        'RESPONSABLE DE VENTA', 'SEDE', 'ALIADO COMERCIAL',
        'CUENTA CONTRATO', 'CLIENTE', 'IMPORTE (S./)', 'Nro. DE CUOTAS'
    ]].astype(str).agg('|'.join, axis=1)

    # Buscar ventas posteriores exitosas
    ids_excluir = set()

    for idx, row in df_validos.iterrows():
        clave = row['clave']
        fecha_hora = row['FECHA_HORA_VENTA']

        # Buscar ventas posteriores con mismas características
        posteriores = claves_fecha[
            (claves_fecha['clave'] == clave) &
            (claves_fecha['FECHA_HORA_VENTA'] > fecha_hora) &
            (claves_fecha['ESTADO'].isin(['ENTREGADO', 'PENDIENTE DE ENTREGA']))
            ]

        # También buscar ventas del mismo día (por si hay error en horas)
        ventas_mismo_dia = claves_fecha[
            (claves_fecha['clave'] == clave) &
            (claves_fecha['FECHA_HORA_VENTA'].dt.date == fecha_hora.date()) &
            (claves_fecha['FECHA_HORA_VENTA'] != fecha_hora) &
            (claves_fecha['ESTADO'].isin(['ENTREGADO', 'PENDIENTE DE ENTREGA']))
            ]

        if not posteriores.empty or not ventas_mismo_dia.empty:
            ids_excluir.add(row['id'])

    resultado = df_validos[~df_validos['id'].isin(ids_excluir)]

    # Limpiar columnas auxiliares
    columnas_limpiar = ['clave', 'id', 'FECHA_HORA_VENTA']
    for col in columnas_limpiar:
        if col in resultado.columns:
            resultado = resultado.drop(columns=[col])

    return resultado


# ACTUALIZACIÓN: Lista de escenarios con cambios solicitados
nombres_archivos = [
    ("Transacciones seguro con estado diferente al producto", "funcion_especial", detectar_diferencias_estado),
    ("Ventas sin numero de pedido", "filtro_booleano",
     lambda df: df['Nro. PEDIDO VENTA'].isna() & df['ESTADO'].isin(['ENTREGADO', 'PENDIENTE DE ENTREGA'])),
    ("Ventas en estado de Validación fallida, Rechazado por biometría o Error de integración con número de pedido",
     "filtro_booleano",
     lambda df: df['ESTADO'].isin(
         ESTADOS_VALIDACION_FALLIDA_VARIACIONES + ['RECHAZADO POR BIOMETRÍA', 'ERROR DE INTEGRACIÓN']) & df[
                    'Nro. PEDIDO VENTA'].notna()),
    ("Ventas Pendientes de validación biométrica", "filtro_booleano",
     lambda df: df['ESTADO'] == 'PENDIENTE DE VALIDACIÓN BIOMÉTRICA'),
    ("Ventas en estado Entregado pero sin fecha de entrega", "filtro_booleano",
     lambda df: (df['ESTADO'] == 'ENTREGADO') & df['FECHA ENTREGA'].isna()),
    ("Ventas Pendientes de Entrega con fecha de entrega registrada", "filtro_booleano",
     lambda df: (df['ESTADO'] == 'PENDIENTE DE ENTREGA') & df['FECHA ENTREGA'].notna()),
    ("Ventas con validación fallida con sustento de entrega y sin venta posterior igual", "funcion_especial",
     filtro_ventas_rechazadas_sin_venta_posterior),
    ("Ventas sin Estado detallado", "filtro_booleano",
     lambda df: df['ESTADO'].isna() | (df['ESTADO'] == '') | (df['ESTADO'] == 'nan'))
]

# ======= NUEVO: Grupos de escenarios para COMERCIAL y PROYECTOS (solo etiquetas) =======
ESCENARIOS_COMERCIAL = {
    "Ventas en estado de Validación fallida, Rechazado por biometría o Error de integración con número de pedido",
    "Ventas en estado Entregado pero sin fecha de entrega",
    "Ventas Pendientes de Entrega con fecha de entrega registrada",
    "Ventas con validación fallida con sustento de entrega y sin venta posterior igual"
}
ESCENARIOS_PROYECTOS = {
    "Transacciones seguro con estado diferente al producto",
    "Ventas sin numero de pedido",
    "Ventas Pendientes de validación biométrica",
    "Ventas sin Estado detallado"
}

archivos = []
# ======= MODIFICADO: Acumuladores separados por grupo para solo mostrar escenarios con casos =======
resumen_html_comercial = ""
resumen_html_proyectos = ""

# CORRECCIÓN: Manejo diferenciado según el tipo de filtro - SIN NUMERACIÓN
for nombre_base, tipo_filtro, filtro_fn in nombres_archivos:
    try:
        print(f"\n🔄 Procesando escenario: {nombre_base}")

        if tipo_filtro == "funcion_especial":
            # Para funciones que devuelven DataFrames directamente
            df_filtrado = filtro_fn(df).copy()
        else:
            # Para funciones que devuelven condiciones booleanas
            condicion = filtro_fn(df)
            df_filtrado = df[condicion].copy()

            # Debug especial para escenarios que usan los nuevos estados
            if any(estado in nombre_base.upper() for estado in ['VALIDACIÓN FALLIDA', 'INVALIDO']):
                print(f"   🔍 Registros que cumplen la condición: {len(df_filtrado)}")
                if len(df_filtrado) > 0 and 'ESTADO' in df_filtrado.columns:
                    estados_encontrados = df_filtrado['ESTADO'].value_counts()
                    print(f"   📋 Estados encontrados en este escenario:")
                    for estado, count in estados_encontrados.items():
                        print(f"      - '{estado}': {count}")

        # ======= MODIFICADO: Solo procesar si hay registros =======
        if not df_filtrado.empty:
            conteo = len(df_filtrado)
            
            nombre_archivo = f"{nombre_base} - {fecha_nombre}.xlsx"
            ruta_archivo = os.path.join(ruta_salida, nombre_archivo)
            
            df_filtrado = formatear_df(df_filtrado)
            exportar_excel(df_filtrado, ruta_archivo)
            archivos.append(ruta_archivo)
            
            # ======= MODIFICADO: Construcción del resumen SIN numeración =======
            resumen = f"<br><b>{nombre_base}</b>: <b>{conteo}</b> registros detectados<br><ul>"

            # Verificar si existe la columna CANAL_VENTA antes de usarla
            if 'CANAL_VENTA' in df_filtrado.columns:
                canal_counts = df_filtrado['CANAL_VENTA'].fillna('SIN CANAL').value_counts()
                for canal, count in canal_counts.items():
                    resumen += f"<li>{canal}: {count}</li>"
            else:
                resumen += f"<li>Información de canal no disponible</li>"
            resumen += "</ul>"

            # ======= MODIFICADO: Solo agregar al grupo correspondiente si hay casos =======
            if nombre_base in ESCENARIOS_COMERCIAL:
                resumen_html_comercial += resumen
            elif nombre_base in ESCENARIOS_PROYECTOS:
                resumen_html_proyectos += resumen
            else:
                # Si por algún motivo no calza, se agrega a COMERCIAL por defecto
                resumen_html_comercial += resumen

            print(f"✅ Procesado: {nombre_base} - {conteo} registros")
        else:
            print(f"⚪ Sin casos: {nombre_base} - 0 registros (no incluido en reporte)")

    except Exception as e:
        print(f"❌ Error procesando {nombre_base}: {str(e)}")
        # ======= MODIFICADO: No agregar errores al resumen para mantenerlo limpio =======

# ======= MODIFICADO: Unir los dos grupos con subtítulos solo si tienen contenido =======
resumen_html = ""
if resumen_html_comercial:
    resumen_html += "<br><b>COMERCIAL</b><br>" + resumen_html_comercial
if resumen_html_proyectos:
    resumen_html += "<br><b>PROYECTOS</b><br>" + resumen_html_proyectos

# Reemplazo solicitado: <p>, </p>, <ul>, </ul>, <li>, </li>
resumen_html = (resumen_html
                .replace("<p>", "")
                .replace("</p>", "<br>")
                .replace("<ul>", "")
                .replace("</ul>", "")
                .replace("<li>", "- ")
                .replace("</li>", "<br>")
               )

# Verificar si hay archivos para enviar
if not archivos:
    print("⚠️  No se generaron archivos. Verificar datos de entrada.")
    exit()

# Leer destinatarios y enviar correos
try:
    correos = pd.read_excel(ruta_destinatarios)
    correos = correos.dropna(subset=['Destinatarios directos'])

    asunto = f"Reporte de transacciones observadas de la plataforma FNB al {fecha_nombre}"
    cuerpo = f"""<html><body style=\"font-family:Aptos, sans-serif; font-size:11pt;\">
    Buenos días:<br><br>
    Se comparte el reporte de transacciones observadas de la plataforma FNB del <b>{fecha_min}</b> al <b>{fecha_max}</b><br>
    {resumen_html}
    <br>Quedo atento a cualquier observación.<br><br>

    Atentamente,<br><br>
    
    <img src=\"cid:firmaimg\">
    </body></html>"""

    outlook = win32.Dispatch("Outlook.Application")
    for idx, row in correos.iterrows():
        try:
            mail = outlook.CreateItem(0)
            mail.To = row['Destinatarios directos']
            if pd.notna(row.get('Destinatarios en copia')):
                mail.CC = row['Destinatarios en copia']
            mail.Subject = asunto

            for archivo in archivos:
                mail.Attachments.Add(archivo)

            # Verificar si existe el archivo de firma antes de adjuntarlo
            if os.path.exists(firma_path):
                attach = mail.Attachments.Add(firma_path)
                attach.PropertyAccessor.SetProperty("http://schemas.microsoft.com/mapi/proptag/0x3712001F", "firmaimg")

            mail.HTMLBody = cuerpo
            mail.Send()
            print(f"📧 Correo preparado para: {row['Destinatarios directos']}")

        except Exception as e:
            print(f"❌ Error preparando correo para {row['Destinatarios directos']}: {str(e)}")

    print("🎉 Proceso de preparación de correos completado.")

    # Verificación final: Buscar registros con INVALIDO que no se procesaron
    print("\n🔍 Verificación final de registros con estado 'INVALIDO':")
    registros_invalido = df[df['ESTADO'] == 'INVALIDO']
    if len(registros_invalido) > 0:
        print(
            f"⚠️  Se encontraron {len(registros_invalido)} registros con estado 'INVALIDO' que podrían no haberse procesado:")
        print("   Considera actualizar los datos fuente para usar los nuevos estados de validación fallida")
    else:
        print("✅ No se encontraron registros con estado 'INVALIDO' - migración completada")

except Exception as e:
    print(f"❌ Error en el proceso de correos: {str(e)}")

    # Verificación final también en caso de error
    print("\n🔍 Verificación final de registros con estado 'INVALIDO':")
    registros_invalido = df[df['ESTADO'] == 'INVALIDO']
    if len(registros_invalido) > 0:
        print(
            f"⚠️  Se encontraron {len(registros_invalido)} registros con estado 'INVALIDO' que podrían no haberse procesado")
    else:
        print("✅ No se encontraron registros con estado 'INVALIDO'")

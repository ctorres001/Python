import pandas as pd
import os
import glob
from datetime import datetime, timedelta
import win32com.client as win32
from xlsxwriter import Workbook
import re

# -------------------------
# Configuración de rutas
# -------------------------
ruta_base = r"D:\FNB\Reportes\19. Reportes IBR"
carpeta_reporte_sap = os.path.join(ruta_base, r"05. Reporte incidencias SAP\Reporte SAP")
ruta_procesado = os.path.join(ruta_base, r"00. Estructura Reporte\Procesado\Archivo_Procesado.xlsx")
ruta_destinatarios = os.path.join(ruta_base,
                                  r"05. Reporte incidencias SAP\Destinatarios\Listado de correos incidencias.xlsx")
ruta_destinatarios_exonerados = os.path.join(
    ruta_base,
    r"05. Reporte incidencias SAP\Destinatarios\Listado de correos incidencias exonerados.xlsx"
)
ruta_salida = os.path.join(ruta_base, r"05. Reporte incidencias SAP\Archivos")
firma_path = os.path.join(ruta_base, r"01. Pendientes de Entrega\Firma\Firma_resized.jpg")
ruta_exonerados = os.path.join(ruta_base, r"05. Reporte incidencias SAP\Exonerados\Exonerados.xlsx")
# Carpeta para duplicados retirados
ruta_duplicados = os.path.join(ruta_base, r"05. Reporte incidencias SAP\Duplicados Retirados SAP")

# Crear directorios si no existen
os.makedirs(ruta_salida, exist_ok=True)
os.makedirs(ruta_duplicados, exist_ok=True)

print("🔄 Iniciando proceso de generación de reporte SAP vs FNB...")

# -------------------------
# Funciones auxiliares
# -------------------------

def extraer_fecha_hora_archivo(nombre_archivo):
    """
    Extrae fecha y hora del nombre del archivo para ordenamiento
    Formato específico: EXPORT_YYYYMMDD_HHMMSS.xlsx
    """
    nombre_sin_ext = os.path.splitext(nombre_archivo)[0]
    patron_export = r'EXPORT_(\d{8})_(\d{6})'
    match = re.search(patron_export, nombre_sin_ext, re.IGNORECASE)
    if match:
        try:
            fecha_str = match.group(1)
            hora_str = match.group(2)
            fecha_hora_str = fecha_str + hora_str
            fecha_hora = datetime.strptime(fecha_hora_str, "%Y%m%d%H%M%S")
            return fecha_hora
        except Exception as e:
            print(f"   ⚠️  Error parseando fecha del archivo {nombre_archivo}: {str(e)}")
            return None

    patrones_alternativos = [
        (r'(\d{8})_(\d{6})', "%Y%m%d%H%M%S"),
        (r'(\d{8})\s+(\d{6})', "%Y%m%d%H%M%S"),
        (r'(\d{2})-(\d{2})-(\d{4})_(\d{2})-(\d{2})-(\d{2})', "%d-%m-%Y_%H-%M-%S"),
    ]

    for patron, formato in patrones_alternativos:
        match = re.search(patron, nombre_sin_ext)
        if match:
            try:
                if len(match.groups()) == 2:
                    fecha_hora_str = match.group(1) + match.group(2)
                    return datetime.strptime(fecha_hora_str, formato.replace('_', ''))
                else:
                    fecha_hora_str = ''.join(match.groups())
                    return datetime.strptime(fecha_hora_str, formato.replace('-', '').replace('_', ''))
            except:
                continue
    return None


def consolidar_archivos_sap(carpeta):
    """
    Consolida múltiples archivos SAP en un solo DataFrame
    """
    print("\n" + "="*60)
    print("📂 CONSOLIDANDO ARCHIVOS SAP")
    print("="*60)

    if not os.path.exists(carpeta):
        print(f"❌ Carpeta SAP no encontrada: {carpeta}")
        exit()

    archivos_excel = glob.glob(os.path.join(carpeta, "*.xlsx"))
    if not archivos_excel:
        print("❌ No se encontraron archivos Excel en la carpeta SAP")
        print(f"📁 Carpeta buscada: {carpeta}")
        exit()

    print(f"📊 Total de archivos encontrados: {len(archivos_excel)}")

    info_archivos = []
    for archivo in archivos_excel:
        nombre = os.path.basename(archivo)
        fecha_nombre = extraer_fecha_hora_archivo(nombre)
        if fecha_nombre is None:
            fecha_mod = datetime.fromtimestamp(os.path.getmtime(archivo))
            fecha_orden = fecha_mod
            origen_fecha = "fecha modificación"
        else:
            fecha_orden = fecha_nombre
            origen_fecha = "nombre archivo"

        info_archivos.append({
            'ruta': archivo,
            'nombre': nombre,
            'fecha_orden': fecha_orden,
            'origen_fecha': origen_fecha,
            'tamano_kb': os.path.getsize(archivo) / 1024
        })

    info_archivos.sort(key=lambda x: x['fecha_orden'])

    print("\n📋 ARCHIVOS A CONSOLIDAR (en orden):")
    print("-" * 60)
    for i, info in enumerate(info_archivos, 1):
        print(f"{i}. {info['nombre']}")
        print(f"   📅 Fecha: {info['fecha_orden'].strftime('%d/%m/%Y %H:%M:%S')} ({info['origen_fecha']})")
        print(f"   💾 Tamaño: {info['tamano_kb']:.2f} KB")
        print()

    print("🔄 Iniciando consolidación...")
    dfs = []
    registro_consolidacion = []

    for i, info in enumerate(info_archivos, 1):
        try:
            print(f"📖 Leyendo archivo {i}/{len(info_archivos)}: {info['nombre']}")
            df_temp = pd.read_excel(info['ruta'], dtype=str)
            df_temp['_ARCHIVO_ORIGEN'] = info['nombre']
            df_temp['_FECHA_ARCHIVO'] = info['fecha_orden'].strftime('%d/%m/%Y %H:%M:%S')
            registros = len(df_temp)
            dfs.append(df_temp)
            registro_consolidacion.append({
                'archivo': info['nombre'],
                'registros': registros,
                'fecha': info['fecha_orden']
            })
            print(f"   ✅ {registros} registros cargados")
        except Exception as e:
            print(f"   ❌ Error leyendo archivo: {str(e)}")
            continue

    if not dfs:
        print("❌ No se pudo cargar ningún archivo")
        exit()

    print("\n🔗 Concatenando archivos...")
    df_consolidado = pd.concat(dfs, ignore_index=True)

    columnas_clave = []
    if 'Id FNB' in df_consolidado.columns:
        columnas_clave.append('Id FNB')
    if 'Numero pedido SAP' in df_consolidado.columns:
        columnas_clave.append('Numero pedido SAP')
    if 'Contrato' in df_consolidado.columns:
        columnas_clave.append('Contrato')

    registros_antes = len(df_consolidado)
    df_duplicados = None

    if columnas_clave:
        print(f"🔍 Eliminando duplicados basados en: {', '.join(columnas_clave)}")
        mask_duplicados = df_consolidado.duplicated(subset=columnas_clave, keep='last')
        df_duplicados = df_consolidado[mask_duplicados].copy()
        df_consolidado = df_consolidado.drop_duplicates(subset=columnas_clave, keep='last')
        registros_despues = len(df_consolidado)
        duplicados = registros_antes - registros_despues

        if duplicados > 0:
            print(f"   🗑️  {duplicados} registros duplicados identificados")
            fecha_proceso = datetime.now().strftime('%Y%m%d_%H%M%S')
            nombre_archivo_duplicados = f"Duplicados_Retirados_{fecha_proceso}.xlsx"
            ruta_archivo_duplicados = os.path.join(ruta_duplicados, nombre_archivo_duplicados)
            try:
                df_duplicados_ordenado = df_duplicados.sort_values('_FECHA_ARCHIVO', ascending=False)
                with pd.ExcelWriter(ruta_archivo_duplicados, engine='xlsxwriter') as writer:
                    df_duplicados_ordenado.to_excel(writer, index=False, sheet_name='Duplicados Retirados')
                    workbook = writer.book
                    worksheet = writer.sheets['Duplicados Retirados']
                    header_format = workbook.add_format({
                        'bold': True,
                        'font_name': 'Aptos',
                        'font_size': 9,
                        'bg_color': '#D32F2F',
                        'font_color': '#FFFFFF',
                        'align': 'center',
                        'valign': 'vcenter',
                        'border': 1
                    })
                    cell_format = workbook.add_format({
                        'font_name': 'Aptos',
                        'font_size': 8,
                        'align': 'left',
                        'valign': 'vcenter',
                        'border': 1
                    })
                    highlight_format = workbook.add_format({
                        'font_name': 'Aptos',
                        'font_size': 8,
                        'align': 'left',
                        'valign': 'vcenter',
                        'bg_color': '#FFF9C4',
                        'border': 1
                    })
                    for col_num, value in enumerate(df_duplicados_ordenado.columns):
                        worksheet.write(0, col_num, value, header_format)
                        max_len = max(
                            df_duplicados_ordenado[value].astype(str).map(len).max(),
                            len(value)
                        )
                        worksheet.set_column(col_num, col_num, min(max_len + 2, 50))
                    for row_num in range(len(df_duplicados_ordenado)):
                        for col_num, col_name in enumerate(df_duplicados_ordenado.columns):
                            value = df_duplicados_ordenado.iloc[row_num, col_num]
                            if col_name in ['_ARCHIVO_ORIGEN', '_FECHA_ARCHIVO']:
                                formato = highlight_format
                            else:
                                formato = cell_format
                            if pd.isna(value):
                                worksheet.write(row_num + 1, col_num, "", formato)
                            else:
                                worksheet.write(row_num + 1, col_num, str(value), formato)
                    df_resumen = df_duplicados_ordenado.groupby('_ARCHIVO_ORIGEN').size().reset_index(name='Cantidad')
                    df_resumen.to_excel(writer, sheet_name='Resumen por Archivo', index=False)
                    worksheet_resumen = writer.sheets['Resumen por Archivo']
                    for col_num, value in enumerate(df_resumen.columns):
                        worksheet_resumen.write(0, col_num, value, header_format)
                        max_len = max(df_resumen[value].astype(str).map(len).max(), len(value))
                        worksheet_resumen.set_column(col_num, col_num, max_len + 2)
                print(f"   💾 Duplicados guardados en: {nombre_archivo_duplicados}")
                print(f"\n   📊 Resumen de duplicados por archivo origen:")
                duplicados_por_archivo = df_duplicados['_ARCHIVO_ORIGEN'].value_counts()
                for archivo, cantidad in duplicados_por_archivo.items():
                    print(f"      • {archivo}: {cantidad} duplicados")
            except Exception as e:
                print(f"   ⚠️  Error guardando duplicados: {str(e)}")
        else:
            print(f"   ✅ No se encontraron duplicados")
    else:
        registros_despues = registros_antes
        duplicados = 0
        print(f"   ℹ️  No hay columnas clave para identificar duplicados")

    print("\n" + "="*60)
    print("✅ CONSOLIDACIÓN COMPLETADA")
    print("="*60)
    print(f"📊 Archivos procesados: {len(registro_consolidacion)}")
    print(f"📊 Total registros antes de deduplicar: {registros_antes:,}")
    if columnas_clave:
        if duplicados > 0:
            print(f"📊 Registros duplicados eliminados: {duplicados:,}")
    print(f"📊 Total registros finales: {registros_despues:,}")
    print("="*60 + "\n")

    print("📋 RESUMEN POR ARCHIVO:")
    print("-" * 60)
    for reg in registro_consolidacion:
        print(f"• {reg['archivo']}: {reg['registros']:,} registros")
    print("-" * 60 + "\n")

    return df_consolidado


# -------------------------
# Validaciones / utilidades
# -------------------------
def aplicar_exoneraciones(df, ids_exonerados):
    """
    Aplica las exoneraciones al DataFrame basándose en la columna 'Id FNB'
    y devuelve también el DataFrame de casuísticas exoneradas
    """
    if not ids_exonerados or 'Id FNB' not in df.columns:
        print("📝 No se aplicarán exoneraciones (sin IDs o sin columna Id FNB)")
        return df, 0, pd.DataFrame()

    print("🚫 Aplicando exoneraciones...")

    registros_antes = len(df)

    mask_exonerado = df['Id FNB'].astype(str).str.strip().isin(ids_exonerados)
    df_exonerados_df = df[mask_exonerado].copy()
    df_filtrado = df[~mask_exonerado].copy()

    registros_exonerados = len(df_exonerados_df)

    print(f"📊 Registros antes de exonerar: {registros_antes}")
    print(f"📊 Registros exonerados: {registros_exonerados}")
    print(f"📊 Registros después de exonerar: {len(df_filtrado)}")

    return df_filtrado, registros_exonerados, df_exonerados_df


def verificar_estados_equivalentes(estado_sap, estado_fnb):
    sap_vacio = pd.isna(estado_sap) or estado_sap == '' or (isinstance(estado_sap, str) and estado_sap.strip() == '')
    fnb_vacio = pd.isna(estado_fnb) or estado_fnb == '' or (isinstance(estado_fnb, str) and estado_fnb.strip() == '')
    if sap_vacio and fnb_vacio:
        return True
    if sap_vacio or fnb_vacio:
        return False
    estado_sap = str(estado_sap).upper().strip()
    estado_fnb = str(estado_fnb).upper().strip()
    equivalencias = {
        'CONCLUIDO': ['ENTREGADO', 'PENDIENTE DE ANULACIÓN'],
        'EN TRATAMIENTO': ['PENDIENTE DE ENTREGA', 'ERROR DE INTEGRACIÓN', 'PENDIENTE DE ANULACIÓN', 'PENDIENTE DE APROBACIÓN'],
        'RECHAZADO': ['ANULADO', 'ANULADO POR CRÉDITO']
    }
    for sap_estado, fnb_estados in equivalencias.items():
        if estado_sap == sap_estado and estado_fnb in fnb_estados:
            return True
    return False


def tiene_datos(valor):
    return pd.notna(valor) and str(valor).strip() != ''


def formatear_df_sap(df_filtrado):
    df_filtrado = df_filtrado.copy()
    fecha_columns = ['Fecha Venta SAP', 'Fecha Venta FNB', 'Fecha Entrega SAP', 'Fecha Entrega FNB']
    for col in fecha_columns:
        if col in df_filtrado.columns:
            df_filtrado[col] = pd.to_datetime(df_filtrado[col], errors='coerce').dt.strftime('%d/%m/%Y').fillna("")
    return df_filtrado


def exportar_excel_sap(df_filtrado, ruta, nombre_hoja='Incidencias SAP'):
    with pd.ExcelWriter(ruta, engine='xlsxwriter') as writer:
        df_filtrado.to_excel(writer, index=False, sheet_name=nombre_hoja)
        workbook: Workbook = writer.book
        worksheet = writer.sheets[nombre_hoja]
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
                elif col_name in ['Importe SAP', 'Importe FNB']:
                    try:
                        num_value = float(str(value).replace(',', ''))
                    except:
                        num_value = 0.0
                    worksheet.write(row, col, num_value, number_format)
                else:
                    worksheet.write(row, col, str(value), cell_format)


# -------------------------
# Consolidar SAP
# -------------------------
print("📂 Verificando archivos...")
df_sap = consolidar_archivos_sap(carpeta_reporte_sap)

# Verificar archivo procesado
if not os.path.exists(ruta_procesado):
    print(f"❌ Archivo procesado no encontrado: {ruta_procesado}")
    exit()

# Cargar archivo de exonerados (lista de IDs a exonerar)
print("📂 Cargando archivo de exonerados...")
df_exonerados = None
registros_exonerados = set()
if os.path.exists(ruta_exonerados):
    try:
        df_exonerados = pd.read_excel(ruta_exonerados, dtype=str)
        if 'Id FNB' in df_exonerados.columns:
            ids_exonerados = df_exonerados['Id FNB'].dropna()
            ids_exonerados = ids_exonerados[ids_exonerados.astype(str).str.strip() != '']
            registros_exonerados = set(ids_exonerados.astype(str).str.strip())
            print(f"✅ Archivo de exonerados cargado: {len(registros_exonerados)} IDs a exonerar")
            if registros_exonerados:
                print(f"📋 Primeros 5 IDs exonerados: {list(registros_exonerados)[:5]}")
        else:
            print("⚠️  El archivo de exonerados no contiene la columna 'Id FNB'")
            print(f"📋 Columnas disponibles: {list(df_exonerados.columns)}")
    except Exception as e:
        print(f"⚠️  Error cargando archivo de exonerados: {str(e)}")
        print("📝 Se continuará sin exoneraciones")
else:
    print(f"⚠️  Archivo de exonerados no encontrado: {ruta_exonerados}")
    print("📝 Se continuará sin exoneraciones")

# Leer archivo procesado
print("📂 Cargando archivo procesado...")
try:
    df_procesado = pd.read_excel(ruta_procesado, dtype=str)
    print(f"✅ Archivo procesado cargado: {len(df_procesado)} registros")
except Exception as e:
    print(f"❌ Error cargando archivo procesado: {str(e)}")
    exit()

# Limpiar datos (quitar espacios en blanco)
print("🧹 Limpiando datos...")
df_sap = df_sap.apply(
    lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x) if col.dtype == 'object' else col)
df_procesado = df_procesado.apply(
    lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x) if col.dtype == 'object' else col)

print("✅ Datos limpios y listos para procesamiento\n")

# -------------------------
# Obtener CANAL_VENTA y TIPO DESPACHO (cruce)
# -------------------------
def obtener_canal_venta_y_tipo_despacho(df_sap, df_procesado):
    """
    Cruce por Nro. PEDIDO VENTA primero, luego por Nro. DE CONTRATO
    También obtiene TIPO DESPACHO del archivo procesado
    """
    print("🔗 Realizando cruce para obtener CANAL_VENTA y TIPO DESPACHO...")

    df_canal = df_procesado[['Nro. PEDIDO VENTA', 'Nro. DE CONTRATO', 'CANAL_VENTA', 'TIPO DESPACHO']].copy()
    df_canal = df_canal.dropna(subset=['CANAL_VENTA'])

    df_canal_pedido = df_canal[['Nro. PEDIDO VENTA', 'CANAL_VENTA', 'TIPO DESPACHO']].dropna(subset=['Nro. PEDIDO VENTA'])
    df_canal_pedido = df_canal_pedido.drop_duplicates(subset=['Nro. PEDIDO VENTA'], keep='first')

    df_canal_contrato = df_canal[['Nro. DE CONTRATO', 'CANAL_VENTA', 'TIPO DESPACHO']].dropna(subset=['Nro. DE CONTRATO'])
    df_canal_contrato = df_canal_contrato.drop_duplicates(subset=['Nro. DE CONTRATO'], keep='first')

    df_sap_trabajo = df_sap[ df_sap['Status General'].notna() & (df_sap['Status General'].astype(str).str.strip() != '') ].copy()
    df_sap_trabajo = df_sap_trabajo.reset_index(drop=True)

    print(f"📊 Registros válidos en SAP (con Status General): {len(df_sap_trabajo)}")
    print(f"📊 Registros totales en SAP original: {len(df_sap)}")

    df_sap_trabajo['CANAL_VENTA'] = 'Canal de venta no identificado'
    df_sap_trabajo['TIPO DESPACHO'] = ''

    # Paso 1: por Id FNB mapped to Nro. PEDIDO VENTA if possible
    print("🔄 Paso 1: Cruce por Nro. PEDIDO VENTA (a partir del archivo procesado)...")
    if 'Id FNB' in df_sap_trabajo.columns:
        try:
            # Crear diccionarios para mapeo
            id_pedido_to_canal = df_canal_pedido.set_index('Nro. PEDIDO VENTA')['CANAL_VENTA'].to_dict()
            id_pedido_to_tipo = df_canal_pedido.set_index('Nro. PEDIDO VENTA')['TIPO DESPACHO'].to_dict()
            
            df_sap_trabajo.loc[:, 'CANAL_VENTA'] = df_sap_trabajo['Id FNB'].map(id_pedido_to_canal).fillna(
                df_sap_trabajo['CANAL_VENTA']
            )
            df_sap_trabajo.loc[:, 'TIPO DESPACHO'] = df_sap_trabajo['Id FNB'].map(id_pedido_to_tipo).fillna(
                df_sap_trabajo['TIPO DESPACHO']
            )
        except Exception as e:
            print(f"⚠️ Error mapeando Id FNB -> Nro. PEDIDO VENTA: {str(e)}")

    # Paso 2: para los que aún no tienen canal, intentar por Contrato
    print("🔄 Paso 2: Cruce por Contrato para registros sin canal...")
    mask_sin_canal = df_sap_trabajo['CANAL_VENTA'] == 'Canal de venta no identificado'
    mask_contrato_valido = (
            df_sap_trabajo['Contrato'].notna() &
            (df_sap_trabajo['Contrato'].astype(str).str.strip() != '') &
            (df_sap_trabajo['Contrato'].astype(str).str.strip() != 'nan')
    )
    mask_para_segundo_cruce = mask_sin_canal & mask_contrato_valido
    registros_segundo_cruce = mask_para_segundo_cruce.sum()
    print(f"📊 Registros sin canal que tienen Contrato válido: {registros_segundo_cruce}")

    if registros_segundo_cruce > 0:
        contrato_to_canal = df_canal_contrato.set_index('Nro. DE CONTRATO')['CANAL_VENTA'].to_dict()
        contrato_to_tipo = df_canal_contrato.set_index('Nro. DE CONTRATO')['TIPO DESPACHO'].to_dict()
        
        df_sap_trabajo.loc[mask_para_segundo_cruce, 'CANAL_VENTA'] = (
            df_sap_trabajo.loc[mask_para_segundo_cruce, 'Contrato'].map(contrato_to_canal)
            .fillna('Canal de venta no identificado')
        )
        df_sap_trabajo.loc[mask_para_segundo_cruce, 'TIPO DESPACHO'] = (
            df_sap_trabajo.loc[mask_para_segundo_cruce, 'Contrato'].map(contrato_to_tipo)
            .fillna('')
        )
        canales_por_contrato = (
                df_sap_trabajo.loc[mask_para_segundo_cruce, 'CANAL_VENTA'] != 'Canal de venta no identificado'
        ).sum()
        print(f"✅ Canales encontrados por Contrato: {canales_por_contrato}")

    total_canales_identificados = (df_sap_trabajo['CANAL_VENTA'] != 'Canal de venta no identificado').sum()
    total_canales_no_identificados = (df_sap_trabajo['CANAL_VENTA'] == 'Canal de venta no identificado').sum()

    print(f"📊 Resumen final:")
    print(f"   • Canales identificados: {total_canales_identificados}")
    print(f"   • Canales no identificados: {total_canales_no_identificados}")
    print(f"   • Total registros procesados: {len(df_sap_trabajo)}")

    print(f"📊 Distribución por canal:")
    canal_counts = df_sap_trabajo['CANAL_VENTA'].value_counts()
    for canal, count in canal_counts.items():
        print(f"   • {canal}: {count}")

    return df_sap_trabajo


# Verificar datos SAP antes del cruce
print("🔍 Verificando datos SAP antes del cruce...")
print("\n" + "=" * 50)

df_sap_con_canal = obtener_canal_venta_y_tipo_despacho(df_sap, df_procesado)

# -------------------------
# Aplicar exoneraciones ANTES de crear escenarios
# -------------------------
df_sap_con_canal, total_exonerados, df_exonerados_df = aplicar_exoneraciones(df_sap_con_canal, registros_exonerados)

# CAMBIO PRINCIPAL: Obtener fechas para el nombre del archivo (fecha actual menos 1 día)
fecha_reporte = datetime.now() - timedelta(days=1)
fecha_actual = fecha_reporte.strftime('%d-%m-%Y')
fecha_actual_texto = fecha_reporte.strftime('%d/%m/%Y')

print(f"📅 Fecha del reporte: {fecha_actual} ({fecha_actual_texto})")

# -------------------------
# Definir escenarios con segmentación
# -------------------------
def crear_escenarios(df):
    todos_los_escenarios = []
    
    # Escenario 1: Fecha de Venta diferente
    df_fecha_venta = df[df['Mensaje comparativa F Venta'].notna() & (df['Mensaje comparativa F Venta'] != '')].copy()
    todos_los_escenarios.append(("Fecha de Venta diferente entre SAP y FNB", df_fecha_venta))
    
    # Escenario 2: Fecha de Entrega diferente (con columna TIPO DESPACHO)
    df_fecha_entrega = df[df['Mensaje comparativa F Entega'].notna() & (df['Mensaje comparativa F Entega'] != '')].copy()
    todos_los_escenarios.append(("Fecha de Entrega diferente entre SAP y FNB", df_fecha_entrega))
    
    # Escenario 3: Responsable de Venta diferente
    df_responsable = df[df['Mensaje comparativa Responsable'].notna() & (df['Mensaje comparativa Responsable'] != '')].copy()
    todos_los_escenarios.append(("Responsable de Venta diferente entre SAP y FNB", df_responsable))
    
    # Escenario 4: Aliado Comercial diferente
    df_aliado = df[df['Mensaje comparativa Aliado'].notna() & (df['Mensaje comparativa Aliado'] != '')].copy()
    todos_los_escenarios.append(("Aliado Comercial (Proveedor) diferente entre SAP y FNB", df_aliado))
    
    # Escenario 5: Sede diferente - SEGMENTADO POR COMERCIAL Y PROYECTOS
    df_sede_base = df[df['Mensaje comparativa Sede'].notna() & (df['Mensaje comparativa Sede'] != '')].copy()
    
    # Sede - Comercial: Codigo sede SAP está en blanco
    condicion_sede_comercial = (
        df_sede_base['Codigo sede SAP'].isna() | 
        (df_sede_base['Codigo sede SAP'].astype(str).str.strip() == '') |
        (df_sede_base['Codigo sede SAP'].astype(str).str.strip() == 'nan')
    )
    df_sede_comercial = df_sede_base[condicion_sede_comercial].copy()
    todos_los_escenarios.append(("Sede diferente entre SAP y FNB - Comercial", df_sede_comercial))
    
    # Sede - Proyectos: Codigo sede SAP tiene valor
    df_sede_proyectos = df_sede_base[~condicion_sede_comercial].copy()
    todos_los_escenarios.append(("Sede diferente entre SAP y FNB - Proyectos", df_sede_proyectos))
    
    # Escenario 6: Importe Financiado diferente
    df_importe = df[df['Mensaje comparativa Importe'].notna() & (df['Mensaje comparativa Importe'] != '')].copy()
    todos_los_escenarios.append(("Importe Financiado diferente entre SAP y FNB", df_importe))
    
    # Escenario 7: Nro. de Cuotas diferentes
    df_cuotas = df[df['Mensaje comparativa Cuotas'].notna() & (df['Mensaje comparativa Cuotas'] != '')].copy()
    todos_los_escenarios.append(("Nro. de Cuotas diferentes entre SAP y FNB", df_cuotas))
    
    # Escenario 8: Estados de Entrega diferentes - SEGMENTADO POR COMERCIAL Y PROYECTOS
    df_estados_diferentes = df[
        ~df.apply(lambda row: verificar_estados_equivalentes(row['Estado SAP'], row['Estado FNB']), axis=1)
    ].copy()
    
    # Estados - Comercial: 
    # 1) Estado SAP es RECHAZADO y Estado FNB es ENTREGADO o PENDIENTE DE ENTREGA
    # 2) Estado FNB contiene "Validación" (case insensitive)
    condicion_estados_comercial = (
        (
            (df_estados_diferentes['Estado SAP'].astype(str).str.upper().str.strip() == 'RECHAZADO') &
            (df_estados_diferentes['Estado FNB'].astype(str).str.upper().str.strip().isin(['ENTREGADO', 'PENDIENTE DE ENTREGA']))
        ) |
        (
            df_estados_diferentes['Estado FNB'].astype(str).str.upper().str.contains('VALIDACIÓN', na=False)
        )
    )
    df_estados_comercial = df_estados_diferentes[condicion_estados_comercial].copy()
    todos_los_escenarios.append(("Estados de Entrega diferentes entre SAP y FNB - Comercial", df_estados_comercial))
    
    # Estados - Proyectos: el resto
    df_estados_proyectos = df_estados_diferentes[~condicion_estados_comercial].copy()
    todos_los_escenarios.append(("Estados de Entrega diferentes entre SAP y FNB - Proyectos", df_estados_proyectos))
    
    # Escenario 9 y 10: Contrato
    df_contrato_general = df[df['Mensaje comparativa Contrato'].notna() & (df['Mensaje comparativa Contrato'] != '')].copy()
    condicion_tienda_virtual = (
            df_contrato_general['Contrato SAP'].notna() &
            (df_contrato_general['Contrato SAP'] != '') &
            (df_contrato_general['Contrato'].isna() | (df_contrato_general['Contrato'] == '')) &
            (df_contrato_general['Nombre Responsable SAP'].str.upper() == 'TIENDA VIRTUAL WEB')
    )
    df_contrato_tienda_virtual = df_contrato_general[condicion_tienda_virtual].copy()
    todos_los_escenarios.append((
        "Nro de Contrato CD - Casos por regularizar el Nro. Contrato CD de la tienda virtual en el reporte de la plataforma FNB",
        df_contrato_tienda_virtual
    ))
    df_contrato_regulares = df_contrato_general[~condicion_tienda_virtual].copy()
    todos_los_escenarios.append(("Nro de Contrato CD - Casos regulares detectados", df_contrato_regulares))
    
    # Escenario 11: Transacciones FNB que no figuran en SAP
    df_fnb_no_sap = df[
        df['Id FNB'].apply(tiene_datos) & ~df['Numero pedido SAP'].apply(tiene_datos)
    ].copy()
    todos_los_escenarios.append(("Transacciones FNB que no figuran en SAP", df_fnb_no_sap))
    
    # Escenario 12: Transacciones SAP que no figuran en FNB
    df_sap_no_fnb = df[
        df['Numero pedido SAP'].apply(tiene_datos) & ~df['Id FNB'].apply(tiene_datos)
    ].copy()
    todos_los_escenarios.append(("Transacciones SAP que no figuran en FNB", df_sap_no_fnb))
    
    return todos_los_escenarios

print("🔄 Procesando escenarios...")
todos_los_escenarios = crear_escenarios(df_sap_con_canal)

# ============================================
# Solicitar exclusión de escenarios (interactivo)
# ============================================
print("\n📋 Escenarios detectados:")
for idx, (nombre_escenario, df_filtrado) in enumerate(todos_los_escenarios, 1):
    print(f"{idx}) {nombre_escenario} → {len(df_filtrado)} registros")

print("\n✏️ Si deseas EXCLUIR algún escenario (se enviará en 2do correo como casuística exonerada), ingresa los números separados por coma.")
print("   Si no deseas excluir ninguno, solo presiona ENTER.\n")

entrada = input("Escenarios a excluir: ").strip()

escenarios_excluir = set()
if entrada:
    try:
        escenarios_excluir = {int(x.strip()) for x in entrada.split(",") if x.strip().isdigit()}
        print(f"🚫 Se excluirán los escenarios: {', '.join(map(str, sorted(escenarios_excluir)))}\n")
    except:
        print("⚠️ Formato inválido. No se excluirá ningún escenario.\n")

# ============================================
# Generar archivos y texto del correo
# ============================================
archivos = []  # archivos para correo principal
exoneradas_archivos = []  # archivos para segundo correo
resumen_html = "<b>PROYECTOS</b><br><br>"

for idx, (nombre_escenario, df_filtrado) in enumerate(todos_los_escenarios, 1):

    if df_filtrado.empty:
        continue

    # Formatear df para exportar
    df_filtrado_formateado = formatear_df_sap(df_filtrado)

    # Nombre de archivo estándar
    safe_nombre = re.sub(r'[\\/*?:\[\]]', ' - ', nombre_escenario)
    nombre_archivo = f"{safe_nombre} - {fecha_actual}.xlsx"
    ruta_archivo = os.path.join(ruta_salida, nombre_archivo)

    if idx in escenarios_excluir:
        print(f"⏭️ Escenario EXONERADO (irá únicamente al 2do correo): {nombre_escenario}")
        try:
            exportar_excel_sap(df_filtrado_formateado, ruta_archivo)
            exoneradas_archivos.append(ruta_archivo)
            print(f"   💾 Archivo exonerado generado: {nombre_archivo}")
        except Exception as e:
            print(f"   ⚠️ Error exportando archivo exonerado {nombre_archivo}: {str(e)}")
        continue

    else:
        print(f"\n🔄 Procesando: {nombre_escenario}")
        try:
            exportar_excel_sap(df_filtrado_formateado, ruta_archivo)
            archivos.append(ruta_archivo)
            print(f"   💾 Archivo generado: {nombre_archivo}")
        except Exception as e:
            print(f"   ⚠️ Error exportando archivo {nombre_archivo}: {str(e)}")

        conteo = len(df_filtrado)
        resumen = f"<b>{nombre_escenario}</b>: <b>{conteo}</b> registros detectados<br>"
        if 'CANAL_VENTA' in df_filtrado.columns:
            canal_counts = df_filtrado['CANAL_VENTA'].fillna('Canal de venta no identificado').value_counts()
            for canal, count in canal_counts.items():
                resumen += f"<span style='font-size: 10pt;'>- {canal}: {count}</span><br>"
        resumen += "<br>"
        resumen_html += resumen

print("\n✅ Escenarios procesados con exclusión aplicada.\n")

# Verificar si hay archivos para enviar en principal
if not archivos:
    print("⚠️  No se generaron archivos para el correo principal. Se continuará con el envío del correo de exonerados (si aplica).")
else:
    # -------------------------
    # Envío de correo principal (automatico)
    # -------------------------
    print("\n📧 Preparando envío de correos principales...")

    try:
        correos = pd.read_excel(ruta_destinatarios)
        correos = correos.dropna(subset=['Destinatarios directos'])

        asunto = f"Reporte de diferencias entre SAP y la plataforma FNB al {fecha_actual}"
        cuerpo = f"""<html><body style="font-family:Aptos, sans-serif; font-size:11pt;">
        Buenos días:<br><br>
        Se comparte el reporte de diferencias entre SAP y la plataforma FNB al <b>{fecha_actual_texto}</b>.<br><br>
        {resumen_html}
        Quedo atento a cualquier observación.<br><br>
        Atentamente,<br><br>
        <img src="cid:firmaimg">
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

                if os.path.exists(firma_path):
                    attach = mail.Attachments.Add(firma_path)
                    attach.PropertyAccessor.SetProperty(
                        "http://schemas.microsoft.com/mapi/proptag/0x3712001F", "firmaimg"
                    )

                mail.HTMLBody = cuerpo
                mail.Send()
                print(f"📧 Correo enviado a: {row['Destinatarios directos']}")

            except Exception as e:
                print(f"❌ Error preparando/enviando correo para {row['Destinatarios directos']}: {str(e)}")

        print("🎉 Envío de correos principales completado.")
    except Exception as e:
        print(f"❌ Error en el proceso de correos principales: {str(e)}")

# -------------------------
# Envío de casuísticas exoneradas en segundo correo (automatico)
# -------------------------
enviar_segundo_correo = False
archivos_segundo_correo = []

# 1) incluir exonerados individuales si existen
if total_exonerados > 0 and not df_exonerados_df.empty:
    nombre_archivo_exon_ind = f"Casuisticas_Exoneradas_Individuales - {fecha_actual}.xlsx"
    ruta_archivo_exon_ind = os.path.join(ruta_salida, nombre_archivo_exon_ind)
    try:
        df_exonerados_formateado = formatear_df_sap(df_exonerados_df)
        exportar_excel_sap(df_exonerados_formateado, ruta_archivo_exon_ind)
        archivos_segundo_correo.append(ruta_archivo_exon_ind)
        enviar_segundo_correo = True
        print(f"💾 Archivo de exonerados individuales generado: {nombre_archivo_exon_ind}")
    except Exception as e:
        print(f"⚠️ Error exportando exonerados individuales: {str(e)}")

# 2) incluir archivos de casuísticas exoneradas (escenarios) si existen
if exoneradas_archivos:
    archivos_segundo_correo.extend(exoneradas_archivos)
    enviar_segundo_correo = True

# Enviar segundo correo solo si hay algo para enviar
if enviar_segundo_correo:
    print("\n📧 Preparando correo para casuísticas exoneradas...")

    asunto_exon = f"Reporte de diferencias entre SAP y la plataforma FNB al {fecha_actual} - Casuísticas exoneradas"

    cuerpo_exon = f"""<html><body style="font-family:Aptos, sans-serif; font-size:11pt;">
    Buenos días:<br><br>
    Las casuísticas reportadas en archivos adjuntos han sido exoneradas para su revisión respecto al funcionamiento del reporte de conciliación de SAP.<br><br>
    <b>Archivos incluidos en este correo:</b><br>
    """
    for a in archivos_segundo_correo:
        cuerpo_exon += f"- {os.path.basename(a)}<br>"
    cuerpo_exon += "<br>Atentamente,<br><br><img src=\"cid:firmaimg\"></body></html>"

    try:
        if os.path.exists(ruta_destinatarios_exonerados):
            correos_exonerados = pd.read_excel(ruta_destinatarios_exonerados)
            correos_exonerados = correos_exonerados.dropna(subset=['Destinatarios directos'])

            outlook = win32.Dispatch("Outlook.Application")
            for idx, row in correos_exonerados.iterrows():
                try:
                    mail = outlook.CreateItem(0)
                    mail.To = row['Destinatarios directos']
                    if pd.notna(row.get('Destinatarios en copia')):
                        mail.CC = row['Destinatarios en copia']
                    mail.Subject = asunto_exon

                    for archivo in archivos_segundo_correo:
                        mail.Attachments.Add(archivo)

                    if os.path.exists(firma_path):
                        attach = mail.Attachments.Add(firma_path)
                        attach.PropertyAccessor.SetProperty(
                            "http://schemas.microsoft.com/mapi/proptag/0x3712001F", "firmaimg"
                        )

                    mail.HTMLBody = cuerpo_exon
                    mail.Send()
                    print(f"📧 Correo de exonerados enviado a: {row['Destinatarios directos']}")
                except Exception as e:
                    print(f"❌ Error enviando correo de exonerados a {row.get('Destinatarios directos')}: {str(e)}")
        else:
            print(f"⚠️ No se encontró el archivo de destinatarios exonerados: {ruta_destinatarios_exonerados}")
    except Exception as e:
        print(f"❌ Error en el envío de casuísticas exoneradas: {str(e)}")
else:
    print("\nℹ️ No hay casuísticas exoneradas para enviar.")

# -------------------------
# Resumen final en consola
# -------------------------
print("\n📊 Resumen del proceso:")
total_registros = sum(len(df_filtrado) for nombre_escenario, df_filtrado in todos_los_escenarios if not df_filtrado.empty)
print(f"✅ Archivos generados para envío principal: {len(archivos)}")
print(f"✅ Archivos generados para envío exonerados (casuísticas): {len(exoneradas_archivos)}")
print(f"✅ Registros procesados en total: {total_registros}")
if total_exonerados > 0:
    print(f"🚫 Registros exonerados individuales (descartados del análisis): {total_exonerados}")
print("🎉 Proceso completado exitosamente.")
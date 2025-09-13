import pandas as pd
import os
import glob
from datetime import datetime, timedelta
import win32com.client as win32
from xlsxwriter import Workbook

# Configuración de rutas
ruta_base = r"D:\FNB\Reportes\19. Reportes IBR"
carpeta_reporte_sap = os.path.join(ruta_base, r"05. Reporte incidencias SAP\Reporte SAP")
ruta_procesado = os.path.join(ruta_base, r"00. Estructura Reporte\Procesado\Archivo_Procesado.xlsx")
ruta_destinatarios = os.path.join(ruta_base,
                                  r"05. Reporte incidencias SAP\Destinatarios\Listado de correos incidencias.xlsx")
ruta_salida = os.path.join(ruta_base, r"05. Reporte incidencias SAP\Archivos")
firma_path = os.path.join(ruta_base, r"01. Pendientes de Entrega\Firma\Firma_resized.jpg")
# NUEVO: Ruta del archivo de exoneraciones
ruta_exonerados = os.path.join(ruta_base, r"05. Reporte incidencias SAP\Exonerados\Exonerados.xlsx")

# Crear directorio de salida si no existe
os.makedirs(ruta_salida, exist_ok=True)

print("🔄 Iniciando proceso de generación de reporte SAP vs FNB...")

# Verificar y encontrar el archivo SAP
print("📂 Verificando archivos...")

# Buscar archivos Excel en la carpeta SAP
if os.path.exists(carpeta_reporte_sap):
    archivos_excel = glob.glob(os.path.join(carpeta_reporte_sap, "*.xlsx"))
    if archivos_excel:
        ruta_reporte_sap = archivos_excel[0]  # Tomar el primer archivo Excel encontrado
        print(f"📁 Archivo SAP encontrado: {os.path.basename(ruta_reporte_sap)}")
    else:
        print("❌ No se encontraron archivos Excel en la carpeta SAP")
        print(f"📁 Carpeta buscada: {carpeta_reporte_sap}")
        if os.path.exists(carpeta_reporte_sap):
            print("📁 Archivos en la carpeta:")
            for archivo in os.listdir(carpeta_reporte_sap):
                print(f"  - {archivo}")
        exit()
else:
    print(f"❌ Carpeta SAP no encontrada: {carpeta_reporte_sap}")
    exit()

# Verificar archivo procesado
if not os.path.exists(ruta_procesado):
    print(f"❌ Archivo procesado no encontrado: {ruta_procesado}")
    exit()

# NUEVO: Cargar archivo de exonerados
print("📂 Cargando archivo de exonerados...")
df_exonerados = None
registros_exonerados = set()

if os.path.exists(ruta_exonerados):
    try:
        df_exonerados = pd.read_excel(ruta_exonerados, dtype=str)
        if 'Id FNB' in df_exonerados.columns:
            # Limpiar y crear set de IDs exonerados (sin valores nulos o vacíos)
            ids_exonerados = df_exonerados['Id FNB'].dropna()
            ids_exonerados = ids_exonerados[ids_exonerados.astype(str).str.strip() != '']
            registros_exonerados = set(ids_exonerados.astype(str).str.strip())
            print(f"✅ Archivo de exonerados cargado: {len(registros_exonerados)} IDs a exonerar")
            
            # Mostrar algunos ejemplos de IDs exonerados
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

# Leer archivos
print("📂 Cargando archivos...")
try:
    # Cargar reporte SAP
    df_sap = pd.read_excel(ruta_reporte_sap, dtype=str)
    print(f"✅ Archivo SAP cargado: {len(df_sap)} registros")

    # Cargar archivo procesado para obtener CANAL_VENTA
    df_procesado = pd.read_excel(ruta_procesado, dtype=str)
    print(f"✅ Archivo procesado cargado: {len(df_procesado)} registros")

except Exception as e:
    print(f"❌ Error cargando archivos: {str(e)}")
    print(f"📁 Ruta SAP: {ruta_reporte_sap}")
    print(f"📁 Ruta procesado: {ruta_procesado}")
    exit()

# Limpiar datos (quitar espacios en blanco)
df_sap = df_sap.apply(
    lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x) if col.dtype == 'object' else col)
df_procesado = df_procesado.apply(
    lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x) if col.dtype == 'object' else col)


def aplicar_exoneraciones(df, ids_exonerados):
    """
    Aplica las exoneraciones al DataFrame basándose en la columna 'Id FNB'
    """
    if not ids_exonerados or 'Id FNB' not in df.columns:
        print("📝 No se aplicarán exoneraciones (sin IDs o sin columna Id FNB)")
        return df, 0
    
    print("🚫 Aplicando exoneraciones...")
    
    # Contar registros antes de exonerar
    registros_antes = len(df)
    
    # Crear máscara para identificar registros NO exonerados
    # Un registro se exonera si su Id FNB está en la lista de exonerados
    mask_no_exonerado = ~df['Id FNB'].astype(str).str.strip().isin(ids_exonerados)
    
    # Aplicar filtro
    df_filtrado = df[mask_no_exonerado].copy()
    
    # Contar registros exonerados
    registros_exonerados = registros_antes - len(df_filtrado)
    
    print(f"📊 Registros antes de exonerar: {registros_antes}")
    print(f"📊 Registros exonerados: {registros_exonerados}")
    print(f"📊 Registros después de exonerar: {len(df_filtrado)}")
    
    return df_filtrado, registros_exonerados


def verificar_datos_sap(df_sap):
    """
    Función para verificar la calidad de los datos en el archivo SAP
    """
    print("🔍 Verificando calidad de datos en archivo SAP...")

    total_registros = len(df_sap)
    print(f"📊 Total registros en SAP: {total_registros}")

    # Verificar Status General
    status_validos = df_sap['Status General'].notna() & (df_sap['Status General'].astype(str).str.strip() != '')
    print(f"📊 Registros con Status General válido: {status_validos.sum()}")

    # Verificar Id FNB
    id_fnb_validos = (
            df_sap['Id FNB'].notna() &
            (df_sap['Id FNB'].astype(str).str.strip() != '') &
            (df_sap['Id FNB'].astype(str).str.strip() != 'nan')
    )
    print(f"📊 Registros con Id FNB válido: {id_fnb_validos.sum()}")

    # Verificar Contrato
    contrato_validos = (
            df_sap['Contrato'].notna() &
            (df_sap['Contrato'].astype(str).str.strip() != '') &
            (df_sap['Contrato'].astype(str).str.strip() != 'nan')
    )
    print(f"📊 Registros con Contrato válido: {contrato_validos.sum()}")

    # Verificar registros sin Id FNB ni Contrato pero con Status General
    sin_id_ni_contrato = status_validos & (~id_fnb_validos) & (~contrato_validos)
    print(f"📊 Registros con Status General pero sin Id FNB ni Contrato: {sin_id_ni_contrato.sum()}")

    if sin_id_ni_contrato.sum() > 0:
        print("⚠️  Primeros 5 registros sin Id FNB ni Contrato:")
        cols_mostrar = ['Id FNB', 'Contrato', 'Status General']
        # Agregar columnas adicionales que puedan ser útiles
        cols_adicionales = ['Numero pedido SAP', 'Contrato SAP']
        for col in cols_adicionales:
            if col in df_sap.columns:
                cols_mostrar.append(col)

        print(df_sap[sin_id_ni_contrato][cols_mostrar].head())

    return {
        'total_registros': total_registros,
        'status_validos': status_validos.sum(),
        'id_fnb_validos': id_fnb_validos.sum(),
        'contrato_validos': contrato_validos.sum(),
        'sin_id_ni_contrato': sin_id_ni_contrato.sum()
    }


def obtener_canal_venta(df_sap, df_procesado):
    """
    Hace cruce entre archivos SAP y procesado para obtener CANAL_VENTA
    CORREGIDO: Maneja correctamente los casos donde Id FNB está vacío
    """
    print("🔗 Realizando cruce para obtener CANAL_VENTA...")

    # Preparar df_procesado para el cruce (SIN MODIFICAR - como solicitas)
    df_canal = df_procesado[['Nro. PEDIDO VENTA', 'Nro. DE CONTRATO', 'CANAL_VENTA']].copy()
    df_canal = df_canal.dropna(subset=['CANAL_VENTA'])

    # Eliminar duplicados en df_canal para evitar multiplicación de registros
    df_canal_pedido = df_canal[['Nro. PEDIDO VENTA', 'CANAL_VENTA']].dropna(subset=['Nro. PEDIDO VENTA'])
    df_canal_pedido = df_canal_pedido.drop_duplicates(subset=['Nro. PEDIDO VENTA'], keep='first')

    df_canal_contrato = df_canal[['Nro. DE CONTRATO', 'CANAL_VENTA']].dropna(subset=['Nro. DE CONTRATO'])
    df_canal_contrato = df_canal_contrato.drop_duplicates(subset=['Nro. DE CONTRATO'], keep='first')

    # CORRECCIÓN PRINCIPAL: Filtrar registros válidos del archivo SAP
    print("📊 Analizando registros válidos en archivo SAP...")

    # Un registro es válido si tiene datos en Status General
    df_sap_trabajo = df_sap[
        df_sap['Status General'].notna() & (df_sap['Status General'].astype(str).str.strip() != '')].copy()
    df_sap_trabajo = df_sap_trabajo.reset_index(drop=True)

    print(f"📊 Registros válidos en SAP (con Status General): {len(df_sap_trabajo)}")
    print(f"📊 Registros totales en SAP original: {len(df_sap)}")

    # Inicializar la columna CANAL_VENTA
    df_sap_trabajo['CANAL_VENTA'] = 'Canal de venta no identificado'

    # PASO 1: Intentar cruce por Id FNB (solo si no está vacío)
    print("🔄 Paso 1: Cruce por Id FNB...")

    # Identificar registros con Id FNB válido
    mask_id_fnb_valido = (
            df_sap_trabajo['Id FNB'].notna() &
            (df_sap_trabajo['Id FNB'].astype(str).str.strip() != '') &
            (df_sap_trabajo['Id FNB'].astype(str).str.strip() != 'nan')
    )

    registros_con_id_fnb = mask_id_fnb_valido.sum()
    print(f"📊 Registros con Id FNB válido: {registros_con_id_fnb}")

    if registros_con_id_fnb > 0:
        # Crear diccionario para mapeo directo por Id FNB
        id_fnb_to_canal = df_canal_pedido.set_index('Nro. PEDIDO VENTA')['CANAL_VENTA'].to_dict()

        # Aplicar el mapeo solo a registros con Id FNB válido
        df_sap_trabajo.loc[mask_id_fnb_valido, 'CANAL_VENTA'] = (
            df_sap_trabajo.loc[mask_id_fnb_valido, 'Id FNB'].map(id_fnb_to_canal)
            .fillna('Canal de venta no identificado')
        )

        # Contar cuántos encontraron canal por Id FNB
        canales_por_id_fnb = (
                df_sap_trabajo.loc[mask_id_fnb_valido, 'CANAL_VENTA'] != 'Canal de venta no identificado'
        ).sum()
        print(f"✅ Canales encontrados por Id FNB: {canales_por_id_fnb}")

    # PASO 2: Para los que no encontraron canal, intentar por Contrato
    print("🔄 Paso 2: Cruce por Contrato para registros sin canal...")

    # Identificar registros que aún no tienen canal Y tienen Contrato válido
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
        # Crear diccionario para mapeo directo por Contrato
        contrato_to_canal = df_canal_contrato.set_index('Nro. DE CONTRATO')['CANAL_VENTA'].to_dict()

        # Aplicar el mapeo
        df_sap_trabajo.loc[mask_para_segundo_cruce, 'CANAL_VENTA'] = (
            df_sap_trabajo.loc[mask_para_segundo_cruce, 'Contrato'].map(contrato_to_canal)
            .fillna('Canal de venta no identificado')
        )

        # Contar cuántos encontraron canal por Contrato
        canales_por_contrato = (
                df_sap_trabajo.loc[mask_para_segundo_cruce, 'CANAL_VENTA'] != 'Canal de venta no identificado'
        ).sum()
        print(f"✅ Canales encontrados por Contrato: {canales_por_contrato}")

    # PASO 3: Estadísticas finales
    total_canales_identificados = (df_sap_trabajo['CANAL_VENTA'] != 'Canal de venta no identificado').sum()
    total_canales_no_identificados = (df_sap_trabajo['CANAL_VENTA'] == 'Canal de venta no identificado').sum()

    print(f"📊 Resumen final:")
    print(f"   • Canales identificados: {total_canales_identificados}")
    print(f"   • Canales no identificados: {total_canales_no_identificados}")
    print(f"   • Total registros procesados: {len(df_sap_trabajo)}")

    # PASO 4: Mostrar distribución por canal
    print(f"📊 Distribución por canal:")
    canal_counts = df_sap_trabajo['CANAL_VENTA'].value_counts()
    for canal, count in canal_counts.items():
        print(f"   • {canal}: {count}")

    return df_sap_trabajo


# Verificar datos SAP antes del cruce
print("🔍 Verificando datos SAP antes del cruce...")
estadisticas_sap = verificar_datos_sap(df_sap)
print("\n" + "=" * 50)

# Realizar el cruce
df_sap_con_canal = obtener_canal_venta(df_sap, df_procesado)

# NUEVO: Aplicar exoneraciones ANTES de crear los escenarios
print("\n" + "=" * 50)
df_sap_con_canal, total_exonerados = aplicar_exoneraciones(df_sap_con_canal, registros_exonerados)
print("=" * 50)

# CAMBIO PRINCIPAL: Obtener fechas para el nombre del archivo (fecha actual menos 1 día)
fecha_reporte = datetime.now() - timedelta(days=1)
fecha_actual = fecha_reporte.strftime('%d-%m-%Y')
fecha_actual_texto = fecha_reporte.strftime('%d/%m/%Y')

print(f"📅 Fecha del reporte: {fecha_actual} ({fecha_actual_texto})")


def formatear_df_sap(df_filtrado):
    """
    Formatea el DataFrame para exportación, adaptado para el reporte SAP
    """
    df_filtrado = df_filtrado.copy()

    # Formatear fechas si existen
    fecha_columns = ['Fecha Venta SAP', 'Fecha Venta FNB', 'Fecha Entrega SAP', 'Fecha Entrega FNB']
    for col in fecha_columns:
        if col in df_filtrado.columns:
            df_filtrado[col] = pd.to_datetime(df_filtrado[col], errors='coerce').dt.strftime('%d/%m/%Y').fillna("")

    return df_filtrado


def exportar_excel_sap(df_filtrado, ruta, nombre_hoja='Incidencias SAP'):
    """
    Exporta DataFrame a Excel con formato específico para reporte SAP
    """
    with pd.ExcelWriter(ruta, engine='xlsxwriter') as writer:
        df_filtrado.to_excel(writer, index=False, sheet_name=nombre_hoja)
        workbook: Workbook = writer.book
        worksheet = writer.sheets[nombre_hoja]

        # Formatos
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

        # Configurar encabezados
        worksheet.set_default_row(11.25)
        for col_num, value in enumerate(df_filtrado.columns):
            worksheet.write(0, col_num, value, header_format)
            max_len = max(df_filtrado[value].astype(str).map(len).max(), len(value))
            worksheet.set_column(col_num, col_num, min(max_len + 2, 50))

        # Escribir datos
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


def verificar_estados_equivalentes(estado_sap, estado_fnb):
    """
    Verifica si los estados de SAP y FNB son equivalentes
    CORREGIDO: Si ambos están vacíos, se consideran equivalentes
    """
    # Si ambos están vacíos o son NaN, se consideran equivalentes
    sap_vacio = pd.isna(estado_sap) or estado_sap == '' or (isinstance(estado_sap, str) and estado_sap.strip() == '')
    fnb_vacio = pd.isna(estado_fnb) or estado_fnb == '' or (isinstance(estado_fnb, str) and estado_fnb.strip() == '')

    if sap_vacio and fnb_vacio:
        return True

    # Si solo uno está vacío, no son equivalentes
    if sap_vacio or fnb_vacio:
        return False

    estado_sap = str(estado_sap).upper().strip()
    estado_fnb = str(estado_fnb).upper().strip()

    # Equivalencias definidas
    equivalencias = {
        'CONCLUIDO': ['ENTREGADO', 'PENDIENTE DE ANULACIÓN'],
        'EN TRATAMIENTO': ['PENDIENTE DE ENTREGA', 'ERROR DE INTEGRACIÓN', 'PENDIENTE DE ANULACIÓN',
                           'PENDIENTE DE APROBACIÓN'],
        'RECHAZADO': ['ANULADO', 'ANULADO POR CRÉDITO']
    }

    for sap_estado, fnb_estados in equivalencias.items():
        if estado_sap == sap_estado and estado_fnb in fnb_estados:
            return True

    return False


def tiene_datos(valor):
    """
    Verifica si un valor tiene datos (no está vacío, no es NaN)
    """
    return pd.notna(valor) and str(valor).strip() != ''


# Definir escenarios de incidencias
print("📋 Definiendo escenarios de incidencias...")


def crear_escenarios(df):
    """
    Crea los diferentes escenarios de incidencias
    """
    todos_los_escenarios = []

    # 1. Fecha de Venta diferente entre SAP y FNB
    df_fecha_venta = df[df['Mensaje comparativa F Venta'].notna() & (df['Mensaje comparativa F Venta'] != '')].copy()
    todos_los_escenarios.append(("Fecha de Venta diferente entre SAP y FNB", df_fecha_venta))

    # 2. Fecha de Entrega diferente entre SAP y FNB
    df_fecha_entrega = df[
        df['Mensaje comparativa F Entega'].notna() & (df['Mensaje comparativa F Entega'] != '')].copy()
    todos_los_escenarios.append(("Fecha de Entrega diferente entre SAP y FNB", df_fecha_entrega))

    # 3. Responsable de Venta diferente entre SAP y FNB
    df_responsable = df[
        df['Mensaje comparativa Responsable'].notna() & (df['Mensaje comparativa Responsable'] != '')].copy()
    todos_los_escenarios.append(("Responsable de Venta diferente entre SAP y FNB", df_responsable))

    # 4. Aliado Comercial (Proveedor) diferente entre SAP y FNB
    df_aliado = df[df['Mensaje comparativa Aliado'].notna() & (df['Mensaje comparativa Aliado'] != '')].copy()
    todos_los_escenarios.append(("Aliado Comercial (Proveedor) diferente entre SAP y FNB", df_aliado))

    # 5. Sede diferente entre SAP y FNB
    df_sede = df[df['Mensaje comparativa Sede'].notna() & (df['Mensaje comparativa Sede'] != '')].copy()
    todos_los_escenarios.append(("Sede diferente entre SAP y FNB", df_sede))

    # 6. Importe Financiado diferente entre SAP y FNB
    df_importe = df[df['Mensaje comparativa Importe'].notna() & (df['Mensaje comparativa Importe'] != '')].copy()
    todos_los_escenarios.append(("Importe Financiado diferente entre SAP y FNB", df_importe))

    # 7. Nro. de Cuotas diferentes entre SAP y FNB
    df_cuotas = df[df['Mensaje comparativa Cuotas'].notna() & (df['Mensaje comparativa Cuotas'] != '')].copy()
    todos_los_escenarios.append(("Nro. de Cuotas diferentes entre SAP y FNB", df_cuotas))

    # 8. Estados de Entrega diferentes entre SAP y FNB
    df_estados_diferentes = df[
        ~df.apply(lambda row: verificar_estados_equivalentes(row['Estado SAP'], row['Estado FNB']), axis=1)].copy()
    todos_los_escenarios.append(("Estados de Entrega diferentes entre SAP y FNB", df_estados_diferentes))

    # 9. Nro de Contrato CD diferente entre SAP y FNB
    df_contrato_general = df[
        df['Mensaje comparativa Contrato'].notna() & (df['Mensaje comparativa Contrato'] != '')].copy()

    # 9a. Casos por regularizar (Tienda Virtual)
    condicion_tienda_virtual = (
            df_contrato_general['Contrato SAP'].notna() &
            (df_contrato_general['Contrato SAP'] != '') &
            (df_contrato_general['Contrato'].isna() | (df_contrato_general['Contrato'] == '')) &
            (df_contrato_general['Nombre Responsable SAP'].str.upper() == 'TIENDA VIRTUAL WEB')
    )
    df_contrato_tienda_virtual = df_contrato_general[condicion_tienda_virtual].copy()
    todos_los_escenarios.append(
        ("Nro de Contrato CD - Casos por regularizar el Nro. Contrato CD de la tienda virtual en el reporte de la plataforma FNB",
         df_contrato_tienda_virtual))

    # 9b. Casos regulares detectados
    df_contrato_regulares = df_contrato_general[~condicion_tienda_virtual].copy()
    todos_los_escenarios.append(("Nro de Contrato CD - Casos regulares detectados", df_contrato_regulares))

    # 10. Transacciones FNB que no figuran en SAP
    # Id FNB tiene datos pero Numero pedido SAP no tiene datos
    df_fnb_no_sap = df[
        df['Id FNB'].apply(tiene_datos) &
        ~df['Numero pedido SAP'].apply(tiene_datos)
        ].copy()
    todos_los_escenarios.append(("Transacciones FNB que no figuran en SAP", df_fnb_no_sap))

    # 11. Transacciones SAP que no figuran en FNB
    # Numero pedido SAP tiene datos pero Id FNB no tiene datos
    df_sap_no_fnb = df[
        df['Numero pedido SAP'].apply(tiene_datos) &
        ~df['Id FNB'].apply(tiene_datos)
        ].copy()
    todos_los_escenarios.append(("Transacciones SAP que no figuran en FNB", df_sap_no_fnb))

    return todos_los_escenarios


# Procesar escenarios - REMOVIDO: la función de seleccionar escenarios
print("🔄 Procesando escenarios...")
todos_los_escenarios = crear_escenarios(df_sap_con_canal)

archivos = []
resumen_html = "<b>PROYECTOS</b><br><br>"

for nombre_escenario, df_filtrado in todos_los_escenarios:
    try:
        print(f"\n🔄 Procesando: {nombre_escenario}")

        nombre_archivo = f"{nombre_escenario} - {fecha_actual}.xlsx"
        ruta_archivo = os.path.join(ruta_salida, nombre_archivo)

        if not df_filtrado.empty:
            df_filtrado_formateado = formatear_df_sap(df_filtrado)
            exportar_excel_sap(df_filtrado_formateado, ruta_archivo)
            archivos.append(ruta_archivo)
            conteo = len(df_filtrado)

            # Crear resumen HTML - AJUSTADO: sin numeración, con guiones, sin listas HTML
            resumen = f"<b>{nombre_escenario}</b>: <b>{conteo}</b> registros detectados<br>"

            # Agregar información por canal de venta
            if 'CANAL_VENTA' in df_filtrado.columns:
                canal_counts = df_filtrado['CANAL_VENTA'].fillna('Canal de venta no identificado').value_counts()
                for canal, count in canal_counts.items():
                    resumen += f"<span style='font-size: 10pt;'>- {canal}: {count}</span><br>"
            else:
                resumen += f"<span style='font-size: 10pt;'>- Información de canal no disponible</span><br>"
            
            resumen += "<br>"  # Salto de línea adicional entre escenarios
        else:
            # AJUSTADO: Solo agregar al resumen si NO hay registros detectados (exonerar casos sin registros)
            continue

        resumen_html += resumen
        print(f"✅ Procesado: {nombre_escenario} - {len(df_filtrado) if not df_filtrado.empty else 0} registros")

    except Exception as e:
        print(f"❌ Error procesando {nombre_escenario}: {str(e)}")

# Verificar si hay archivos para enviar
if not archivos:
    print("⚠️  No se generaron archivos. No hay registros para reportar.")
    exit()

# Enviar correos
print("\n📧 Preparando envío de correos...")
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

            # Adjuntar archivos generados
            for archivo in archivos:
                mail.Attachments.Add(archivo)

            # Adjuntar firma si existe
            if os.path.exists(firma_path):
                attach = mail.Attachments.Add(firma_path)
                attach.PropertyAccessor.SetProperty(
                    "http://schemas.microsoft.com/mapi/proptag/0x3712001F", "firmaimg"
                )

            mail.HTMLBody = cuerpo
            mail.Display()
            print(f"📧 Correo preparado para: {row['Destinatarios directos']}")

        except Exception as e:
            print(f"❌ Error preparando correo para {row['Destinatarios directos']}: {str(e)}")

    print("🎉 Proceso de preparación de correos completado.")

except Exception as e:
    print(f"❌ Error en el proceso de correos: {str(e)}")

print("\n📊 Resumen del proceso:")
print(f"✅ Archivos generados: {len(archivos)}")
total_registros = sum(len(df_filtrado) for nombre_escenario, df_filtrado in todos_los_escenarios if not df_filtrado.empty)
print(f"✅ Registros procesados en total: {total_registros}")
if total_exonerados > 0:
    print(f"🚫 Registros exonerados: {total_exonerados}")
print("🎉 Proceso completado exitosamente.")
import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from datetime import datetime, time as dt_time
import warnings
import webbrowser
import base64
from io import BytesIO
import asyncio
import zipfile
from importlib.machinery import SourceFileLoader

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')


class SalesTransformer:
    def __init__(self):
        self.ruta_canal_fija = r"D:\FNB\Reportes\19. Reportes IBR\Archivos comunes\Canal\Canal.xlsx"
        self.columnas_producto = [
            "PRODUCTO", "SKU", "CANTIDAD", "PRECIO", "CATEGORIA", "MARCA", "SUBCANAL",
            "CATEGORIA REAL", "TIPO PRODUCTO", "MODELO PRODUCTO", "SKU2", "DESCRIPCION"
        ]
        self.mapeo_canales = self._cargar_mapeo_canales()
        self.rangos_hora = [f"{h:02d}:{m:02d} - {(h + (m + 30) // 60) % 24:02d}:{(m + 30) % 60:02d}"
                            for h in range(24) for m in range(0, 60, 30)]
        self._cache_rango_hora = {}

    def _cargar_mapeo_canales(self):
        try:
            df_canal = pd.read_excel(self.ruta_canal_fija, sheet_name='Hoja1')
            mapeo = pd.Series(df_canal.iloc[:, 2].values,
                              index=df_canal.iloc[:, 0].astype(str).str.strip().str.upper()).to_dict()
            return mapeo
        except Exception as e:
            print(f"Error cargando Canal.xlsx: {e}")
            return {}

    def _procesar_hora_venta(self, df):
        """Procesa la columna HORA VENTA con múltiples formatos"""
        print("🔍 Procesando horas...")

        formatos_hora = ['%H:%M:%S', '%H:%M', '%I:%M:%S %p', '%I:%M %p']
        hora_procesada = None

        for formato in formatos_hora:
            try:
                hora_temp = pd.to_datetime(df['HORA VENTA'], format=formato, errors='coerce')
                horas_validas = hora_temp.notna().sum()

                if horas_validas > 0:
                    hora_procesada = hora_temp
                    print(f"   ✅ Formato exitoso: {formato} ({horas_validas} horas válidas)")
                    break
            except:
                continue

        if hora_procesada is None or hora_procesada.notna().sum() == 0:
            print("   ⚠️ Asignando horas por defecto...")
            n_registros = len(df)
            horas_default = []

            for i in range(n_registros):
                minutos_del_dia = 480 + (i * 720 // n_registros)
                hora = minutos_del_dia // 60
                minuto = minutos_del_dia % 60
                horas_default.append(dt_time(hour=hora % 24, minute=minuto))

            df['HORA VENTA'] = horas_default
        else:
            df['HORA VENTA'] = hora_procesada.dt.time

        return df

    def _asignar_rango_hora(self, series_hora: pd.Series) -> pd.Series:
        def get_rango(hora):
            if pd.isna(hora) or hora is None:
                return "08:00 - 08:30"

            hora_str = str(hora)
            if hora_str in self._cache_rango_hora:
                return self._cache_rango_hora[hora_str]

            try:
                if isinstance(hora, str):
                    if ':' in hora:
                        hora_obj = datetime.strptime(hora, '%H:%M:%S').time()
                    else:
                        return "08:00 - 08:30"
                elif isinstance(hora, dt_time):
                    hora_obj = hora
                else:
                    return "08:00 - 08:30"

                minutos = hora_obj.hour * 60 + hora_obj.minute
                idx = min(minutos // 30, len(self.rangos_hora) - 1)
                resultado = self.rangos_hora[idx]
                self._cache_rango_hora[hora_str] = resultado
                return resultado
            except:
                return "08:00 - 08:30"

        return series_hora.apply(get_rango)

    def _determinar_canal_venta(self, df: pd.DataFrame) -> pd.Series:
        responsable = df['RESPONSABLE DE VENTA'].astype(str).str.strip().str.upper()
        aliado = df['ALIADO COMERCIAL'].astype(str).str.strip().str.upper()

        try:
            fecha_venta = pd.to_datetime(df['FECHA VENTA'], format='%d/%m/%Y', errors='coerce', dayfirst=True)
        except:
            fecha_venta = pd.to_datetime(df['FECHA VENTA'], errors='coerce', dayfirst=True)

        sede = df['SEDE'].astype(str).str.strip().str.upper()
        categoria = df.get('CATEGORIA_1', pd.Series([''] * len(df))).astype(str).str.strip().str.upper()

        canal = pd.Series([''] * len(df), index=df.index)
        fecha_limite = pd.to_datetime('2024-02-01', format='%Y-%m-%d')
        fecha_limite_1 = pd.to_datetime('2025-08-01', format='%Y-%m-%d')

        cond_retail = (fecha_venta >= fecha_limite) & (responsable.isin(["CONECTA RETAIL", "INTEGRA RETAIL"]))
        cond_retail_1 = (fecha_venta >= fecha_limite_1) & (responsable.isin(["TOPITOP"]))
        cond_materiales = (
            (categoria == "MATERIALES Y ACABADOS DE CONSTRUCCIÓN") &
            (~responsable.isin(["A & G INGENIERIA", "INCOSER GAS PERU S.A.C.", "PROMART"]))
        )
        cond_motos = (
            categoria.isin(["MOTOS", "MOTOS ELECTRICAS", "ACCESORIOS MOTOS"]) &
            (~responsable.isin(["CONECTA RETAIL", "INTEGRA RETAIL"]))
        )
        cond_merpes = (aliado == "GRUPO MERPES") & (categoria == "MUEBLES")

        canal.loc[cond_retail] = "RETAIL"
        canal.loc[cond_retail_1] = "RETAIL"
        canal.loc[cond_materiales] = "MATERIALES Y ACABADOS DE CONSTRUCCIÓN"
        canal.loc[cond_motos] = "MOTOS"
        canal.loc[cond_merpes] = "MATERIALES Y ACABADOS DE CONSTRUCCIÓN"

        mask_sin_canal = canal == ''
        canal.loc[mask_sin_canal] = sede.loc[mask_sin_canal].map(self.mapeo_canales).fillna('')
        canal = canal.replace('CHATBOT', 'DIGITAL')
        return canal


    def procesar_archivo(self, ruta_archivo: str) -> pd.DataFrame:
        """Procesa un archivo Excel de ventas"""
        print(f"-> Cargando: {os.path.basename(ruta_archivo)}")
        df = pd.read_excel(ruta_archivo, engine='openpyxl')

        # APLICAR FILTRO POR ESTADO - NUEVO
        estados_validos = ['PENDIENTE DE ENTREGA', 'ENTREGADO', 'PENDIENTE DE APROBACIÓN']
        
        if 'ESTADO' in df.columns:
            registros_antes = len(df)
            df = df[df['ESTADO'].isin(estados_validos)].copy()
            registros_despues = len(df)
            print(f"   🔍 Filtro ESTADO aplicado: {registros_antes} → {registros_despues} registros")
            print(f"   ✅ Estados incluidos: {', '.join(estados_validos)}")
        else:
            print("   ⚠️ Columna ESTADO no encontrada, continuando sin filtro")

        # Procesar horas
        df = self._procesar_hora_venta(df)

        # Identificar transacciones únicas
        columnas_disponibles = df.columns.tolist()
        columnas_g2 = [col for col in self.columnas_producto if col in columnas_disponibles]
        columnas_g1 = [col for col in columnas_disponibles if col not in columnas_g2]

        df['codigo_unico'] = pd.util.hash_pandas_object(df[columnas_g1].astype(str).fillna(''), index=False)
        df_transacciones = df.drop_duplicates('codigo_unico').copy()
        df_transacciones['RANGO HORA'] = self._asignar_rango_hora(df_transacciones['HORA VENTA'])

        # Obtener categoría del primer producto
        df_productos = df[['codigo_unico'] + columnas_g2].copy()
        df_productos['producto_idx'] = df_productos.groupby('codigo_unico').cumcount() + 1
        df_producto_1 = df_productos[df_productos['producto_idx'] == 1].add_suffix('_1')

        df_final = pd.merge(df_transacciones, df_producto_1, left_on='codigo_unico', right_on='codigo_unico_1',
                            how='left')

        # Asignar canal
        df_final['CANAL_VENTA'] = self._determinar_canal_venta(df_final)
        df_final['CANAL_VENTA'] = df_final['CANAL_VENTA'].fillna("NO IDENTIFICADO")
        df_final.loc[df_final['CANAL_VENTA'] == '', 'CANAL_VENTA'] = "NO IDENTIFICADO"

        print(f"   ✅ {len(df_final)} transacciones procesadas")
        return df_final


def extraer_fecha_nombre(ruta_archivo):
    """Extrae fecha del nombre del archivo con mejor detección"""
    import re
    nombre = os.path.basename(ruta_archivo)

    # Patrones de fecha más específicos
    patrones = [
        r'(\d{1,2}[-/]\d{1,2}[-/]\d{4})',  # dd/mm/yyyy o dd-mm-yyyy
        r'(\d{4}[-/]\d{1,2}[-/]\d{1,2})',  # yyyy/mm/dd o yyyy-mm-dd
        r'(\d{1,2}[-/]\d{1,2}[-/]\d{2})',  # dd/mm/yy o dd-mm-yy
    ]

    for patron in patrones:
        match = re.search(patron, nombre)
        if match:
            fecha_str = match.group(1)
            # Convertir guiones a barras para consistencia
            return fecha_str.replace('-', '/')

    # Si no encuentra fecha, intentar extraer de fecha de modificación del archivo
    try:
        timestamp = os.path.getmtime(ruta_archivo)
        fecha_mod = datetime.fromtimestamp(timestamp)
        return fecha_mod.strftime('%d/%m/%Y')
    except:
        return "Sin fecha"


def determinar_hora_corte():
    """Determina la lógica del corte de hora según la hora actual"""
    hora_actual = datetime.now().time()

    # Si es entre 12:00 y 23:59, usar hora máxima del archivo nuevo
    if hora_actual >= dt_time(12, 0):
        return "nuevo"
    # Si es entre 00:00 y 11:59, usar la hora mayor entre ambos archivos
    else:
        return "mayor"


def crear_tablas_detalladas(df_anterior_filtrado, df_nuevo_filtrado, col_importe, fecha_anterior, fecha_nueva):
    """Crear tablas detalladas dinámicas por canal con orden específico"""
    
    # Configuración de agrupación por canal
    config_canales = {
        'ALO CÁLIDDA': ['ASESOR DE VENTAS'],  # Cambiado de 'ASESOR DE VENTA' a 'SEDE'
        'CSC': ['SEDE'],
        'DIGITAL': ['SEDE'],
        'TIENDAS CÁLIDDA': ['SEDE'],
        'RETAIL': ['ALIADO COMERCIAL', 'SEDE'],
        'MOTOS': ['ALIADO COMERCIAL', 'SEDE'],
        'GRANDES SUPERFICIES': ['ALIADO COMERCIAL', 'SEDE'],
        'MATERIALES Y ACABADOS DE CONSTRUCCIÓN': ['ALIADO COMERCIAL', 'SEDE'],
        'CANAL PROVEEDOR': ['ALIADO COMERCIAL', 'SEDE'],
        'FFVV - PUERTA A PUERTA': ['ALIADO COMERCIAL', 'SEDE'],
        # Se pueden agregar más canales dinámicamente
    }
    
    # Debug: Mostrar canales disponibles
    print(f"\n🔍 DEBUG - Canales disponibles:")
    if not df_anterior_filtrado.empty:
        print(f"   Anterior: {sorted(df_anterior_filtrado['CANAL_VENTA'].unique())}")
    if not df_nuevo_filtrado.empty:
        print(f"   Nuevo: {sorted(df_nuevo_filtrado['CANAL_VENTA'].unique())}")
    
    # Orden específico para mostrar los canales
    orden_canales = [
        'ALO CÁLIDDA',
        'CSC', 
        'DIGITAL',
        'TIENDAS CÁLIDDA',
        'RETAIL',
        'MOTOS',
        'GRANDES SUPERFICIES',
        'MATERIALES Y ACABADOS DE CONSTRUCCIÓN',
        'CANAL PROVEEDOR',
        'FFVV - PUERTA A PUERTA'
    ]
    
    tablas_html = {}
    
    # Obtener todos los canales únicos de ambos dataframes
    canales_anteriores = set(df_anterior_filtrado['CANAL_VENTA'].unique()) if not df_anterior_filtrado.empty else set()
    canales_nuevos = set(df_nuevo_filtrado['CANAL_VENTA'].unique()) if not df_nuevo_filtrado.empty else set()
    todos_canales = canales_anteriores.union(canales_nuevos)
    
    # Procesar canales en orden específico primero
    for canal in orden_canales:
        print(f"\n🔍 Procesando canal: {canal}")
        print(f"   ¿Está en todos_canales? {canal in todos_canales}")
        
        if canal in todos_canales:
            # Filtrar datos por canal
            df_ant_canal = df_anterior_filtrado[df_anterior_filtrado['CANAL_VENTA'] == canal] if not df_anterior_filtrado.empty else pd.DataFrame()
            df_nue_canal = df_nuevo_filtrado[df_nuevo_filtrado['CANAL_VENTA'] == canal] if not df_nuevo_filtrado.empty else pd.DataFrame()
            
            print(f"   Registros anteriores: {len(df_ant_canal)}")
            print(f"   Registros nuevos: {len(df_nue_canal)}")
            
            if df_ant_canal.empty and df_nue_canal.empty:
                print(f"   ⚠️ Sin datos para {canal}")
                continue
                
            # Determinar columnas de agrupación
            if canal in config_canales:
                columnas_grupo = config_canales[canal]
                print(f"   Columnas configuradas: {columnas_grupo}")
            else:
                # Para canales no configurados, usar SEDE por defecto
                columnas_grupo = ['SEDE']
                print(f"   Usando columnas por defecto: {columnas_grupo}")
                
            # Verificar que las columnas existen en los datos
            columnas_disponibles_ant = df_ant_canal.columns.tolist() if not df_ant_canal.empty else []
            columnas_disponibles_nue = df_nue_canal.columns.tolist() if not df_nue_canal.empty else []
            columnas_disponibles = set(columnas_disponibles_ant + columnas_disponibles_nue)
            
            columnas_grupo = [col for col in columnas_grupo if col in columnas_disponibles]
            print(f"   Columnas disponibles: {columnas_grupo}")
            
            if not columnas_grupo:
                print(f"   ❌ No hay columnas válidas para {canal}")
                continue
                
            # Crear tabla detallada
            tabla_html = crear_tabla_canal(df_ant_canal, df_nue_canal, canal, columnas_grupo, col_importe, fecha_anterior, fecha_nueva)
            if tabla_html:
                tablas_html[canal] = tabla_html
                print(f"   ✅ Tabla creada para {canal}")
            else:
                print(f"   ❌ No se pudo crear tabla para {canal}")
        else:
            print(f"   ❌ Canal {canal} no encontrado en los datos")
    
    # Procesar canales restantes que no están en el orden específico
    for canal in todos_canales:
        if canal not in orden_canales and canal not in ['NO IDENTIFICADO', '']:
            # Filtrar datos por canal
            df_ant_canal = df_anterior_filtrado[df_anterior_filtrado['CANAL_VENTA'] == canal] if not df_anterior_filtrado.empty else pd.DataFrame()
            df_nue_canal = df_nuevo_filtrado[df_nuevo_filtrado['CANAL_VENTA'] == canal] if not df_nuevo_filtrado.empty else pd.DataFrame()
            
            if df_ant_canal.empty and df_nue_canal.empty:
                continue
                
            # Para canales no configurados, usar SEDE por defecto
            columnas_grupo = ['SEDE']
                
            # Verificar que las columnas existen en los datos
            columnas_disponibles_ant = df_ant_canal.columns.tolist() if not df_ant_canal.empty else []
            columnas_disponibles_nue = df_nue_canal.columns.tolist() if not df_nue_canal.empty else []
            columnas_disponibles = set(columnas_disponibles_ant + columnas_disponibles_nue)
            
            columnas_grupo = [col for col in columnas_grupo if col in columnas_disponibles]
            
            if not columnas_grupo:
                continue
                
            # Crear tabla detallada
            tabla_html = crear_tabla_canal(df_ant_canal, df_nue_canal, canal, columnas_grupo, col_importe, fecha_anterior, fecha_nueva)
            if tabla_html:
                tablas_html[canal] = tabla_html
    
    return tablas_html


def crear_tabla_canal(df_anterior, df_nuevo, canal, columnas_grupo, col_importe, fecha_anterior, fecha_nueva):
    """Crear tabla HTML para un canal específico con agrupaciones dinámicas"""
    
    def procesar_dataframe(df, sufijo):
        if df.empty:
            return pd.DataFrame()
            
        # Si hay una sola columna de agrupación
        if len(columnas_grupo) == 1:
            grupo = df.groupby(columnas_grupo[0]).agg(
                Importe=(col_importe, 'sum'),
                Transacciones=('codigo_unico', 'nunique')
            )
            grupo.columns = [f'Importe_{sufijo}', f'Transacciones_{sufijo}']
            return grupo
            
        # Si hay múltiples columnas (como RETAIL y otros con ALIADO + SEDE)
        else:
            # Crear agrupación jerárquica
            grupo = df.groupby(columnas_grupo).agg(
                Importe=(col_importe, 'sum'),
                Transacciones=('codigo_unico', 'nunique')
            )
            grupo.columns = [f'Importe_{sufijo}', f'Transacciones_{sufijo}']
            return grupo
    
    # Procesar ambos períodos
    tabla_anterior = procesar_dataframe(df_anterior, 'Anterior')
    tabla_nuevo = procesar_dataframe(df_nuevo, 'Nuevo')
    
    if tabla_anterior.empty and tabla_nuevo.empty:
        return None
        
    # Combinar tablas
    if tabla_anterior.empty:
        tabla_combinada = tabla_nuevo.copy()
        tabla_combinada[f'Importe_Anterior'] = 0
        tabla_combinada[f'Transacciones_Anterior'] = 0
    elif tabla_nuevo.empty:
        tabla_combinada = tabla_anterior.copy()
        tabla_combinada[f'Importe_Nuevo'] = 0
        tabla_combinada[f'Transacciones_Nuevo'] = 0
    else:
        tabla_combinada = pd.merge(tabla_anterior, tabla_nuevo, left_index=True, right_index=True, how='outer').fillna(0)
    
    # Calcular variación
    tabla_combinada['Variacion_Importe'] = tabla_combinada[f'Importe_Nuevo'] - tabla_combinada[f'Importe_Anterior']
    
    # Determinar si necesita funcionalidad expandir/contraer
    canales_expandibles = ['RETAIL', 'CANAL PROVEEDOR', 'GRANDES SUPERFICIES', 'FFVV - PUERTA A PUERTA', 
                          'MATERIALES Y ACABADOS DE CONSTRUCCIÓN', 'MOTOS']
    es_expandible = canal in canales_expandibles and len(columnas_grupo) > 1
    
    # Generar HTML de la tabla
    html = f"""
    <div class="canal-section">
        <div class="canal-header">
            <h3 class="canal-title">📊 {canal}</h3>
        </div>
        <div class="table-container">
            <div class="table-responsive">
                <table class="detail-table">
                    <thead>
                        <tr>
    """
    
    # Cabeceras dinámicas según columnas de agrupación
    for col in columnas_grupo:
        html += f"<th>{col}</th>"
    
    html += f"""
                            <th>Importe {fecha_anterior}</th>
                            <th>Trans. {fecha_anterior}</th>
                            <th>Importe {fecha_nueva}</th>
                            <th>Trans. {fecha_nueva}</th>
                            <th>Variación</th>
                        </tr>
                    </thead>
                    <tbody>
    """
    
    # Si hay agrupación jerárquica (múltiples columnas) con funcionalidad expandible
    if len(columnas_grupo) > 1:
        # Agrupar por primera columna para crear subtotales
        primer_nivel = tabla_combinada.groupby(level=0)
        
        aliado_counter = 0
        for grupo_nombre, grupo_data in primer_nivel:
            aliado_counter += 1
            
            # Fila de subtotal del grupo (expandible)
            subtotal_importe_ant = grupo_data[f'Importe_Anterior'].sum()
            subtotal_trans_ant = grupo_data[f'Transacciones_Anterior'].sum()
            subtotal_importe_nue = grupo_data[f'Importe_Nuevo'].sum()
            subtotal_trans_nue = grupo_data[f'Transacciones_Nuevo'].sum()
            subtotal_variacion = subtotal_importe_nue - subtotal_importe_ant
            
            # ID único para esta sección
            section_id = f"{canal.replace(' ', '_').replace('-', '_')}_{aliado_counter}"
            
            # Determinar clase de variación para subtotales
            clase_variacion_subtotal = 'positive' if subtotal_variacion > 0 else 'negative' if subtotal_variacion < 0 else ''
            
            if es_expandible:
                html += f"""
                        <tr class="subtotal-row clickable-row" onclick="toggleSection('{section_id}')">
                            <td>
                                <div class="expand-control">
                                    <span class="expand-icon" id="icon_{section_id}">▼</span>
                                    <strong>{grupo_nombre}</strong>
                                </div>
                            </td>
                            <td colspan="{len(columnas_grupo)-1}"></td>
                            <td><strong>S/ {subtotal_importe_ant:,.0f}</strong></td>
                            <td><strong>{subtotal_trans_ant:,.0f}</strong></td>
                            <td><strong>S/ {subtotal_importe_nue:,.0f}</strong></td>
                            <td><strong>{subtotal_trans_nue:,.0f}</strong></td>
                            <td class="{clase_variacion_subtotal}"><strong>S/ {subtotal_variacion:+,.0f}</strong></td>
                        </tr>
                """
            else:
                html += f"""
                        <tr class="subtotal-row">
                            <td><strong>{grupo_nombre}</strong></td>
                            <td colspan="{len(columnas_grupo)-1}"></td>
                            <td><strong>S/ {subtotal_importe_ant:,.0f}</strong></td>
                            <td><strong>{subtotal_trans_ant:,.0f}</strong></td>
                            <td><strong>S/ {subtotal_importe_nue:,.0f}</strong></td>
                            <td><strong>{subtotal_trans_nue:,.0f}</strong></td>
                            <td class="{clase_variacion_subtotal}"><strong>S/ {subtotal_variacion:+,.0f}</strong></td>
                        </tr>
                """
            
            # Filas de detalle del grupo (colapsables si es expandible)
            for idx, row in grupo_data.iterrows():
                valores_idx = list(idx) if isinstance(idx, tuple) else [idx]
                
                clase_detalle = f"detail-row-{section_id}" if es_expandible else ""
                style_detalle = 'style="display: none;"' if es_expandible else ""
                
                # Determinar clase de variación para filas de detalle
                clase_variacion_detalle = 'positive' if row['Variacion_Importe'] > 0 else 'negative' if row['Variacion_Importe'] < 0 else ''
                
                html += f'<tr class="{clase_detalle}" {style_detalle}>'
                
                # Primera columna vacía (ya mostrada en subtotal)
                html += "<td class='detail-indent'></td>"
                
                # Resto de columnas
                for i, valor in enumerate(valores_idx[1:], 1):
                    html += f"<td class='detail-cell'>{valor}</td>"
                
                html += f"""
                        <td class='detail-cell'>S/ {row[f'Importe_Anterior']:,.0f}</td>
                        <td class='detail-cell'>{row[f'Transacciones_Anterior']:,.0f}</td>
                        <td class='detail-cell'>S/ {row[f'Importe_Nuevo']:,.0f}</td>
                        <td class='detail-cell'>{row[f'Transacciones_Nuevo']:,.0f}</td>
                        <td class="detail-cell {clase_variacion_detalle}">S/ {row['Variacion_Importe']:+,.0f}</td>
                    </tr>
                """
    else:
        # Agrupación simple (una columna)
        for idx, row in tabla_combinada.iterrows():
            clase_variacion = 'positive' if row['Variacion_Importe'] > 0 else 'negative' if row['Variacion_Importe'] < 0 else ''
            html += f"""
                    <tr>
                        <td>{idx}</td>
                        <td>S/ {row[f'Importe_Anterior']:,.0f}</td>
                        <td>{row[f'Transacciones_Anterior']:,.0f}</td>
                        <td>S/ {row[f'Importe_Nuevo']:,.0f}</td>
                        <td>{row[f'Transacciones_Nuevo']:,.0f}</td>
                        <td class="{clase_variacion}">S/ {row['Variacion_Importe']:+,.0f}</td>
                    </tr>
            """
    
    # Fila de total con color plomo claro
    total_importe_ant = tabla_combinada[f'Importe_Anterior'].sum()
    total_trans_ant = tabla_combinada[f'Transacciones_Anterior'].sum()
    total_importe_nue = tabla_combinada[f'Importe_Nuevo'].sum()
    total_trans_nue = tabla_combinada[f'Transacciones_Nuevo'].sum()
    total_variacion = total_importe_nue - total_importe_ant
    
    # Determinar clase de variación para total
    clase_variacion_total = 'positive' if total_variacion > 0 else 'negative' if total_variacion < 0 else ''
    
    html += f"""
                    <tr class="total-row-detalle">
                        <td colspan="{len(columnas_grupo)}"><strong>TOTAL {canal}</strong></td>
                        <td><strong>S/ {total_importe_ant:,.0f}</strong></td>
                        <td><strong>{total_trans_ant:,.0f}</strong></td>
                        <td><strong>S/ {total_importe_nue:,.0f}</strong></td>
                        <td><strong>{total_trans_nue:,.0f}</strong></td>
                        <td class="{clase_variacion_total}"><strong>S/ {total_variacion:+,.0f}</strong></td>
                    </tr>
                </tbody>
            </table>
        </div>
    </div>
</div>
    """
    
    return html


def generar_dashboard_html_detallado(df_comparativo, fecha_anterior, fecha_nueva, hora_corte, tablas_detalladas):
    """Genera un dashboard HTML sin gráfico y con mejores estilos móviles"""
    
    # Generar HTML
    html_template = f"""
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Avance de Ventas FNB - {fecha_anterior} vs {fecha_nueva}</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 15px;
            font-size: 13px;
        }}
        
        .dashboard-container {{
            max-width: 1600px;
            margin: 0 auto;
            background: white;
            border-radius: 15px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        
        .header {{
            background: linear-gradient(135deg, #2c3e50 0%, #3498db 100%);
            color: white;
            padding: 20px;
            text-align: center;
        }}
        
        .header h1 {{
            font-size: 1.8em;
            margin-bottom: 8px;
            font-weight: 300;
        }}
        
        .header .subtitle {{
            font-size: 1em;
            opacity: 0.9;
        }}
        
        .summary-section {{
            padding: 20px;
            background: #f8f9fa;
        }}
        
        .summary-container {{
            background: white;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 5px 15px rgba(0,0,0,0.08);
        }}
        
        .summary-header {{
            background: #34495e;
            color: white;
            padding: 15px;
            text-align: center;
        }}
        
        .table-responsive {{
            overflow-x: auto;
            -webkit-overflow-scrolling: touch;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            min-width: 600px;
        }}
        
        th, td {{
            padding: 8px 4px;
            text-align: center;
            border-bottom: 1px solid #ecf0f1;
            font-size: 11px;
            white-space: nowrap;
        }}
        
        th {{
            background: #3498db;
            color: white;
            font-weight: 600;
            font-size: 10px;
            padding: 10px 4px;
        }}
        
        tbody tr:nth-child(even) {{
            background-color: #f8f9fa;
        }}
        
        tbody tr:hover {{
            background-color: #e3f2fd;
        }}
        
        .total-row {{
            background: #2c3e50 !important;
            color: white;
            font-weight: bold;
        }}
        
        /* MEJORA: Clases corregidas para variaciones positivas y negativas */
        .positive {{
            color: #27ae60 !important;
            font-weight: bold;
        }}
        
        .negative {{
            color: #e74c3c !important;
            font-weight: bold;
        }}
        
        .details-section {{
            padding: 20px;
            background: #f8f9fa;
        }}
        
        .canal-section {{
            margin-bottom: 20px;
        }}
        
        .canal-header {{
            background: #34495e;
            border-radius: 8px 8px 0 0;
        }}
        
        .canal-title {{
            background: transparent;
            color: white;
            padding: 12px 15px;
            margin: 0;
            font-size: 1.1em;
            text-align: center;
        }}
        
        .table-container {{
            background: white;
            border-radius: 0 0 8px 8px;
            overflow: hidden;
            box-shadow: 0 5px 15px rgba(0,0,0,0.08);
        }}
        
        .detail-table {{
            min-width: 700px;
        }}
        
        .detail-table th {{
            background: #3498db;
            color: white;
            font-size: 10px;
            padding: 10px 4px;
        }}
        
        .detail-table td {{
            font-size: 10px;
            padding: 6px 3px;
        }}
        
        .subtotal-row {{
            background: #3498db !important;
            color: white;
            font-weight: bold;
        }}
        
        .subtotal-row.collapsed {{
            background: white !important;
            color: #2c3e50;
        }}
        
        .subtotal-row.collapsed:hover {{
            background: #f8f9fa !important;
        }}
        
        .subtotal-row:hover {{
            background: #2980b9 !important;
        }}
        
        /* MEJORA: Nueva clase para fila de totales en tablas detalladas con plomo claro */
        .total-row-detalle {{
            background: #bdc3c7 !important;
            color: #2c3e50 !important;
            font-weight: bold;
        }}
        
        .total-row-detalle:hover {{
            background: #a9b2ba !important;
        }}
        
        .clickable-row {{
            cursor: pointer;
            transition: background-color 0.2s ease;
        }}
        
        .clickable-row:hover {{
            background-color: #2980b9 !important;
        }}
        
        .expand-control {{
            display: flex;
            align-items: center;
            gap: 6px;
        }}
        
        .expand-icon {{
            font-size: 0.7em;
            transition: transform 0.3s ease;
            color: #fff;
            font-weight: bold;
        }}
        
        .expand-icon.collapsed {{
            transform: rotate(-90deg);
        }}
        
        .detail-indent {{
            padding-left: 20px !important;
            border-left: 2px solid #3498db;
            background-color: #f8f9fa !important;
        }}
        
        .detail-cell {{
            background-color: #f8f9fa !important;
            font-size: 9px;
        }}
        
        .detail-row-collapsed {{
            display: none;
        }}
        
        .mobile-scroll {{
            overflow-x: auto;
            -webkit-overflow-scrolling: touch;
        }}
        
        .footer {{
            background: #2c3e50;
            color: white;
            padding: 15px;
            text-align: center;
            font-size: 0.8em;
        }}
        
        /* Estilos específicos para móvil */
        @media (max-width: 768px) {{
            body {{
                padding: 10px;
                font-size: 11px;
            }}
            
            .header {{
                padding: 15px;
            }}
            
            .header h1 {{
                font-size: 1.5em;
            }}
            
            .header .subtitle {{
                font-size: 0.9em;
            }}
            
            .summary-section, .details-section {{
                padding: 15px;
            }}
            
            .summary-header {{
                padding: 12px;
            }}
            
            th, td {{
                padding: 6px 2px;
                font-size: 9px;
            }}
            
            th {{
                font-size: 8px;
                padding: 8px 2px;
            }}
            
            .detail-table th {{
                font-size: 8px;
                padding: 8px 2px;
            }}
            
            .detail-table td {{
                font-size: 8px;
                padding: 4px 2px;
            }}
            
            .detail-cell {{
                font-size: 8px;
            }}
            
            .canal-title {{
                font-size: 0.9em;
                padding: 10px 12px;
            }}
            
            .expand-control {{
                font-size: 0.8em;
                gap: 4px;
            }}
            
            .expand-icon {{
                font-size: 0.6em;
            }}
            
            .detail-indent {{
                padding-left: 15px !important;
            }}
            
            table {{
                min-width: 500px;
            }}
            
            .detail-table {{
                min-width: 550px;
            }}
        }}
        
        @media (max-width: 480px) {{
            body {{
                padding: 8px;
                font-size: 10px;
            }}
            
            .header h1 {{
                font-size: 1.3em;
            }}
            
            .header .subtitle {{
                font-size: 0.8em;
            }}
            
            th, td {{
                padding: 4px 1px;
                font-size: 8px;
            }}
            
            th {{
                font-size: 7px;
                padding: 6px 1px;
            }}
            
            .detail-table th {{
                font-size: 7px;
                padding: 6px 1px;
            }}
            
            .detail-table td {{
                font-size: 7px;
                padding: 3px 1px;
            }}
            
            .detail-cell {{
                font-size: 7px;
            }}
            
            .canal-title {{
                font-size: 0.8em;
                padding: 8px 10px;
            }}
            
            table {{
                min-width: 450px;
            }}
            
            .detail-table {{
                min-width: 500px;
            }}
        }}
    </style>
</head>
<body>
    <div class="dashboard-container">
        <div class="header">
            <h1>📊 Reporte Comparativo de Ventas</h1>
            <div class="subtitle">Periodos: {fecha_anterior} vs {fecha_nueva} | Corte: {hora_corte}</div>
        </div>
        
        <div class="summary-section">
            <div class="summary-container">
                <div class="summary-header">
                    <h2>Resumen General por Canal</h2>
                </div>
                <div class="table-responsive">
                    <table>
                        <thead>
                            <tr>
                                <th>Canal</th>
                                <th>Importe {fecha_anterior}</th>
                                <th># Trx {fecha_anterior}</th>
                                <th>Importe {fecha_nueva}</th>
                                <th># Trx {fecha_nueva}</th>
                                <th>Variación Importe</th>
                            </tr>
                        </thead>
                        <tbody>"""

    # Agregar filas de la tabla resumen con lógica corregida de colores
    for canal, row in df_comparativo.iterrows():
        clase_fila = "total-row" if canal == "TOTAL" else ""
        
        # MEJORA: Lógica corregida para determinar variaciones positivas/negativas
        variacion_str = str(row['Variación Importe'])
        clase_variacion = ""
        
        # Extraer el valor numérico de la variación
        try:
            # Eliminar formato "S/ " y "," para obtener el número
            valor_numerico = variacion_str.replace('S/ ', '').replace(',', '')
            # Convertir a float para comparación
            valor_float = float(valor_numerico)
            
            if valor_float > 0:
                clase_variacion = "positive"
            elif valor_float < 0:
                clase_variacion = "negative"
        except (ValueError, AttributeError):
            # En caso de error en conversión, mantener sin clase
            clase_variacion = ""
        
        html_template += f"""
                            <tr class="{clase_fila}">
                                <td><strong>{canal}</strong></td>
                                <td>{row['Importe_Anterior']}</td>
                                <td>{row['Transacciones_Anterior']}</td>
                                <td>{row['Importe_Nuevo']}</td>
                                <td>{row['Transacciones_Nuevo']}</td>
                                <td class="{clase_variacion}">{row['Variación Importe']}</td>
                            </tr>
        """

    html_template += """
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
        
        <div class="details-section">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; flex-wrap: wrap;">
                <h2 style="color: #2c3e50; font-size: 1.5em; margin: 0;">📋 Análisis Detallado por Canal</h2>
                <div style="display: flex; gap: 8px; margin-top: 8px;">
                    <button onclick="toggleAllSections(true)" style="background: #27ae60; color: white; border: none; padding: 6px 12px; border-radius: 4px; cursor: pointer; font-size: 0.8em;">
                        📂 Expandir Todo
                    </button>
                    <button onclick="toggleAllSections(false)" style="background: #e74c3c; color: white; border: none; padding: 6px 12px; border-radius: 4px; cursor: pointer; font-size: 0.8em;">
                        📁 Contraer Todo
                    </button>
                </div>
            </div>
    """

    # Agregar tablas detalladas en orden específico
    orden_canales = [
        'ALO CÁLIDDA',
        'CSC', 
        'DIGITAL',
        'TIENDAS CÁLIDDA',
        'RETAIL',
        'MOTOS',
        'GRANDES SUPERFICIES',
        'MATERIALES Y ACABADOS DE CONSTRUCCIÓN',
        'CANAL PROVEEDOR',
        'FFVV - PUERTA A PUERTA'
    ]
    
    # Mostrar primero los canales en orden específico
    for canal in orden_canales:
        if canal in tablas_detalladas:
            html_template += tablas_detalladas[canal]
    
    # Luego mostrar canales adicionales que no están en la lista
    for canal, tabla_html in tablas_detalladas.items():
        if canal not in orden_canales:
            html_template += tabla_html

    html_template += f"""
        </div>
        
        <div class="footer">
            <p>📅 Generado el {datetime.now().strftime('%d/%m/%Y a las %H:%M:%S')}</p>
        </div>
    </div>
    
    <script>
        // Función para expandir/contraer secciones
        function toggleSection(sectionId) {{
            const detailRows = document.querySelectorAll('.detail-row-' + sectionId);
            const icon = document.getElementById('icon_' + sectionId);
            
            let isVisible = false;
            detailRows.forEach(row => {{
                if (row.style.display === 'none' || row.style.display === '') {{
                    row.style.display = 'table-row';
                    isVisible = true;
                }} else {{
                    row.style.display = 'none';
                }}
            }});
            
            // Cambiar ícono
            if (isVisible) {{
                icon.textContent = '▼';
                icon.classList.remove('collapsed');
            }} else {{
                icon.textContent = '▶';
                icon.classList.add('collapsed');
            }}
        }}
        
        // Inicializar todas las secciones como contraídas
        document.addEventListener('DOMContentLoaded', function() {{
            // Encontrar todas las filas de subtotal clickeable
            const subtotalRows = document.querySelectorAll('.clickable-row');
            
            subtotalRows.forEach(row => {{
                const onclick = row.getAttribute('onclick');
                if (onclick) {{
                    const sectionId = onclick.match(/toggleSection\\('([^']+)'\\)/)[1];
                    const icon = document.getElementById('icon_' + sectionId);
                    
                    // Contraer inicialmente
                    if (icon) {{
                        icon.textContent = '▶';
                        icon.classList.add('collapsed');
                        row.classList.add('collapsed');
                    }}
                }}
            }});
            
            // Manejar scroll horizontal en móviles
            const tables = document.querySelectorAll('.table-responsive');
            tables.forEach(table => {{
                table.addEventListener('touchstart', function() {{
                    this.style.webkitOverflowScrolling = 'touch';
                }});
            }});
        }});
        
        // Función para expandir/contraer todas las secciones
        function toggleAllSections(expand = null) {{
            const subtotalRows = document.querySelectorAll('.clickable-row');
            
            subtotalRows.forEach(row => {{
                const onclick = row.getAttribute('onclick');
                if (onclick) {{
                    const sectionId = onclick.match(/toggleSection\\('([^']+)'\\)/)[1];
                    const detailRows = document.querySelectorAll('.detail-row-' + sectionId);
                    const icon = document.getElementById('icon_' + sectionId);
                    
                    if (expand === null) {{
                        // Toggle automático
                        const isCurrentlyVisible = detailRows[0] && detailRows[0].style.display === 'table-row';
                        expand = !isCurrentlyVisible;
                    }}
                    
                    detailRows.forEach(detailRow => {{
                        detailRow.style.display = expand ? 'table-row' : 'none';
                    }});
                    
                    if (icon) {{
                        icon.textContent = expand ? '▼' : '▶';
                        if (expand) {{
                            icon.classList.remove('collapsed');
                            row.classList.remove('collapsed');
                        }} else {{
                            icon.classList.add('collapsed');
                            row.classList.add('collapsed');
                        }}
                    }}
                }}
            }});
        }}
    </script>
</body>
</html>
    """
    
    return html_template


def duplicar_html_como_txt(ruta_html: str) -> str | None:
    """Crea una copia .txt del HTML para evitar bloqueo de extensión en WhatsApp."""
    if not os.path.exists(ruta_html):
        print(f"   ❌ No existe el HTML para copiar: {ruta_html}")
        return None
    ruta_txt = os.path.splitext(ruta_html)[0] + ".txt"
    try:
        with open(ruta_html, 'r', encoding='utf-8') as src, open(ruta_txt, 'w', encoding='utf-8') as dst:
            dst.write(src.read())
        print(f"   ✅ Copia TXT generada: {ruta_txt}")
        return ruta_txt
    except Exception as e:
        print(f"   ❌ No se pudo crear la copia TXT: {e}")
        return None


def comprimir_html_en_zip(ruta_html: str) -> str | None:
    """Comprime el HTML en un ZIP para enviar por WhatsApp."""
    if not os.path.exists(ruta_html):
        print(f"   ❌ No existe el HTML para comprimir: {ruta_html}")
        return None
    
    base, _ = os.path.splitext(ruta_html)
    ruta_zip = base + ".zip"
    
    try:
        with zipfile.ZipFile(ruta_zip, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(ruta_html, arcname=os.path.basename(ruta_html))
        print(f"   ✅ HTML comprimido en ZIP: {os.path.basename(ruta_zip)}")
        return ruta_zip
    except Exception as e:
        print(f"   ❌ No se pudo crear el ZIP: {e}")
        return None


async def enviar_documento_whatsapp(whatsapp, ruta_archivo):
    """Adjunta un archivo (HTML) en el chat abierto."""
    if not os.path.exists(ruta_archivo):
        print(f"   ❌ Archivo no encontrado para enviar: {ruta_archivo}")
        return False

    try:
        # Paso 1: Click en botón de adjuntar (clip)
        print("   🔍 Buscando botón adjuntar...")
        clip_selectors = [
            "[data-testid='clip']",
            "span[data-icon='clip']",
            "[data-icon='clip']",
            "button[aria-label='Adjuntar']",
            "button[aria-label='Attach']",
            "[title='Adjuntar']",
            "[title='Attach']",
        ]
        boton_clip = None
        for sel in clip_selectors:
            try:
                boton_clip = await whatsapp.page.wait_for_selector(sel, timeout=3000, state='visible')
                if boton_clip:
                    print(f"   ✅ Botón adjuntar encontrado: {sel}")
                    break
            except Exception:
                continue

        if not boton_clip:
            print("   ⚠️ No se encontró con selectores específicos, buscando todos los botones...")
            try:
                # Buscar cualquier botón que contenga un icono de clip
                all_buttons = await whatsapp.page.query_selector_all("button, span[role='button']")
                for btn in all_buttons:
                    try:
                        # Verificar si tiene data-icon o aria-label relacionado con adjuntar
                        data_icon = await btn.get_attribute("data-icon")
                        aria_label = await btn.get_attribute("aria-label")
                        title = await btn.get_attribute("title")
                        
                        if (data_icon and 'clip' in data_icon.lower()) or \
                           (aria_label and ('adjuntar' in aria_label.lower() or 'attach' in aria_label.lower())) or \
                           (title and ('adjuntar' in title.lower() or 'attach' in title.lower())):
                            boton_clip = btn
                            print(f"   ✅ Botón adjuntar encontrado por búsqueda amplia")
                            break
                    except:
                        continue
            except Exception as e:
                print(f"   ⚠️ Error en búsqueda amplia: {e}")

        if not boton_clip:
            print("   ❌ No se encontró el botón de adjuntar en WhatsApp Web")
            return False

        await boton_clip.click()
        print("   ✅ Click en botón adjuntar, esperando menú...")
        await asyncio.sleep(2)  # Esperar a que aparezca el menú completo

        # Paso 2: Click en opción "Documento" del menú (botón visible, NO el input)
        print("   🔍 Buscando botón Documento en el menú...")
        btn_doc_selectors = [
            "li[data-testid='mi-attach-document']",
            "button[aria-label*='ocumento']",
            "span[data-icon='document']",
            "[title*='ocumento']",
            "input[accept*='*'][type='file']",  # Input que acepta cualquier archivo
        ]
        
        btn_documento = None
        for sel in btn_doc_selectors:
            try:
                btn_documento = await whatsapp.page.wait_for_selector(sel, timeout=2000, state='attached')
                if btn_documento:
                    print(f"   ✅ Botón/Input Documento encontrado: {sel}")
                    break
            except Exception:
                continue

        # Si no lo encontramos, buscar entre todos los elementos visibles
        if not btn_documento:
            print("   🔍 Búsqueda amplia de opción Documento...")
            try:
                all_items = await whatsapp.page.query_selector_all("li, button, span, input[type='file']")
                for item in all_items:
                    try:
                        data_testid = await item.get_attribute("data-testid")
                        aria_label = await item.get_attribute("aria-label")
                        title = await item.get_attribute("title")
                        data_icon = await item.get_attribute("data-icon")
                        
                        if (data_testid and 'document' in data_testid.lower()) or \
                           (aria_label and 'document' in aria_label.lower()) or \
                           (title and 'document' in title.lower()) or \
                           (data_icon and 'document' in data_icon.lower()):
                            btn_documento = item
                            print(f"   ✅ Elemento Documento encontrado por búsqueda amplia")
                            break
                    except:
                        continue
            except Exception as e:
                print(f"   ⚠️ Error en búsqueda amplia: {e}")

        if not btn_documento:
            print("   ❌ No se encontró el botón Documento en el menú")
            print("   💡 Intenta ejecutar manualmente para verificar la estructura del menú")
            return False

        # Paso 3: Adjuntar archivo directamente al input (sin click, bypass del menú visual)
        print("   📎 Buscando input file para adjuntar...")
        
        try:
            # Buscar inputs file en orden de preferencia (menos restrictivos primero)
            input_file = None
            selectors_input = [
                "input[type='file']",  # Cualquier input sin restricciones
                "input[type='file'][accept*='*']",  # Que acepte cualquier tipo
                "input[type='file']:not([accept])",  # Sin atributo accept
            ]
            
            for sel in selectors_input:
                try:
                    input_file = await whatsapp.page.query_selector(sel)
                    if input_file:
                        accept = await input_file.get_attribute("accept")
                        print(f"   ✅ Input file encontrado - accept: {accept if accept else 'sin restricción'}")
                        break
                except Exception:
                    continue
            
            if not input_file:
                # Fallback: obtener todos los inputs y usar el que tenga menos restricciones
                print("   🔍 Búsqueda amplia de inputs file...")
                inputs = await whatsapp.page.query_selector_all("input[type='file']")
                if inputs:
                    input_file = inputs[0]  # Usar el primero encontrado
                    accept = await input_file.get_attribute("accept")
                    print(f"   ✅ Input file seleccionado - accept: {accept if accept else 'sin restricción'}")
            
            if not input_file:
                print("   ❌ No se encontró ningún input[type='file']")
                return False
            
            # Mostrar info del archivo a adjuntar
            print(f"   📂 Adjuntando: {os.path.basename(ruta_archivo)}")
            print(f"   📊 Tamaño: {os.path.getsize(ruta_archivo) / 1024:.1f} KB")
            
            # Adjuntar directamente
            await input_file.set_input_files(ruta_archivo)
            print("   ✅ Archivo adjuntado al input")
            
        except Exception as e:
            print(f"   ❌ Error al adjuntar archivo: {e}")
            return False

        # Paso 4: Esperar preview y buscar botón de enviar
        print("   ⏳ Esperando preview del documento...")
        await asyncio.sleep(2)  # esperar a que aparezca el preview y botones

        print("   🔍 Buscando botón enviar...")
        selectors_enviar = [
            "span[data-icon='send']",
            "span[data-testid='send']",
            "[data-testid='compose-btn-send']",
            "button[aria-label*='Enviar']",
            "button[aria-label*='Send']",
        ]

        enviado = False
        for sel in selectors_enviar:
            try:
                btn_send = await whatsapp.page.wait_for_selector(sel, timeout=5000, state='visible')
                if btn_send:
                    print(f"   ✅ Botón enviar encontrado: {sel}")
                    await btn_send.click()
                    enviado = True
                    break
            except Exception as e:
                continue

        if not enviado:
            print("   ⚠️ No se encontró el botón de enviar, intentando Enter...")
            try:
                await whatsapp.page.keyboard.press("Enter")
                enviado = True
                print("   ✅ Enter presionado")
            except Exception:
                pass

        if not enviado:
            print("   ❌ No se pudo enviar el archivo adjunto")
            return False

        await asyncio.sleep(2)
        print(f"   ✅ Archivo enviado exitosamente: {os.path.basename(ruta_archivo)}")
        return True
    except Exception as e:
        print(f"   ❌ Error enviando archivo: {e}")
        return False


async def enviar_reporte_whatsapp(ruta_dashboard, fecha_anterior, fecha_nueva, hora_corte):
    """Envía saludo y adjunto HTML vía WhatsApp usando WhatsAppSender de 02.8."""
    print("\n=== ENVIANDO REPORTE POR WHATSAPP ===")

    try:
        modulo_whatsapp = SourceFileLoader(
            "avance_whatsapp", r"D:\FNB\Proyectos\Python\02.8.AvanceVentasCortesImagen.py"
        ).load_module()
        WhatsAppSender = getattr(modulo_whatsapp, "WhatsAppSender")
    except Exception as e:
        print(f"❌ No se pudo cargar WhatsAppSender desde 02.8: {e}")
        return False

    numeros_destino = [
        #"51976650091",  # Stefany
        #"51940193512",  # Chema
        "51941377441",  # Carlos
    ]

    # Preparar ZIP del HTML (WhatsApp bloquea .html por seguridad)
    print("\n📦 Comprimiendo HTML en ZIP para envío...")
    ruta_zip = comprimir_html_en_zip(ruta_dashboard)
    if not ruta_zip:
        print("❌ No se pudo comprimir el HTML")
        return False

    whatsapp = WhatsAppSender()

    print("\n🔄 Inicializando WhatsApp Web...")
    inicializado = await whatsapp.inicializar_driver()
    if not inicializado:
        print("\n❌ No se pudo inicializar WhatsApp Web")
        return False

    print("✅ WhatsApp Web inicializado correctamente\n")
    print("🔍 Verificando que WhatsApp Web esté listo...")
    await asyncio.sleep(3)
    try:
        await whatsapp.page.wait_for_selector("[data-testid='chat-list'], canvas", timeout=5000)
        print("✅ WhatsApp Web listo para enviar mensajes\n")
    except Exception:
        print("⚠️ No se pudo verificar el estado de WhatsApp Web, continuando...\n")

    hora_actual = datetime.now().time()
    saludo_base = f"se brinda el avance comparativo de ventas. Periodo {fecha_anterior} vs {fecha_nueva} - Corte {hora_corte}"
    if hora_actual < dt_time(12, 0):
        saludo = f"Buenos días, {saludo_base}"
    else:
        saludo = f"Buenas tardes, {saludo_base}"

    try:
        for idx, numero in enumerate(numeros_destino, 1):
            print(f"📱 [{idx}/{len(numeros_destino)}] Enviando a {numero}")

            if not await whatsapp.buscar_contacto(numero):
                print(f"   ❌ No se pudo abrir chat con {numero}, se omite")
                continue

            exito_saludo = await whatsapp.enviar_mensaje(saludo)
            if not exito_saludo:
                print("   ⚠️ No se pudo enviar el saludo")

            exito_archivo = await enviar_documento_whatsapp(whatsapp, ruta_zip)
            if not exito_archivo:
                print("   ❌ No se pudo enviar el archivo ZIP adjunto")

            await asyncio.sleep(2)

        return True
    except Exception as e:
        print(f"❌ Error durante el envío por WhatsApp: {e}")
        return False
    finally:
        print("\n🔒 Cerrando WhatsApp Web...")
        try:
            await whatsapp.cerrar()
        except Exception as cierre_err:
            print(f"⚠️ Error al cerrar WhatsApp Web: {cierre_err}")


def generar_reporte_dashboard():
    """Función principal modificada para generar dashboard HTML detallado sin gráfico"""
    # CONFIGURACIÓN
    ruta_fechas = r"D:\FNB\Reportes\19. Reportes IBR\06. Avance de ventas cortes\Fechas"
    ruta_anterior = os.path.join(ruta_fechas, "Fecha Anterior.xlsx")
    ruta_nueva = os.path.join(ruta_fechas, "Fecha Nueva.xlsx")
    col_importe = "IMPORTE (S./)"

    # VALIDACIONES
    for ruta in [ruta_anterior, ruta_nueva]:
        if not os.path.exists(ruta):
            print(f"❌ Error: No se encontró {os.path.basename(ruta)}")
            return

    # PROCESAMIENTO
    print("🔄 Iniciando procesamiento de archivos...")
    print("🔍 FILTRO APLICADO: Solo registros con ESTADO = PENDIENTE DE ENTREGA, ENTREGADO, PENDIENTE DE APROBACIÓN")
    
    transformer = SalesTransformer()
    df_anterior = transformer.procesar_archivo(ruta_anterior)
    df_nuevo = transformer.procesar_archivo(ruta_nueva)

    # CONVERTIR FECHA VENTA A DATETIME
    df_anterior['FECHA VENTA'] = pd.to_datetime(df_anterior['FECHA VENTA'], errors='coerce', dayfirst=True)
    df_nuevo['FECHA VENTA'] = pd.to_datetime(df_nuevo['FECHA VENTA'], errors='coerce', dayfirst=True)

    # OBTENER FECHAS DE LOS ARCHIVOS
    fecha_anterior = df_anterior['FECHA VENTA'].min().strftime('%d/%m/%Y')
    fecha_nueva = df_nuevo['FECHA VENTA'].min().strftime('%d/%m/%Y')
    print(f"📅 Fechas detectadas: {fecha_anterior} vs {fecha_nueva}")

    # LÓGICA DE CORTE MEJORADA
    hora_max_anterior = df_anterior['HORA VENTA'].max()
    hora_max_nueva = df_nuevo['HORA VENTA'].max()
    tipo_corte = determinar_hora_corte()

    if tipo_corte == "nuevo":
        hora_corte = hora_max_nueva
        print(f"⏰ Usando hora máxima del archivo nuevo: {hora_corte}")
    else:  # tipo_corte == "mayor"
        hora_corte = max(hora_max_anterior, hora_max_nueva)
        archivo_usado = "anterior" if hora_max_anterior >= hora_max_nueva else "nuevo"
        print(f"⏰ Usando hora mayor ({archivo_usado}): {hora_corte}")

    # FILTRAR DATOS
    df_anterior_filtrado = df_anterior[df_anterior['HORA VENTA'] <= hora_corte].copy()
    df_nuevo_filtrado = df_nuevo[df_nuevo['HORA VENTA'] <= hora_corte].copy()

    # CREAR PIVOTS PARA RESUMEN
    def crear_pivot(df, sufijo):
        if df.empty:
            return pd.DataFrame(columns=[f'Importe_{sufijo}', f'Transacciones_{sufijo}'])

        pivot = df.groupby('CANAL_VENTA').agg(
            Importe=(col_importe, 'sum'),
            Transacciones=('codigo_unico', 'nunique')
        )
        pivot.columns = [f'Importe_{sufijo}', f'Transacciones_{sufijo}']
        return pivot

    pivot_anterior = crear_pivot(df_anterior_filtrado, 'Anterior')
    pivot_nuevo = crear_pivot(df_nuevo_filtrado, 'Nuevo')

    # COMBINAR RESULTADOS
    if pivot_anterior.empty and pivot_nuevo.empty:
        print("❌ No hay datos para comparar")
        return
    elif pivot_anterior.empty:
        df_comparativo = pivot_nuevo.copy()
        df_comparativo['Importe_Anterior'] = 0
        df_comparativo['Transacciones_Anterior'] = 0
    elif pivot_nuevo.empty:
        df_comparativo = pivot_anterior.copy()
        df_comparativo['Importe_Nuevo'] = 0
        df_comparativo['Transacciones_Nuevo'] = 0
    else:
        df_comparativo = pd.merge(pivot_anterior, pivot_nuevo, left_index=True, right_index=True, how='outer').fillna(0)

    # CALCULAR VARIACIÓN
    df_comparativo['Variación Importe'] = df_comparativo['Importe_Nuevo'] - df_comparativo['Importe_Anterior']

    # REORDENAR COLUMNAS
    columnas_orden = ['Importe_Anterior', 'Transacciones_Anterior', 'Importe_Nuevo', 'Transacciones_Nuevo',
                      'Variación Importe']
    df_comparativo = df_comparativo[[col for col in columnas_orden if col in df_comparativo.columns]]

    # AGREGAR TOTALES
    df_comparativo.loc['TOTAL'] = df_comparativo.sum()

    # FORMATEAR
    for col in ['Importe_Anterior', 'Importe_Nuevo', 'Variación Importe']:
        if col in df_comparativo.columns:
            df_comparativo[col] = df_comparativo[col].map('S/ {:,.0f}'.format)

    for col in ['Transacciones_Anterior', 'Transacciones_Nuevo']:
        if col in df_comparativo.columns:
            df_comparativo[col] = df_comparativo[col].map('{:,.0f}'.format)

    print("\n📊 RESUMEN GENERAL:")
    print(df_comparativo)

    # CREAR TABLAS DETALLADAS
    print("\n🔍 Generando tablas detalladas por canal...")
    tablas_detalladas = crear_tablas_detalladas(df_anterior_filtrado, df_nuevo_filtrado, col_importe, fecha_anterior, fecha_nueva)
    
    print(f"   ✅ {len(tablas_detalladas)} canales procesados: {', '.join(tablas_detalladas.keys())}")

    # GENERAR DASHBOARD HTML DETALLADO SIN GRÁFICO
    print(f"\n🌐 Generando Dashboard HTML detallado sin gráfico...")
    
    html_content = generar_dashboard_html_detallado(df_comparativo, fecha_anterior, fecha_nueva, hora_corte, tablas_detalladas)
    
    # GUARDAR ARCHIVO HTML EN RUTA ESPECÍFICA
    ruta_dashboards = r"D:\FNB\Reportes\19. Reportes IBR\06. Avance de ventas cortes\Dashboards"
    
    # Crear directorio si no existe
    os.makedirs(ruta_dashboards, exist_ok=True)
    
    # Formato de hora para el nombre (convertir de time a string formateado)
    hora_corte_str = str(hora_corte)
    nombre_archivo = f"Avance de Ventas FNB - Periodo {fecha_anterior.replace('/', '-')} vs {fecha_nueva.replace('/', '-')} - Corte {hora_corte_str.replace(':', '-')}.html"
    ruta_dashboard = os.path.join(ruta_dashboards, nombre_archivo)
    
    with open(ruta_dashboard, 'w', encoding='utf-8') as file:
        file.write(html_content)
    
    print(f"✅ Dashboard detallado generado: {nombre_archivo}")
    print(f"📁 Ubicación: {ruta_dashboard}")
    
    # MOSTRAR ESTADÍSTICAS
    print(f"\n📈 ESTADÍSTICAS DEL DASHBOARD:")
    print(f"   🏪 Canales analizados: {len(tablas_detalladas)}")
    print(f"   📊 Tablas detalladas: {len(tablas_detalladas)}")
    print(f"   📋 Registros anteriores (filtrados): {len(df_anterior_filtrado)}")
    print(f"   📋 Registros nuevos (filtrados): {len(df_nuevo_filtrado)}")
    print(f"   🔍 Filtro ESTADO: PENDIENTE DE ENTREGA, ENTREGADO, PENDIENTE DE APROBACIÓN")
    
    # ABRIR AUTOMÁTICAMENTE EN EL NAVEGADOR
    try:
        webbrowser.open(f'file://{ruta_dashboard}')
        print("🌐 Dashboard abierto en el navegador")
    except Exception as e:
        print(f"⚠️ No se pudo abrir automáticamente: {e}")
        print("🔗 Abra manualmente el archivo HTML en su navegador")
    
    # ENVIAR POR WHATSAPP: saludo + archivo HTML
    try:
        asyncio.run(enviar_reporte_whatsapp(ruta_dashboard, fecha_anterior, fecha_nueva, str(hora_corte)))
    except Exception as e:
        print(f"⚠️ No se pudo completar el envío por WhatsApp: {e}")

    # MOSTRAR INFORMACIÓN PARA COMPARTIR
    print(f"\n📤 INSTRUCCIONES PARA COMPARTIR:")
    print(f"1. El archivo '{nombre_archivo}' incluye análisis detallado por canal")
    print(f"2. Filtro aplicado por ESTADO (solo registros válidos)")
    print(f"3. Tablas dinámicas con agrupaciones específicas:")
    for canal in tablas_detalladas.keys():
        print(f"   • {canal}")
    print(f"4. Colores corregidos para variaciones positivas (verde) y negativas (rojo)")
    print(f"5. Filas de totales en tablas detalladas con fondo plomo claro")
    print(f"6. Completamente independiente - no requiere conexión a internet")
    print(f"7. Compatible con todos los navegadores web")
    print(f"8. Optimizado para dispositivos móviles")
    
    return ruta_dashboard


if __name__ == "__main__":
    generar_reporte_dashboard()
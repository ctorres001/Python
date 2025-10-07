import pandas as pd
import os
from pathlib import Path
from datetime import datetime

# Configuración de rutas
ARCHIVO_ORIGEN = r"D:\FNB\Reportes\19. Reportes IBR\00. Estructura Reporte\Procesado\Archivo_Procesado.xlsx"
CARPETA_DESTINO = r"D:\FNB\Reportes\19. Reportes IBR\12. Pendientes de Entrega Blackout\Archivos actualizados"
CARPETA_NO_MAPEADOS = r"D:\FNB\Reportes\19. Reportes IBR\12. Pendientes de Entrega Blackout\Archivos no mapeados"

# Lista de aliados comerciales
ALIADOS = [
    "A & G INGENIERIA",
    "AC COMPANY",
    "AGHASO PERÚ",
    "ALMAJER",
    "CREDIVARGAS",
    "CROSLAND AUTOMOTRIZ S.A.C.",
    "DISTRIBUIDORA LUNA",
    "DISTRIBUIDORA VICKY - AMPER",
    "GAME CENTER OFICIAL",
    "GASODOMESTICOS",
    "GRUPO MERPES",
    "HARDTECH",
    "HERTFORD AUTOMOTRIZ",
    "INCOSER GAS PERU S.A.C.",
    "INTEGRA RETAIL",
    "KITCHEN CENTER",
    "KOJAC",
    "L & H ARQUITECTURA E INGENIERIA",
    "LOGISTICALS",
    "LQ TRAIDING",
    "MALL HOGAR",
    "MAQUIMOTORA",
    "MATERIALES NASCA",
    "MI PC LISTA",
    "MULTITOP",
    "PERU SMART",
    "RHEEM PERU",
    "SACO",
    "SANY PERU",
    "SEI PERU",
    "SOCOPUR",
    "TOP MOTORS",
    "TRS",
    "VAINSA"
]

# Columnas finales del reporte (incluye los 20 grupos de productos)
COLUMNAS_REPORTE = [
    "RESPONSABLE DE VENTA", "SEDE", "ALIADO COMERCIAL", "CUENTA CONTRATO", 
    "CLIENTE", "DNI", "TELÉFONO", "CORREO", "Nro. PEDIDO VENTA", 
    "Nro. DE CONTRATO", "IMPORTE (S./)", "CRÉDITO UTILIZADO", "Nro. DE CUOTAS", 
    "FECHA VENTA", "HORA VENTA", "FECHA ENTREGA", "TIPO DESPACHO", "ESTADO", 
    "ASESOR DE VENTAS", "VALIDACIÓN MOTORIZADO"
]

# Agregar columnas de productos del 1 al 20
for i in range(1, 21):
    COLUMNAS_REPORTE.extend([
        f"PRODUCTO_{i}", f"SKU_{i}", f"CANTIDAD_{i}", f"PRECIO_{i}", 
        f"CATEGORIA_{i}", f"MARCA_{i}", f"SUBCANAL_{i}", f"CATEGORIA REAL_{i}", 
        f"TIPO PRODUCTO_{i}", f"MODELO PRODUCTO_{i}", f"SKU2_{i}", f"DESCRIPCION_{i}"
    ])

def crear_carpeta_destino():
    """Crea la carpeta de destino si no existe"""
    Path(CARPETA_DESTINO).mkdir(parents=True, exist_ok=True)
    print(f"✓ Carpeta de destino verificada: {CARPETA_DESTINO}")
    
    Path(CARPETA_NO_MAPEADOS).mkdir(parents=True, exist_ok=True)
    print(f"✓ Carpeta no mapeados verificada: {CARPETA_NO_MAPEADOS}")

def leer_archivo_origen():
    """Lee el archivo origen de Excel"""
    print(f"\nLeyendo archivo origen: {ARCHIVO_ORIGEN}")
    try:
        df = pd.read_excel(ARCHIVO_ORIGEN)
        print(f"✓ Archivo leído exitosamente. Total de registros: {len(df)}")
        
        # Estandarizar la columna ALIADO COMERCIAL a mayúsculas
        if 'ALIADO COMERCIAL' in df.columns:
            df['ALIADO COMERCIAL'] = df['ALIADO COMERCIAL'].str.upper().str.strip()
            print(f"✓ Columna ALIADO COMERCIAL estandarizada a mayúsculas")
        
        # Estandarizar TIPO DESPACHO a mayúsculas
        if 'TIPO DESPACHO' in df.columns:
            df['TIPO DESPACHO'] = df['TIPO DESPACHO'].str.upper().str.strip()
            print(f"✓ Columna TIPO DESPACHO estandarizada a mayúsculas")
        
        # Estandarizar ESTADO a mayúsculas
        if 'ESTADO' in df.columns:
            df['ESTADO'] = df['ESTADO'].str.upper().str.strip()
            print(f"✓ Columna ESTADO estandarizada a mayúsculas")
        
        return df
    except Exception as e:
        print(f"✗ Error al leer el archivo origen: {e}")
        return None

def filtrar_datos(df):
    """Filtra los datos según los criterios especificados"""
    print("\nAplicando filtros...")
    
    # Filtro 1: Aliados comerciales (para archivos principales)
    df_filtrado = df[df['ALIADO COMERCIAL'].isin(ALIADOS)].copy()
    print(f"  - Después de filtrar por aliados mapeados: {len(df_filtrado)} registros")
    
    # Filtro 2: Tipo de despacho
    tipos_despacho = ["DELIVERY A DOMICILIO", "RECOJO EN TIENDA"]
    df_filtrado = df_filtrado[df_filtrado['TIPO DESPACHO'].isin(tipos_despacho)]
    print(f"  - Después de filtrar por tipo despacho: {len(df_filtrado)} registros")
    
    # Filtro 3: Estado
    df_filtrado = df_filtrado[df_filtrado['ESTADO'] == 'PENDIENTE DE ENTREGA']
    print(f"  - Después de filtrar por estado: {len(df_filtrado)} registros")
    
    # Filtrar registros NO MAPEADOS (excluyendo CARDIF)
    print("\n--- Buscando aliados no mapeados ---")
    df_no_mapeados = df[
        (~df['ALIADO COMERCIAL'].isin(ALIADOS)) &  # No está en la lista de aliados
        (df['ALIADO COMERCIAL'] != 'CARDIF') &     # No es CARDIF
        (df['ESTADO'] == 'PENDIENTE DE ENTREGA')   # Estado pendiente
    ].copy()
    print(f"  - Registros no mapeados encontrados: {len(df_no_mapeados)}")
    
    if len(df_no_mapeados) > 0:
        aliados_unicos = df_no_mapeados['ALIADO COMERCIAL'].unique()
        print(f"  - Aliados no mapeados: {len(aliados_unicos)}")
        for aliado in sorted(aliados_unicos):
            cantidad = len(df_no_mapeados[df_no_mapeados['ALIADO COMERCIAL'] == aliado])
            print(f"    • {aliado}: {cantidad} registros")
    
    return df_filtrado, df_no_mapeados

def ajustar_columnas(df):
    """Ajusta las columnas para incluir todos los grupos de productos del 1 al 20"""
    
    # Crear DataFrame con todas las columnas necesarias
    df_final = pd.DataFrame(columns=COLUMNAS_REPORTE)
    
    # Copiar columnas existentes
    for col in COLUMNAS_REPORTE:
        if col in df.columns:
            df_final[col] = df[col]
    
    # Formatear columnas numéricas con 2 decimales
    # Columnas de importes
    columnas_numericas = ['IMPORTE (S./)', 'CRÉDITO UTILIZADO']
    
    # Agregar todas las columnas PRECIO_1 a PRECIO_20
    for i in range(1, 21):
        columnas_numericas.append(f'PRECIO_{i}')
    
    # Aplicar formato de 2 decimales
    for col in columnas_numericas:
        if col in df_final.columns:
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce')
            df_final[col] = df_final[col].round(2)
    
    return df_final

def cargar_archivo_existente(ruta_archivo):
    """Carga un archivo Excel existente o retorna un DataFrame vacío"""
    if os.path.exists(ruta_archivo):
        try:
            df_existente = pd.read_excel(ruta_archivo)
            print(f"  - Archivo existente encontrado con {len(df_existente)} registros")
            return df_existente
        except Exception as e:
            print(f"  - Error al leer archivo existente: {e}. Se creará uno nuevo.")
            return pd.DataFrame(columns=COLUMNAS_REPORTE)
    else:
        print(f"  - Archivo nuevo, se creará desde cero")
        return pd.DataFrame(columns=COLUMNAS_REPORTE)

def realizar_carga_incremental(df_nuevo, df_existente):
    """Combina datos nuevos con existentes, evitando duplicados por Nro. DE CONTRATO"""
    if df_existente.empty:
        return df_nuevo
    
    # Identificar contratos existentes
    contratos_existentes = set(df_existente['Nro. DE CONTRATO'].dropna())
    
    # Filtrar solo registros nuevos
    df_incremental = df_nuevo[~df_nuevo['Nro. DE CONTRATO'].isin(contratos_existentes)]
    
    print(f"    → Registros nuevos a agregar: {len(df_incremental)}")
    
    # Combinar
    df_combinado = pd.concat([df_existente, df_incremental], ignore_index=True)
    
    return df_combinado

def procesar_aliados(df_filtrado):
    """Procesa cada aliado comercial y genera/actualiza su archivo"""
    print(f"\n{'='*60}")
    print("PROCESANDO ALIADOS COMERCIALES")
    print(f"{'='*60}")
    
    resumen = []
    
    for aliado in ALIADOS:
        # Filtrar datos del aliado
        df_aliado = df_filtrado[df_filtrado['ALIADO COMERCIAL'] == aliado].copy()
        
        if df_aliado.empty:
            continue
        
        print(f"\n[{aliado}]")
        print(f"  • Registros encontrados: {len(df_aliado)}")
        
        # Ajustar columnas
        df_aliado = ajustar_columnas(df_aliado)
        
        # Ordenar por FECHA VENTA
        if 'FECHA VENTA' in df_aliado.columns:
            df_aliado['FECHA VENTA'] = pd.to_datetime(df_aliado['FECHA VENTA'], errors='coerce')
            df_aliado = df_aliado.sort_values('FECHA VENTA', ascending=True)
        
        # Nombre del archivo
        nombre_archivo = f"{aliado}.xlsx"
        ruta_archivo = os.path.join(CARPETA_DESTINO, nombre_archivo)
        
        # Cargar archivo existente
        df_existente = cargar_archivo_existente(ruta_archivo)
        
        # Realizar carga incremental
        df_final = realizar_carga_incremental(df_aliado, df_existente)
        
        # Guardar archivo
        try:
            df_final.to_excel(ruta_archivo, index=False, engine='openpyxl')
            print(f"  ✓ Archivo guardado: {nombre_archivo}")
            print(f"    → Total de registros en archivo: {len(df_final)}")
            
            resumen.append({
                'Aliado': aliado,
                'Registros Nuevos': len(df_aliado),
                'Total Registros': len(df_final),
                'Archivo': nombre_archivo
            })
        except Exception as e:
            print(f"  ✗ Error al guardar archivo: {e}")
    
    return resumen

def procesar_no_mapeados(df_no_mapeados):
    """Procesa y guarda los registros de aliados no mapeados"""
    if df_no_mapeados.empty:
        print(f"\n{'='*60}")
        print("ALIADOS NO MAPEADOS")
        print(f"{'='*60}")
        print("✓ No se encontraron aliados no mapeados")
        return None
    
    print(f"\n{'='*60}")
    print("PROCESANDO ALIADOS NO MAPEADOS")
    print(f"{'='*60}")
    
    # Ajustar columnas
    df_no_mapeados = ajustar_columnas(df_no_mapeados)
    
    # Ordenar por FECHA VENTA
    if 'FECHA VENTA' in df_no_mapeados.columns:
        df_no_mapeados['FECHA VENTA'] = pd.to_datetime(df_no_mapeados['FECHA VENTA'], errors='coerce')
        df_no_mapeados = df_no_mapeados.sort_values('FECHA VENTA', ascending=True)
    
    # Nombre del archivo
    nombre_archivo = "Aliados_No_Mapeados.xlsx"
    ruta_archivo = os.path.join(CARPETA_NO_MAPEADOS, nombre_archivo)
    
    print(f"\n[ALIADOS NO MAPEADOS]")
    print(f"  • Registros encontrados: {len(df_no_mapeados)}")
    
    # Cargar archivo existente
    df_existente = cargar_archivo_existente(ruta_archivo)
    
    # Realizar carga incremental
    df_final = realizar_carga_incremental(df_no_mapeados, df_existente)
    
    # Guardar archivo
    try:
        df_final.to_excel(ruta_archivo, index=False, engine='openpyxl')
        print(f"  ✓ Archivo guardado: {nombre_archivo}")
        print(f"    → Total de registros en archivo: {len(df_final)}")
        print(f"    → Ubicación: {CARPETA_NO_MAPEADOS}")
        
        return {
            'Registros Nuevos': len(df_no_mapeados),
            'Total Registros': len(df_final),
            'Archivo': nombre_archivo
        }
    except Exception as e:
        print(f"  ✗ Error al guardar archivo: {e}")
        return None

def mostrar_resumen(resumen, resumen_no_mapeados):
    """Muestra un resumen final del procesamiento"""
    print(f"\n{'='*60}")
    print("RESUMEN DEL PROCESAMIENTO")
    print(f"{'='*60}")
    
    if not resumen and not resumen_no_mapeados:
        print("No se procesaron archivos.")
        return
    
    # Resumen de aliados mapeados
    if resumen:
        df_resumen = pd.DataFrame(resumen)
        print(f"\n📊 ALIADOS MAPEADOS:")
        print(f"  • Total de aliados procesados: {len(resumen)}")
        print(f"  • Total de registros nuevos: {df_resumen['Registros Nuevos'].sum()}")
        print(f"\n  Detalle por aliado:")
        print("  " + df_resumen.to_string(index=False).replace('\n', '\n  '))
    
    # Resumen de aliados no mapeados
    if resumen_no_mapeados:
        print(f"\n⚠️  ALIADOS NO MAPEADOS:")
        print(f"  • Registros nuevos: {resumen_no_mapeados['Registros Nuevos']}")
        print(f"  • Total registros: {resumen_no_mapeados['Total Registros']}")
        print(f"  • Archivo: {resumen_no_mapeados['Archivo']}")
    
    print(f"\n{'='*60}")
    print(f"✓ Proceso completado exitosamente")
    print(f"  Archivos mapeados: {CARPETA_DESTINO}")
    print(f"  Archivos no mapeados: {CARPETA_NO_MAPEADOS}")
    print(f"{'='*60}")

def main():
    """Función principal"""
    print(f"{'='*60}")
    print("GENERADOR DE REPORTES IBR POR ALIADO COMERCIAL")
    print(f"{'='*60}")
    print(f"Fecha/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Crear carpeta de destino
    crear_carpeta_destino()
    
    # Leer archivo origen
    df_origen = leer_archivo_origen()
    if df_origen is None:
        return
    
    # Filtrar datos
    df_filtrado, df_no_mapeados = filtrar_datos(df_origen)
    
    if df_filtrado.empty and df_no_mapeados.empty:
        print("\n⚠ No se encontraron registros que cumplan los criterios de filtrado.")
        return
    
    # Procesar cada aliado mapeado
    resumen = []
    if not df_filtrado.empty:
        resumen = procesar_aliados(df_filtrado)
    
    # Procesar aliados no mapeados
    resumen_no_mapeados = None
    if not df_no_mapeados.empty:
        resumen_no_mapeados = procesar_no_mapeados(df_no_mapeados)
    
    # Mostrar resumen
    mostrar_resumen(resumen, resumen_no_mapeados)

if __name__ == "__main__":
    main()
import pandas as pd
import xlwings as xw
import os
from pathlib import Path
from datetime import datetime
import time
import win32com.client as win32

# Configuración de rutas
ARCHIVO_ORIGEN = r"D:\FNB\Reportes\19. Reportes IBR\00. Estructura Reporte\Procesado\Archivo_Procesado.xlsx"
CARPETA_BASE = r"C:\Users\carlos.torres2\Gas Natural de Lima y Callao S.A. (GNLC)\FNB - 99. BlackOut\09-2025\Aliados"
CARPETA_NO_MAPEADOS = os.path.join(CARPETA_BASE, "z_OTROS")

# Contraseña para protección
PASSWORD = "Calidda2024NC"

# Lista de aliados comerciales
ALIADOS = [
    "A & G INGENIERIA", "AC COMPANY", "AGHASO PERÚ", "ALMAJER", "CREDIVARGAS",
    "CROSLAND AUTOMOTRIZ S.A.C.", "DISTRIBUIDORA LUNA", "DISTRIBUIDORA VICKY - AMPER",
    "GAME CENTER OFICIAL", "GASODOMESTICOS", "GRUPO MERPES", "HARDTECH",
    "HERTFORD AUTOMOTRIZ", "INCOSER GAS PERU S.A.C.", "INTEGRA RETAIL", "KITCHEN CENTER",
    "KOJAC", "L & H ARQUITECTURA E INGENIERIA", "LOGISTICALS", "LQ TRAIDING",
    "MALL HOGAR", "MAQUIMOTORA", "MATERIALES NASCA", "MI PC LISTA", "MULTITOP",
    "PERU SMART", "RHEEM PERU", "SACO", "SANY PERU", "SEI PERU", "SOCOPUR",
    "TOP MOTORS", "TRS", "VAINSA"
]

# Columnas del reporte
COLUMNAS_REPORTE = [
    "RESPONSABLE DE VENTA", "SEDE", "ALIADO COMERCIAL", "CUENTA CONTRATO", 
    "CLIENTE", "DNI", "TELÉFONO", "CORREO", "Nro. PEDIDO VENTA", 
    "Nro. DE CONTRATO", "IMPORTE (S./)", "CRÉDITO UTILIZADO", "Nro. DE CUOTAS", 
    "FECHA VENTA", "HORA VENTA", "FECHA ENTREGA", "TIPO DESPACHO", "ESTADO", 
    "ASESOR DE VENTAS", "VALIDACIÓN MOTORIZADO"
]

# Agregar columnas de productos
for i in range(1, 21):
    COLUMNAS_REPORTE.extend([
        f"PRODUCTO_{i}", f"SKU_{i}", f"CANTIDAD_{i}", f"PRECIO_{i}", 
        f"CATEGORIA_{i}", f"MARCA_{i}", f"SUBCANAL_{i}", f"CATEGORIA REAL_{i}", 
        f"TIPO PRODUCTO_{i}", f"MODELO PRODUCTO_{i}", f"SKU2_{i}", f"DESCRIPCION_{i}"
    ])

def crear_carpetas():
    """Crea las carpetas necesarias si no existen"""
    Path(CARPETA_NO_MAPEADOS).mkdir(parents=True, exist_ok=True)
    print(f"✓ Carpeta no mapeados verificada: {CARPETA_NO_MAPEADOS}")

def leer_archivo_origen():
    """Lee el archivo origen de Excel"""
    print(f"\nLeyendo archivo origen: {ARCHIVO_ORIGEN}")
    try:
        df = pd.read_excel(ARCHIVO_ORIGEN)
        print(f"✓ Archivo leído exitosamente. Total de registros: {len(df)}")
        
        # Estandarizar columnas
        if 'ALIADO COMERCIAL' in df.columns:
            df['ALIADO COMERCIAL'] = df['ALIADO COMERCIAL'].str.upper().str.strip()
        if 'TIPO DESPACHO' in df.columns:
            df['TIPO DESPACHO'] = df['TIPO DESPACHO'].str.upper().str.strip()
        if 'ESTADO' in df.columns:
            df['ESTADO'] = df['ESTADO'].str.upper().str.strip()
        
        return df
    except Exception as e:
        print(f"✗ Error al leer el archivo origen: {e}")
        return None

def filtrar_datos(df):
    """Filtra los datos según los criterios especificados"""
    print("\nAplicando filtros...")
    
    df_filtrado = df[df['ALIADO COMERCIAL'].isin(ALIADOS)].copy()
    print(f"  - Después de filtrar por aliados mapeados: {len(df_filtrado)} registros")
    
    tipos_despacho = ["DELIVERY A DOMICILIO", "RECOJO EN TIENDA"]
    df_filtrado = df_filtrado[df_filtrado['TIPO DESPACHO'].isin(tipos_despacho)]
    print(f"  - Después de filtrar por tipo despacho: {len(df_filtrado)} registros")
    
    df_filtrado = df_filtrado[df_filtrado['ESTADO'] == 'PENDIENTE DE ENTREGA']
    print(f"  - Después de filtrar por estado: {len(df_filtrado)} registros")
    
    print("\n--- Buscando aliados no mapeados ---")
    df_no_mapeados = df[
        (~df['ALIADO COMERCIAL'].isin(ALIADOS)) &
        (df['ALIADO COMERCIAL'] != 'CARDIF') &
        (df['ESTADO'] == 'PENDIENTE DE ENTREGA')
    ].copy()
    print(f"  - Registros no mapeados encontrados: {len(df_no_mapeados)}")
    
    if len(df_no_mapeados) > 0:
        aliados_unicos = df_no_mapeados['ALIADO COMERCIAL'].unique()
        print(f"  - Aliados no mapeados: {len(aliados_unicos)}")
        for aliado in sorted(aliados_unicos):
            cantidad = len(df_no_mapeados[df_no_mapeados['ALIADO COMERCIAL'] == aliado])
            print(f"    • {aliado}: {cantidad} registros")
    
    return df_filtrado, df_no_mapeados

def preparar_datos(df):
    """Prepara los datos con formato correcto"""
    if 'FECHA VENTA' in df.columns:
        df['FECHA VENTA'] = pd.to_datetime(df['FECHA VENTA'], errors='coerce')
    if 'FECHA ENTREGA' in df.columns:
        df['FECHA ENTREGA'] = pd.to_datetime(df['FECHA ENTREGA'], errors='coerce')
    
    columnas_orden = []
    if 'FECHA VENTA' in df.columns:
        columnas_orden.append('FECHA VENTA')
    if 'HORA VENTA' in df.columns:
        columnas_orden.append('HORA VENTA')
    
    if columnas_orden:
        df = df.sort_values(columnas_orden, ascending=True)
    
    columnas_numericas = ['IMPORTE (S./)', 'CRÉDITO UTILIZADO']
    for i in range(1, 21):
        columnas_numericas.append(f'PRECIO_{i}')
    
    for col in columnas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].round(2)
    
    columnas_disponibles = [col for col in COLUMNAS_REPORTE if col in df.columns]
    df = df[columnas_disponibles]
    
    return df

def actualizar_archivo_aliado(aliado, df_nuevos):
    """Actualiza el archivo de un aliado específico usando xlwings"""
    print(f"\n[{aliado}]")
    print(f"  • Registros nuevos encontrados: {len(df_nuevos)}")
    
    carpeta_aliado = os.path.join(CARPETA_BASE, aliado)
    nombre_archivo = f"Base Pendientes para Proveedores - {aliado}.xlsx"
    ruta_archivo = os.path.join(carpeta_aliado, nombre_archivo)
    
    if not os.path.exists(ruta_archivo):
        print(f"  ⚠ Archivo no encontrado: {ruta_archivo}")
        return None
    
    app = None
    wb = None
    
    try:
        # Abrir Excel con configuraciones iniciales
        app = xw.App(visible=False)
        app.display_alerts = False
        app.screen_updating = False
        app.enable_events = False
        
        wb = app.books.open(ruta_archivo, update_links=False, read_only=False)
        ws = wb.sheets[0]
        
        print(f"  ✓ Archivo abierto: {nombre_archivo}")
        
        # Esperar a que se cargue completamente
        time.sleep(0.3)
        
        # Desproteger libro - usar try/except independiente
        try:
            wb.api.Unprotect(PASSWORD)
            time.sleep(0.1)
            print(f"  ✓ Libro desprotegido")
        except Exception as e:
            print(f"  ⚠ Advertencia al desproteger libro: {e}")
        
        # Desproteger hoja - verificar que ws.api existe
        try:
            if ws.api is not None:
                ws.api.Unprotect(PASSWORD)
                time.sleep(0.1)
                print(f"  ✓ Hoja desprotegida")
            else:
                print(f"  ⚠ Advertencia: No se pudo acceder a ws.api")
        except Exception as e:
            print(f"  ⚠ Advertencia al desproteger hoja: {e}")
        
        # Encontrar última fila con datos
        try:
            ultima_fila = ws.api.Cells(ws.api.Rows.Count, 5).End(-4162).Row
            print(f"  • Última fila con datos: {ultima_fila}")
        except:
            try:
                ultima_fila = ws.used_range.last_cell.row
                print(f"  • Última fila con datos: {ultima_fila}")
            except:
                ultima_fila = 1
                print(f"  • No hay datos previos, comenzando desde fila 1")
        
        # Leer contratos existentes
        contratos_existentes = set()
        if ultima_fila > 1:
            try:
                idx_contrato = COLUMNAS_REPORTE.index('Nro. DE CONTRATO')
                col_contrato = idx_contrato + 5  # Columna E = 5
                
                for fila in range(2, ultima_fila + 1):
                    try:
                        valor = ws.range((fila, col_contrato)).value
                        if valor is not None:
                            contratos_existentes.add(str(valor))
                    except:
                        continue
                
                print(f"  • Contratos existentes: {len(contratos_existentes)}")
            except Exception as e:
                print(f"  ⚠ Error al leer contratos: {e}")
        
        # Filtrar registros nuevos
        df_nuevos['Nro. DE CONTRATO'] = df_nuevos['Nro. DE CONTRATO'].astype(str)
        df_incremental = df_nuevos[~df_nuevos['Nro. DE CONTRATO'].isin(contratos_existentes)]
        
        registros_agregados = 0
        if df_incremental.empty:
            print(f"  → No hay registros nuevos para agregar")
            ultima_fila_final = ultima_fila
        else:
            print(f"  → Registros a agregar: {len(df_incremental)}")
            fila_inicio = ultima_fila + 1
            
            datos = df_incremental.values.tolist()
            
            # Escribir datos fila por fila
            try:
                for i, fila_datos in enumerate(datos):
                    fila_actual = fila_inicio + i
                    for j, valor in enumerate(fila_datos):
                        col = j + 5  # Columna E = 5
                        try:
                            ws.range((fila_actual, col)).value = valor
                        except:
                            continue
                    registros_agregados += 1
                
                print(f"  ✓ {registros_agregados} registros escritos desde fila {fila_inicio}")
            except Exception as e:
                print(f"  ✗ Error al escribir datos: {e}")
                registros_agregados = 0
            
            ultima_fila_final = ultima_fila + registros_agregados
        
        # Aplicar formatos solo si hay datos y ws.api está disponible
        if ultima_fila_final > 1 and ws.api is not None:
            try:
                print(f"  • Aplicando formato...")
                
                # Fuente
                num_columnas = len(COLUMNAS_REPORTE)
                rango = ws.range((1, 5), (ultima_fila_final, 4 + num_columnas))
                rango.api.Font.Name = "Aptos Narrow"
                rango.api.Font.Size = 8
                print(f"  ✓ Formato de fuente aplicado")
                
                # Formato de fechas
                try:
                    idx_fecha_venta = COLUMNAS_REPORTE.index('FECHA VENTA')
                    col_fv = idx_fecha_venta + 5
                    rango_fv = ws.range((2, col_fv), (ultima_fila_final, col_fv))
                    rango_fv.number_format = 'dd/mm/yyyy'
                    print(f"  ✓ Formato de FECHA VENTA aplicado")
                except:
                    pass
                
                try:
                    idx_fecha_entrega = COLUMNAS_REPORTE.index('FECHA ENTREGA')
                    col_fe = idx_fecha_entrega + 5
                    rango_fe = ws.range((2, col_fe), (ultima_fila_final, col_fe))
                    rango_fe.number_format = 'dd/mm/yyyy'
                    print(f"  ✓ Formato de FECHA ENTREGA aplicado")
                except:
                    pass
            
            except Exception as e:
                print(f"  ⚠ Error al aplicar formatos: {e}")
        
        # Proteger hoja con parámetros correctos para xlwings
        try:
            if ws.api is not None:
                ws.api.Protect(Password=PASSWORD, Contents=True)
                time.sleep(0.1)
                print(f"  ✓ Hoja protegida")
        except Exception as e:
            print(f"  ⚠ Error al proteger hoja: {e}")
        
        # Proteger libro
        try:
            wb.api.Protect(PASSWORD, True, False)
            time.sleep(0.1)
            print(f"  ✓ Libro protegido")
        except Exception as e:
            print(f"  ⚠ Error al proteger libro: {e}")
        
        # Guardar
        try:
            wb.save()
            time.sleep(0.2)
            print(f"  ✓ Archivo guardado")
        except Exception as e:
            print(f"  ✗ Error al guardar: {e}")
            return None
        
        print(f"  ✓ Proceso completado exitosamente")
        print(f"    → Total de registros en archivo: {ultima_fila_final - 1}")
        
        return {
            'Aliado': aliado,
            'Registros Nuevos': registros_agregados,
            'Total Registros': ultima_fila_final - 1,
            'Archivo': nombre_archivo
        }
        
    except Exception as e:
        import traceback
        print(f"  ✗ Error al procesar archivo: {e}")
        print(traceback.format_exc())
        return None
    
    finally:
        if wb:
            try:
                wb.close()
            except:
                pass
        if app:
            try:
                app.quit()
            except:
                pass
        time.sleep(0.5)

def procesar_aliados(df_filtrado):
    """Procesa cada aliado comercial"""
    print(f"\n{'='*60}")
    print("PROCESANDO ALIADOS COMERCIALES")
    print(f"{'='*60}")
    
    resumen = []
    
    for aliado in ALIADOS:
        df_aliado = df_filtrado[df_filtrado['ALIADO COMERCIAL'] == aliado].copy()
        
        if df_aliado.empty:
            continue
        
        df_aliado = preparar_datos(df_aliado)
        resultado = actualizar_archivo_aliado(aliado, df_aliado)
        
        if resultado:
            resumen.append(resultado)
    
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
    
    df_no_mapeados = preparar_datos(df_no_mapeados)
    
    nombre_archivo = "Aliados_No_Mapeados.xlsx"
    ruta_archivo = os.path.join(CARPETA_NO_MAPEADOS, nombre_archivo)
    
    print(f"\n[ALIADOS NO MAPEADOS]")
    print(f"  • Registros encontrados: {len(df_no_mapeados)}")
    
    df_existente = pd.DataFrame()
    if os.path.exists(ruta_archivo):
        try:
            df_existente = pd.read_excel(ruta_archivo)
            print(f"  - Archivo existente encontrado con {len(df_existente)} registros")
        except Exception as e:
            print(f"  - Error al leer archivo existente: {e}")
    
    if not df_existente.empty:
        contratos_existentes = set(df_existente['Nro. DE CONTRATO'].astype(str))
        df_no_mapeados['Nro. DE CONTRATO'] = df_no_mapeados['Nro. DE CONTRATO'].astype(str)
        df_incremental = df_no_mapeados[~df_no_mapeados['Nro. DE CONTRATO'].isin(contratos_existentes)]
        df_final = pd.concat([df_existente, df_incremental], ignore_index=True)
    else:
        df_final = df_no_mapeados
    
    try:
        df_final.to_excel(ruta_archivo, index=False, engine='openpyxl')
        print(f"  ✓ Archivo guardado: {nombre_archivo}")
        print(f"    → Total de registros en archivo: {len(df_final)}")
        
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
    
    if resumen:
        df_resumen = pd.DataFrame(resumen)
        print(f"\n📊 ALIADOS MAPEADOS:")
        print(f"  • Total de aliados procesados: {len(resumen)}")
        print(f"  • Total de registros nuevos: {df_resumen['Registros Nuevos'].sum()}")
        print(f"\n  Detalle por aliado:")
        print("  " + df_resumen.to_string(index=False).replace('\n', '\n  '))
    
    if resumen_no_mapeados:
        print(f"\n⚠️  ALIADOS NO MAPEADOS:")
        print(f"  • Registros nuevos: {resumen_no_mapeados['Registros Nuevos']}")
        print(f"  • Total registros: {resumen_no_mapeados['Total Registros']}")
        print(f"  • Archivo: {resumen_no_mapeados['Archivo']}")
    
    print(f"\n{'='*60}")
    print(f"✓ Proceso completado exitosamente")
    print(f"  Carpeta base: {CARPETA_BASE}")
    print(f"  No mapeados: {CARPETA_NO_MAPEADOS}")
    print(f"{'='*60}")

def main():
    """Función principal"""
    print(f"{'='*60}")
    print("ACTUALIZADOR DE REPORTES IBR CON XLWINGS")
    print(f"{'='*60}")
    print(f"Fecha/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    crear_carpetas()
    
    df_origen = leer_archivo_origen()
    if df_origen is None:
        return
    
    df_filtrado, df_no_mapeados = filtrar_datos(df_origen)
    
    if df_filtrado.empty and df_no_mapeados.empty:
        print("\n⚠ No se encontraron registros que cumplan los criterios de filtrado.")
        return
    
    resumen = []
    if not df_filtrado.empty:
        resumen = procesar_aliados(df_filtrado)
    
    resumen_no_mapeados = None
    if not df_no_mapeados.empty:
        resumen_no_mapeados = procesar_no_mapeados(df_no_mapeados)
    
    mostrar_resumen(resumen, resumen_no_mapeados)

if __name__ == "__main__":
    main()
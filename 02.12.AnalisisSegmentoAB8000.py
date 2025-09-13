import pandas as pd
from datetime import datetime
import os

def analizar_clientes_potenciales():
    """
    Análisis de clientes potenciales FNB cruzando datos con el dashboard
    Enfoque en NSE 4-5 y Saldo Créd >= 8000
    Incluye evolutivo diario y desglose por canal
    """
    
    # Rutas de archivos
    archivo_potenciales = r"D:\FNB\Reportes\04 Reporte Clientes Potenciales\Convertir BP a CC Clientes Potenciales FNB.xlsx"
    archivo_dashboard = r"D:\FNB\Reportes\01. Reporte Diario\01 Dashboard FNB.xlsm"
    ruta_salida = r"D:\FNB\Reportes\04 Reporte Clientes Potenciales"
    
    # Fecha de corte
    fecha_corte = datetime(2025, 9, 1)
    
    print("Leyendo archivos...")
    
    try:
        # Leer archivo de clientes potenciales - ambas hojas
        df_base1 = pd.read_excel(archivo_potenciales, sheet_name='Base 1')
        df_base2 = pd.read_excel(archivo_potenciales, sheet_name='Base 2')
        
        # Combinar ambas bases
        df_potenciales = pd.concat([df_base1, df_base2], ignore_index=True)
        
        print(f"Clientes potenciales cargados: {len(df_potenciales)} registros")
        print(f"Columnas disponibles en potenciales: {list(df_potenciales.columns)}")
        
        # Leer archivo dashboard (cabecera en fila 2, índice 1 en Python)
        df_dashboard = pd.read_excel(archivo_dashboard, sheet_name='BD Colocaciones FNB', header=1)
        
        # Filtrar dashboard: excluir CARDIF de Nombre de Proveedor
        df_dashboard = df_dashboard[df_dashboard['Nombre de Proveedor'] != 'CARDIF']
        
        print(f"Dashboard cargado (sin CARDIF): {len(df_dashboard)} registros")
        print(f"Columnas disponibles en dashboard: {list(df_dashboard.columns)}")
        
        # Convertir la columna F. Registro a datetime
        df_dashboard['F. Registro'] = pd.to_datetime(df_dashboard['F. Registro'], errors='coerce')
        
        # Función para encontrar columnas por nombre parcial
        def encontrar_columna(df, nombres_posibles, descripcion):
            for nombres in nombres_posibles:
                if isinstance(nombres, str):
                    nombres = [nombres]
                for col in df.columns:
                    col_lower = col.lower().strip()
                    if all(nombre.lower() in col_lower for nombre in nombres):
                        return col
            return None
        
        # Identificar columnas necesarias
        cuenta_col_pot = encontrar_columna(df_potenciales, [['cuenta', 'contrato']], 'Cuenta Contrato en potenciales')
        cuenta_col_dash = encontrar_columna(df_dashboard, [['cuenta', 'contrato']], 'Cuenta Contrato en dashboard')
        nse_col = encontrar_columna(df_potenciales, ['nse'], 'NSE')
        saldo_col = encontrar_columna(df_potenciales, [['saldo', 'créd'], ['saldo', 'cred']], 'Saldo Créd')
        
        # Buscar columna Canal exacta
        canal_col = None
        if 'Canal' in df_dashboard.columns:
            canal_col = 'Canal'
        else:
            print("ADVERTENCIA: No se encontró la columna 'Canal' exacta en el dashboard")
            print("Columnas disponibles que contienen 'canal':")
            for col in df_dashboard.columns:
                if 'canal' in col.lower():
                    print(f"  - {col}")
        
        if not cuenta_col_pot or not cuenta_col_dash:
            print("ERROR: No se encontró la columna 'Cuenta Contrato' en uno de los archivos")
            print(f"Potenciales: {cuenta_col_pot}")
            print(f"Dashboard: {cuenta_col_dash}")
            return
        
        print(f"Columna cuenta en potenciales: {cuenta_col_pot}")
        print(f"Columna cuenta en dashboard: {cuenta_col_dash}")
        print(f"Columna NSE: {nse_col}")
        print(f"Columna Saldo Créd: {saldo_col}")
        print(f"Columna Canal: {canal_col}")
        
        # Convertir columnas a numérico
        if nse_col:
            df_potenciales[nse_col] = pd.to_numeric(df_potenciales[nse_col], errors='coerce')
        if saldo_col:
            df_potenciales[saldo_col] = pd.to_numeric(df_potenciales[saldo_col], errors='coerce')
        
        # Función para crear evolutivo diario
        def crear_evolutivo_diario(df_compras, titulo):
            if len(df_compras) == 0:
                return pd.DataFrame()
            
            # Agrupar por fecha
            evolutivo = df_compras.groupby(df_compras['F. Registro'].dt.date)[cuenta_col_dash].nunique().reset_index()
            evolutivo.columns = ['Fecha', 'Clientes']
            
            # Calcular acumulado
            evolutivo = evolutivo.sort_values('Fecha')
            evolutivo['Acumulado'] = evolutivo['Clientes'].cumsum()
            
            print(f"\nEvolutivo diario - {titulo}:")
            print(evolutivo.to_string(index=False))
            
            return evolutivo
        
        # Función para crear desglose por canal
        def crear_desglose_canal(df_compras, titulo):
            if len(df_compras) == 0 or not canal_col:
                return pd.DataFrame()
            
            desglose = df_compras.groupby(canal_col)[cuenta_col_dash].nunique().reset_index()
            desglose.columns = ['Canal', 'Clientes']
            desglose = desglose.sort_values('Clientes', ascending=False)
            
            print(f"\nDesglose por Canal - {titulo}:")
            print(desglose.to_string(index=False))
            
            return desglose
        
        # ========== ANÁLISIS NSE 4 y 5 ==========
        print("\n" + "="*50)
        print("ANALIZANDO NSE 4 Y 5")
        print("="*50)
        
        nse_4_5_results = {}
        clientes_nse_4_5 = pd.DataFrame()
        clientes_nse_4_5_no_compraron_antes = pd.DataFrame()
        clientes_nse_4_5_compraron_despues = pd.DataFrame()
        evolutivo_nse = pd.DataFrame()
        desglose_canal_nse = pd.DataFrame()
        
        if nse_col:
            # 1. Total NSE 4 y 5
            clientes_nse_4_5 = df_potenciales[df_potenciales[nse_col].isin([4, 5])].copy()
            print(f"Total NSE 4 y 5: {len(clientes_nse_4_5)}")
            
            # 2. Identificar compras antes del 01/09/2025 para NSE 4-5
            compras_antes_nse = df_dashboard[
                (df_dashboard[cuenta_col_dash].isin(clientes_nse_4_5[cuenta_col_pot])) &
                (df_dashboard['F. Registro'].notna()) &
                (df_dashboard['F. Registro'] < fecha_corte)
            ][cuenta_col_dash].unique()
            
            # 3. NSE 4-5 que NO compraron antes
            clientes_nse_4_5_no_compraron_antes = clientes_nse_4_5[
                ~clientes_nse_4_5[cuenta_col_pot].isin(compras_antes_nse)
            ].copy()
            print(f"NSE 4-5 que NO compraron antes del 01/09/2025: {len(clientes_nse_4_5_no_compraron_antes)}")
            
            # 4. De los que NO compraron antes, cuántos compraron después
            compras_despues_nse = df_dashboard[
                (df_dashboard[cuenta_col_dash].isin(clientes_nse_4_5_no_compraron_antes[cuenta_col_pot])) &
                (df_dashboard['F. Registro'].notna()) &
                (df_dashboard['F. Registro'] >= fecha_corte)
            ]
            
            cuentas_compraron_despues_nse = compras_despues_nse[cuenta_col_dash].unique()
            clientes_nse_4_5_compraron_despues = clientes_nse_4_5_no_compraron_antes[
                clientes_nse_4_5_no_compraron_antes[cuenta_col_pot].isin(cuentas_compraron_despues_nse)
            ].copy()
            print(f"NSE 4-5 que compraron DESPUÉS del 01/09/2025: {len(clientes_nse_4_5_compraron_despues)}")
            
            # 5. Crear evolutivo diario NSE 4-5
            evolutivo_nse = crear_evolutivo_diario(compras_despues_nse, "NSE 4-5")
            
            # 6. Crear desglose por canal NSE 4-5
            desglose_canal_nse = crear_desglose_canal(compras_despues_nse, "NSE 4-5")
            
            nse_4_5_results = {
                'total': len(clientes_nse_4_5),
                'no_compraron_antes': len(clientes_nse_4_5_no_compraron_antes),
                'compraron_despues': len(clientes_nse_4_5_compraron_despues)
            }
        else:
            print("ADVERTENCIA: No se encontró la columna NSE")
        
        # ========== ANÁLISIS SALDO CRÉD >= 8000 ==========
        print("\n" + "="*50)
        print("ANALIZANDO SALDO CRÉD >= 8000")
        print("="*50)
        
        saldo_8000_results = {}
        clientes_saldo_8000 = pd.DataFrame()
        clientes_saldo_8000_no_compraron_antes = pd.DataFrame()
        clientes_saldo_8000_compraron_despues = pd.DataFrame()
        evolutivo_saldo = pd.DataFrame()
        desglose_canal_saldo = pd.DataFrame()
        
        if saldo_col:
            # 1. Total Saldo >= 8000
            clientes_saldo_8000 = df_potenciales[df_potenciales[saldo_col] >= 8000].copy()
            print(f"Total Saldo Créd >= 8000: {len(clientes_saldo_8000)}")
            
            # 2. Identificar compras antes del 01/09/2025 para Saldo >= 8000
            compras_antes_saldo = df_dashboard[
                (df_dashboard[cuenta_col_dash].isin(clientes_saldo_8000[cuenta_col_pot])) &
                (df_dashboard['F. Registro'].notna()) &
                (df_dashboard['F. Registro'] < fecha_corte)
            ][cuenta_col_dash].unique()
            
            # 3. Saldo >= 8000 que NO compraron antes
            clientes_saldo_8000_no_compraron_antes = clientes_saldo_8000[
                ~clientes_saldo_8000[cuenta_col_pot].isin(compras_antes_saldo)
            ].copy()
            print(f"Saldo >= 8000 que NO compraron antes del 01/09/2025: {len(clientes_saldo_8000_no_compraron_antes)}")
            
            # 4. De los que NO compraron antes, cuántos compraron después
            compras_despues_saldo = df_dashboard[
                (df_dashboard[cuenta_col_dash].isin(clientes_saldo_8000_no_compraron_antes[cuenta_col_pot])) &
                (df_dashboard['F. Registro'].notna()) &
                (df_dashboard['F. Registro'] >= fecha_corte)
            ]
            
            cuentas_compraron_despues_saldo = compras_despues_saldo[cuenta_col_dash].unique()
            clientes_saldo_8000_compraron_despues = clientes_saldo_8000_no_compraron_antes[
                clientes_saldo_8000_no_compraron_antes[cuenta_col_pot].isin(cuentas_compraron_despues_saldo)
            ].copy()
            print(f"Saldo >= 8000 que compraron DESPUÉS del 01/09/2025: {len(clientes_saldo_8000_compraron_despues)}")
            
            # 5. Crear evolutivo diario Saldo >= 8000
            evolutivo_saldo = crear_evolutivo_diario(compras_despues_saldo, "Saldo >= 8000")
            
            # 6. Crear desglose por canal Saldo >= 8000
            desglose_canal_saldo = crear_desglose_canal(compras_despues_saldo, "Saldo >= 8000")
            
            saldo_8000_results = {
                'total': len(clientes_saldo_8000),
                'no_compraron_antes': len(clientes_saldo_8000_no_compraron_antes),
                'compraron_despues': len(clientes_saldo_8000_compraron_despues)
            }
        else:
            print("ADVERTENCIA: No se encontró la columna Saldo Créd")
        
        # ========== EVOLUTIVO GENERAL (TODOS LOS CLIENTES POSTERIORES) ==========
        print("\n" + "="*50)
        print("EVOLUTIVO GENERAL - TODAS LAS COMPRAS POSTERIORES")
        print("="*50)
        
        # Todas las compras posteriores al 01/09/2025
        todas_compras_posteriores = df_dashboard[
            (df_dashboard['F. Registro'].notna()) &
            (df_dashboard['F. Registro'] >= fecha_corte)
        ]
        
        evolutivo_general = crear_evolutivo_diario(todas_compras_posteriores, "Todas las compras posteriores")
        desglose_canal_general = crear_desglose_canal(todas_compras_posteriores, "Todas las compras posteriores")
        
        # ========== CREAR RESUMEN FINAL ==========
        resultados = {
            'Métrica': [],
            'Cantidad': []
        }
        
        # Agregar métricas NSE
        if nse_col:
            resultados['Métrica'].extend([
                'Total NSE 4 y 5',
                'NSE 4-5: NO compraron antes del 01/09/2025',
                'NSE 4-5: Compraron DESPUÉS del 01/09/2025'
            ])
            resultados['Cantidad'].extend([
                nse_4_5_results['total'],
                nse_4_5_results['no_compraron_antes'],
                nse_4_5_results['compraron_despues']
            ])
        
        # Agregar métricas Saldo
        if saldo_col:
            resultados['Métrica'].extend([
                'Total Saldo Créd >= 8,000',
                'Saldo >= 8,000: NO compraron antes del 01/09/2025',
                'Saldo >= 8,000: Compraron DESPUÉS del 01/09/2025'
            ])
            resultados['Cantidad'].extend([
                saldo_8000_results['total'],
                saldo_8000_results['no_compraron_antes'],
                saldo_8000_results['compraron_despues']
            ])
        
        # Agregar métricas generales
        resultados['Métrica'].extend([
            'Total clientes con compras posteriores al 01/09/2025'
        ])
        resultados['Cantidad'].extend([
            len(todas_compras_posteriores[cuenta_col_dash].unique())
        ])
        
        df_resultados = pd.DataFrame(resultados)
        
        # Mostrar resultados finales
        print("\n" + "="*60)
        print("RESULTADOS FINALES DEL ANÁLISIS")
        print("="*60)
        for i, row in df_resultados.iterrows():
            print(f"{row['Métrica']}: {row['Cantidad']}")
        
        # Guardar en Excel
        nombre_archivo = f"Analisis_Clientes_Potenciales_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        ruta_completa = os.path.join(ruta_salida, nombre_archivo)
        
        with pd.ExcelWriter(ruta_completa, engine='openpyxl') as writer:
            # Hoja de resumen principal
            df_resultados.to_excel(writer, sheet_name='Resumen', index=False)
            
            # Evolutivos diarios
            if len(evolutivo_general) > 0:
                evolutivo_general.to_excel(writer, sheet_name='Evolutivo_General', index=False)
            if len(evolutivo_nse) > 0:
                evolutivo_nse.to_excel(writer, sheet_name='Evolutivo_NSE_4_5', index=False)
            if len(evolutivo_saldo) > 0:
                evolutivo_saldo.to_excel(writer, sheet_name='Evolutivo_Saldo_8000', index=False)
            
            # Desgloses por canal
            if len(desglose_canal_general) > 0:
                desglose_canal_general.to_excel(writer, sheet_name='Canal_General', index=False)
            if len(desglose_canal_nse) > 0:
                desglose_canal_nse.to_excel(writer, sheet_name='Canal_NSE_4_5', index=False)
            if len(desglose_canal_saldo) > 0:
                desglose_canal_saldo.to_excel(writer, sheet_name='Canal_Saldo_8000', index=False)
            
            # Hojas NSE 4-5
            if nse_col and len(clientes_nse_4_5) > 0:
                clientes_nse_4_5.to_excel(writer, sheet_name='NSE_4_5_Todos', index=False)
                if len(clientes_nse_4_5_no_compraron_antes) > 0:
                    clientes_nse_4_5_no_compraron_antes.to_excel(writer, sheet_name='NSE_4_5_No_Compraron_Antes', index=False)
                if len(clientes_nse_4_5_compraron_despues) > 0:
                    clientes_nse_4_5_compraron_despues.to_excel(writer, sheet_name='NSE_4_5_Compraron_Despues', index=False)
            
            # Hojas Saldo >= 8000
            if saldo_col and len(clientes_saldo_8000) > 0:
                clientes_saldo_8000.to_excel(writer, sheet_name='Saldo_8000_Todos', index=False)
                if len(clientes_saldo_8000_no_compraron_antes) > 0:
                    clientes_saldo_8000_no_compraron_antes.to_excel(writer, sheet_name='Saldo_8000_No_Compraron_Antes', index=False)
                if len(clientes_saldo_8000_compraron_despues) > 0:
                    clientes_saldo_8000_compraron_despues.to_excel(writer, sheet_name='Saldo_8000_Compraron_Despues', index=False)
        
        print(f"\nArchivo guardado exitosamente en: {ruta_completa}")
        print("\nHojas creadas en el Excel:")
        print("- Resumen: Métricas principales")
        print("- Evolutivo_General: Evolución diaria de todas las compras posteriores")
        print("- Evolutivo_NSE_4_5: Evolución diaria de compras NSE 4-5")
        print("- Evolutivo_Saldo_8000: Evolución diaria de compras Saldo >= 8000")
        print("- Canal_General: Desglose por canal de todas las compras")
        print("- Canal_NSE_4_5: Desglose por canal de compras NSE 4-5")
        print("- Canal_Saldo_8000: Desglose por canal de compras Saldo >= 8000")
        print("- Hojas detalladas de cada segmento de clientes (con columna Canal)")
        
        # Mostrar resumen final de verificación
        print("\n" + "="*60)
        print("VERIFICACIÓN FINAL DE CONSISTENCIA")
        print("="*60)
        if nse_col and len(clientes_nse_4_5_compraron_despues) > 0:
            print(f"NSE 4-5 que compraron después:")
            print(f"  - Resumen: {nse_4_5_results['compraron_despues']}")
            print(f"  - Hoja detalle: {len(clientes_nse_4_5_compraron_despues)}")
            print(f"  - Evolutivo: {len(compras_despues_nse[cuenta_col_dash].unique()) if len(compras_despues_nse) > 0 else 0}")
        
        if saldo_col and len(clientes_saldo_8000_compraron_despues) > 0:
            print(f"Saldo >= 8000 que compraron después:")
            print(f"  - Resumen: {saldo_8000_results['compraron_despues']}")
            print(f"  - Hoja detalle: {len(clientes_saldo_8000_compraron_despues)}")
            print(f"  - Evolutivo: {len(compras_despues_saldo[cuenta_col_dash].unique()) if len(compras_despues_saldo) > 0 else 0}")
        
        print("\nTodos los números deberían coincidir ahora. Si hay diferencias:")
        print("- Un cliente puede haber comprado por múltiples canales")
        print("- Un cliente puede haber hecho múltiples compras en diferentes fechas")
        print("- Verificar que no haya duplicados en los datos origen")
        
    except FileNotFoundError as e:
        print(f"ERROR: No se encontró el archivo: {e}")
    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analizar_clientes_potenciales()
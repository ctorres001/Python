from __future__ import annotations
import os
import re
from pathlib import Path
from typing import Optional, Tuple, Dict, List, Set
import polars as pl
from datetime import datetime
import tempfile

# =============================
# CONFIGURACIÓN
# =============================
CARPETA_OFFLINE = Path(r"D:\FNB\Reportes\04 Reporte Clientes Potenciales\Historico OFFLINE")
ARCHIVO_SEGMENTACION = Path(r"D:\FNB\Reportes\25. Segmentación")

# Patrones para extraer fechas de nombres de archivo
PATRON_FECHA = re.compile(r"(20\d{6})")

# Posibles nombres de columna interlocutor
COLUMNAS_INTERLOCUTOR = [
    "interloc.comercial", "interloc comercial", "interlocutor comercial",
    "int.cial.", "int cial", "intcial", "int.cial",
    "interlocutor", "interloc", "comercial"
]

# Columnas de segmentación
SEG_INTERLOCUTOR = "INTERLOCUTOR"
SEG_FLAG = "FLAG_SEGMENTACION" 
SEG_FECHA = "FECHA_CORTE"

# Archivo de salida
ARCHIVO_SALIDA = "Evolutivo_OFFLINE_vs_Segmentacion.xlsx"


# =============================
# UTILIDADES BÁSICAS
# =============================
def extraer_fecha_archivo(nombre_archivo: str) -> Optional[str]:
    match = PATRON_FECHA.search(nombre_archivo)
    if match:
        fecha_completa = match.group(1)  # YYYYMMDD
        return fecha_completa[:6]        # YYYYMM
    return None


def formato_mes(yyyymm: str) -> str:
    return f"{yyyymm[:4]}-{yyyymm[4:6]}"


def normalizar_interlocutor(valor: str) -> str:
    if not valor or valor.strip() == "":
        return ""
    return str(valor).strip().upper()


def log_info(mensaje: str, nivel: int = 0):
    indent = "  " * nivel
    print(f"{indent}{mensaje}")


# =============================
# DETECCIÓN AUTOMÁTICA DE FORMATO
# =============================
def detectar_formato_archivo(archivo: Path) -> Dict:
    log_info(f"🔍 Detectando formato de: {archivo.name}")
    encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']
    delimitadores_cand = ['\t', '|', ';', ',', '  ', '   ', ' ', 'REGEX_SPACES']

    mejor = None
    mejor_score = -10**9

    for encoding in encodings:
        try:
            with open(archivo, 'r', encoding=encoding, errors='replace') as f:
                lines = [f.readline().rstrip('\n') for _ in range(20)]
        except Exception:
            continue

        for delim in delimitadores_cand:
            for fila_header in [0, 8]:  # 👈 SOLO fila 1 o 9
                if fila_header >= len(lines):
                    continue
                line = lines[fila_header]

                if delim == 'REGEX_SPACES':
                    parts = [p.strip() for p in re.split(r'\s{2,}|\t', line.strip()) if p.strip()]
                else:
                    parts = [p.strip() for p in line.split(delim) if p.strip()]

                if len(parts) < 3:
                    continue

                # Buscar columna interlocutor
                col_interlocutor = None
                for i, col in enumerate(parts):
                    col_norm = col.lower().replace('.', '').replace(' ', '')
                    for patron in COLUMNAS_INTERLOCUTOR:
                        if patron.replace('.', '').replace(' ', '') in col_norm:
                            col_interlocutor = col
                            break
                    if col_interlocutor:
                        break

                if col_interlocutor:
                    score = len(parts) + (10 - fila_header)
                    if score > mejor_score:
                        mejor_score = score
                        mejor = {
                            'encoding': encoding,
                            'delimitador': '\t' if delim == 'REGEX_SPACES' else delim,
                            'use_regex_split': (delim == 'REGEX_SPACES'),
                            'fila_header': fila_header,
                            'columnas': parts,
                            'columna_interlocutor': col_interlocutor,
                            'indice_interlocutor': parts.index(col_interlocutor),
                        }

    if mejor:
        log_info(f"  ✅ Formato detectado:", 1)
        log_info(f"    - Encoding: {mejor['encoding']}", 1)
        log_info(f"    - Delimitador: '{mejor['delimitador']}'", 1)
        log_info(f"    - Header en fila: {mejor['fila_header']+1}", 1)  # mostrar humano (1 o 9)
        log_info(f"    - Columna interlocutor: '{mejor['columna_interlocutor']}'", 1)
        log_info(f"    - Total columnas: {len(mejor['columnas'])}", 1)

    return mejor



def preprocessar_archivo_si_corresponde(archivo: Path, config: Dict) -> Path:
    if not config or not config.get('use_regex_split', False):
        return archivo

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".txt", mode='w', encoding=config['encoding'])
    tmp_path = Path(tmp.name)
    with open(archivo, 'r', encoding=config['encoding'], errors='replace') as fr, open(tmp_path, 'w', encoding=config['encoding']) as fw:
        for line in fr:
            new = re.sub(r'\s{2,}', '\t', line.rstrip('\n'))
            fw.write(new + "\n")
    tmp.close()
    return tmp_path


# =============================
# PROCESAMIENTO DE ARCHIVOS OFFLINE
# =============================
def procesar_archivo_offline(archivo: Path) -> Tuple[Set[str], str, int]:
    log_info(f"📂 Procesando OFFLINE: {archivo.name}")

    mes = extraer_fecha_archivo(archivo.name)
    if not mes:
        raise ValueError(f"No se pudo extraer fecha del archivo: {archivo.name}")

    config = detectar_formato_archivo(archivo)
    if not config:
        raise ValueError(f"No se pudo detectar formato válido para: {archivo.name}")

    archivo_a_leer = preprocessar_archivo_si_corresponde(archivo, config)

    interlocutores_unicos = set()
    total_registros = 0

    log_info("🔄 Extrayendo interlocutores únicos...", 1)

    try:
        df = pl.read_csv(
            archivo_a_leer,
            separator=config['delimitador'],
            skip_rows=config['fila_header'],
            has_header=True,
            encoding=config['encoding'],
            ignore_errors=True,
            null_values=["", "NA", "N/A", "null", "None", "NULL"],
            low_memory=False
        )

        log_info(f"    - Columnas detectadas por Polars: {df.columns}", 2)
        try:
            muestra_rows = df.head(5).to_dicts()
            log_info(f"    - Primeras filas (muestra): {muestra_rows}", 2)
        except Exception:
            pass

        if config['columna_interlocutor'] not in df.columns:
            col_encontrada = None
            for col in df.columns:
                col_norm = col.lower().replace('.', '').replace(' ', '').replace('_', '')
                config_norm = config['columna_interlocutor'].lower().replace('.', '').replace(' ', '').replace('_', '')
                if col_norm == config_norm or config_norm in col_norm or col_norm in config_norm:
                    col_encontrada = col
                    break
            if not col_encontrada:
                raise ValueError(f"Columna '{config['columna_interlocutor']}' no encontrada en DataFrame: {df.columns}")
            config['columna_interlocutor'] = col_encontrada

        total_registros = df.height

        df_unicos = (
            df
            .select(pl.col(config['columna_interlocutor']).alias('interlocutor'))
            .filter(
                pl.col('interlocutor').is_not_null() &
                (pl.col('interlocutor').cast(pl.Utf8).str.strip_chars() != "")
            )
            .with_columns(
                pl.col('interlocutor').cast(pl.Utf8).str.strip_chars().str.to_uppercase().alias('interlocutor')
            )
            .filter(pl.col('interlocutor') != "")
            .unique()
        )

        interlocutores_unicos = set(df_unicos['interlocutor'].to_list())

    except Exception as e:
        log_info(f"  ❌ Error con Polars, intentando método manual: {e}", 1)
        with open(archivo_a_leer, 'r', encoding=config['encoding'], errors='replace') as f:
            # Saltar hasta header correcto
            for _ in range(config['fila_header']):
                next(f)

            # Leer la línea del header real
            header_line = next(f).strip()
            header_cols = re.split(r'\s{2,}|\t', header_line)
            col_map = {c.strip().upper(): i for i, c in enumerate(header_cols)}

            # Buscar índice exacto de "INT.CIAL."
            idx_interlocutor = col_map.get("INT.CIAL.", config['indice_interlocutor'])

            for linea in f:
                total_registros += 1
                try:
                    campos = [campo.strip() for campo in re.split(r'\s{2,}|\t', linea.strip())]
                    if len(campos) > idx_interlocutor:
                        interlocutor = normalizar_interlocutor(campos[idx_interlocutor])
                        if interlocutor:
                            interlocutores_unicos.add(interlocutor)
                except Exception:
                    continue


    if archivo_a_leer != archivo:
        try:
            os.remove(archivo_a_leer)
        except Exception:
            pass

    unicos_count = len(interlocutores_unicos)
    log_info(f"  ✅ Completado:", 1)
    log_info(f"    - Total registros: {total_registros:,}", 1)
    log_info(f"    - Únicos encontrados: {unicos_count:,}", 1)
    log_info(f"    - Mes: {formato_mes(mes)}", 1)
    muestra = list(interlocutores_unicos)[:10]
    log_info(f"    - Muestra de interlocutores: {muestra}", 1)

    return interlocutores_unicos, mes, total_registros


# =============================
# PROCESAMIENTO DE SEGMENTACIÓN
# =============================
def cargar_segmentacion_mes(ruta_segmentacion: Path, mes: str) -> Dict[str, str]:
    log_info(f"🎯 Cargando segmentación para {formato_mes(mes)}")
    if ruta_segmentacion.is_file():
        archivos = [ruta_segmentacion]
    else:
        archivos = list(ruta_segmentacion.glob("*.txt"))

    if not archivos:
        log_info("  ⚠️ No se encontraron archivos de segmentación", 1)
        return {}

    segmentacion = {}
    for archivo in archivos:
        log_info(f"  📄 Procesando: {archivo.name}", 1)
        try:
            df = pl.read_csv(
                archivo,
                separator='\t',
                has_header=True,
                encoding='utf8-lossy',
                ignore_errors=True,
                null_values=["", "NA", "N/A", "null", "None"],
                low_memory=False
            )
            cols_lower = {col.lower(): col for col in df.columns}
            col_interlocutor = cols_lower.get('interlocutor') or cols_lower.get(SEG_INTERLOCUTOR.lower())
            col_flag = cols_lower.get('flag_segmentacion') or cols_lower.get(SEG_FLAG.lower())
            col_fecha = cols_lower.get('fecha_corte') or cols_lower.get(SEG_FECHA.lower())
            if not all([col_interlocutor, col_flag, col_fecha]):
                log_info(f"    ❌ Faltan columnas necesarias en {archivo.name}", 1)
                continue

            df_filtrado = (
                df
                .with_columns([
                    pl.when(pl.col(col_fecha).cast(pl.Utf8).str.contains("-"))
                    .then(pl.col(col_fecha).cast(pl.Utf8).str.strptime(pl.Date, "%Y-%m-%d", strict=False))
                    .when(pl.col(col_fecha).cast(pl.Utf8).str.contains("/"))
                    .then(pl.col(col_fecha).cast(pl.Utf8).str.strptime(pl.Date, "%d/%m/%Y", strict=False))
                    .otherwise(None)
                    .alias('fecha_parsed')
                ])
                .filter(pl.col('fecha_parsed').is_not_null())
                .with_columns(pl.col('fecha_parsed').dt.strftime("%Y%m").alias('mes'))
                .filter(pl.col('mes') == mes)
                .select([
                    pl.col(col_interlocutor).cast(pl.Utf8).str.strip_chars().str.to_uppercase().alias('interlocutor'),
                    pl.col(col_flag).cast(pl.Utf8).str.strip_chars().str.to_uppercase().alias('flag')
                ])
                .filter(pl.col('interlocutor').is_not_null() & (pl.col('interlocutor') != "") & pl.col('flag').is_not_null())
                .unique(subset=['interlocutor'])
            )
            for row in df_filtrado.iter_rows(named=True):
                segmentacion[row['interlocutor']] = row['flag']
            log_info(f"    ✅ Registros cargados: {df_filtrado.height:,}", 1)
        except Exception as e:
            log_info(f"    ❌ Error procesando {archivo.name}: {e}", 1)
            continue

    log_info(f"  ✅ Total segmentación cargada: {len(segmentacion):,} registros", 1)
    return segmentacion


# =============================
# CRUCE DE DATOS
# =============================
def cruzar_offline_segmentacion(interlocutores_offline: Set[str], segmentacion: Dict[str, str], mes: str) -> Dict[str, int]:
    log_info(f"🔗 Cruzando datos para {formato_mes(mes)}")
    conteos_flag = {}
    for interlocutor in interlocutores_offline:
        flag = segmentacion.get(interlocutor, "SIN_FLAG")
        conteos_flag[flag] = conteos_flag.get(flag, 0) + 1
    log_info(f"  ✅ Distribución de flags:", 1)
    for flag, cantidad in sorted(conteos_flag.items()):
        log_info(f"    - {flag}: {cantidad:,}", 1)
    return conteos_flag


# =============================
# GENERACIÓN DE REPORTES
# =============================
def generar_reporte_excel(datos_evolutivo: List[Dict], datos_flags: List[Dict], ruta_salida: Path):
    log_info("📊 Generando reporte Excel...")
    df_evolutivo = pl.DataFrame(datos_evolutivo).sort('mes')
    df_flags_raw = pl.DataFrame(datos_flags)
    if df_flags_raw.height > 0:
        df_flags_pivot = df_flags_raw.pivot(values='cantidad', index='flag', on='mes', aggregate_function='sum').sort('flag')
        meses_cols = [col for col in df_flags_pivot.columns if col != 'flag']
        for col in meses_cols:
            df_flags_pivot = df_flags_pivot.with_columns(pl.col(col).fill_null(0))
        df_flags_pivot = df_flags_pivot.with_columns(pl.sum_horizontal(pl.exclude('flag')).alias('TOTAL'))
        totales_data = {'flag': ['TOTAL_GENERAL']}
        for col in df_flags_pivot.columns:
            if col != 'flag':
                totales_data[col] = [df_flags_pivot[col].sum()]
        df_totales = pl.DataFrame(totales_data)
        df_flags_pivot = pl.concat([df_flags_pivot, df_totales], how='diagonal')
    else:
        df_flags_pivot = pl.DataFrame({'flag': [], 'TOTAL': []})
    try:
        archivo_evolutivo = ruta_salida.parent / "Evolutivo_Unicos.xlsx"
        archivo_flags = ruta_salida.parent / "Evolutivo_Flags.xlsx"
        df_evolutivo.write_excel(archivo_evolutivo, worksheet="Evolutivo_Unicos")
        df_flags_pivot.write_excel(archivo_flags, worksheet="Evolutivo_Flags")
        log_info(f"  ✅ Archivos generados:", 1)
        log_info(f"    📈 {archivo_evolutivo}", 1)
        log_info(f"    📊 {archivo_flags}", 1)
        return archivo_evolutivo, archivo_flags
    except Exception as e:
        log_info(f"  ❌ Error generando Excel: {e}", 1)
        return None, None


# =============================
# FUNCIÓN PRINCIPAL
# =============================
def main():
    print("=" * 80)
    print("🚀 EVOLUTIVO OFFLINE vs SEGMENTACIÓN")
    print("=" * 80)
    inicio = datetime.now()
    archivos_offline = list(CARPETA_OFFLINE.glob("*.txt"))
    if not archivos_offline:
        log_info("❌ No se encontraron archivos OFFLINE")
        return
    log_info(f"📁 Archivos OFFLINE encontrados: {len(archivos_offline)}")
    for archivo in archivos_offline:
        log_info(f"  - {archivo.name}", 1)
    datos_evolutivo = []
    datos_flags = []
    for archivo in archivos_offline:
        try:
            log_info(f"\n📋 PROCESANDO: {archivo.name}")
            log_info("-" * 60)
            interlocutores_unicos, mes, total_registros = procesar_archivo_offline(archivo)
            datos_evolutivo.append({'mes': formato_mes(mes), 'total_registros': total_registros, 'unicos': len(interlocutores_unicos)})
            segmentacion = cargar_segmentacion_mes(ARCHIVO_SEGMENTACION, mes)
            conteos_flag = cruzar_offline_segmentacion(interlocutores_unicos, segmentacion, mes)
            for flag, cantidad in conteos_flag.items():
                datos_flags.append({'mes': formato_mes(mes), 'flag': flag, 'cantidad': cantidad})
        except Exception as e:
            log_info(f"❌ Error procesando {archivo.name}: {e}")
            continue
    log_info(f"\n📊 GENERANDO REPORTE FINAL")
    log_info("-" * 60)
    ruta_salida = CARPETA_OFFLINE / ARCHIVO_SALIDA
    archivo_evolutivo, archivo_flags = generar_reporte_excel(datos_evolutivo, datos_flags, ruta_salida)
    fin = datetime.now()
    duracion = fin - inicio
    print("\n" + "=" * 80)
    print("🎉 PROCESO COMPLETADO")
    print("=" * 80)
    log_info(f"⏱️ Tiempo total: {duracion}")
    log_info(f"📊 Archivos procesados: {len([d for d in datos_evolutivo])}")
    if datos_evolutivo:
        log_info(f"📈 Resumen evolutivo:")
        for dato in datos_evolutivo:
            log_info(f"  {dato['mes']}: {dato['unicos']:,} únicos ({dato['total_registros']:,} registros)", 1)


if __name__ == "__main__":
    main()

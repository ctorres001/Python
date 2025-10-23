# core/queries.py
"""
Capa de acceso a datos para la app Control de Actividades (Neon - PostgreSQL).
Lecturas: se asume que `conn` es un objeto devuelto por st.connection("neon_db", type="sql")
          que implementa .query(sql, params=..., ttl=...)
Escrituras: se usa SQLAlchemy engine obtenido desde core.engine_connection.get_engine()
            para realizar transacciones con begin() y RETURNING cuando aplica.
"""

from datetime import datetime, time as dt_time
import pandas as pd
from sqlalchemy import text
from core.engine_connection import get_engine


# ---------------------------
# === ASESOR (Operaciones) ===
# ---------------------------

def get_user_by_username(conn, username):
    """
    Devuelve fila con datos del usuario (incluye rol y campaña).
    Retorna un DataFrame o None si no existe.
    """
    df = conn.query(
        """
        SELECT u.*, r.nombre as rol_nombre, c.nombre as campaña_nombre
        FROM usuarios u
        LEFT JOIN roles r ON u.rol_id = r.id
        LEFT JOIN campañas c ON u.campaña_id = c.id
        WHERE u.nombre_usuario = :username
        """,
        params={"username": username},
        ttl=0
    )
    return df.to_dict('records')[0] if not df.empty else None


def get_active_activities(conn):
    """
    Lista de actividades activas ordenadas (id, nombre_actividad, orden, etc.)
    """
    return conn.query(
        "SELECT id, nombre_actividad, orden FROM actividades WHERE activo = TRUE ORDER BY orden, id",
        ttl=600
    )


def get_subactivities(conn, activity_id):
    """
    Subactividades activas para una actividad.
    Devuelve DataFrame con columnas: id, nombre_subactividad, orden, activo.
    """
    df = conn.query(
        """
        SELECT id, nombre_subactividad, orden, activo
        FROM subactividades
        WHERE actividad_id = :activity_id
          AND activo = TRUE
        ORDER BY orden, nombre_subactividad
        """,
        params={"activity_id": activity_id},
        ttl=600
    )
    return df


def get_activity_by_name(conn, activity_name):
    """
    Devuelve id de actividad por nombre (o None).
    """
    df = conn.query(
        "SELECT id FROM actividades WHERE nombre_actividad = :name LIMIT 1",
        params={"name": activity_name},
        ttl=600
    )
    return int(df['id'].iloc[0]) if not df.empty else None


def get_last_activity_status(conn, user_id):
    """
    Último registro de actividad (más reciente) para un usuario.
    """
    df = conn.query(
        """
        SELECT r.*, a.nombre_actividad, s.nombre_subactividad
        FROM registro_actividades r
        JOIN actividades a ON r.actividad_id = a.id
        LEFT JOIN subactividades s ON r.subactividad_id = s.id
        WHERE r.usuario_id = :user_id
        ORDER BY r.id DESC
        LIMIT 1
        """,
        params={"user_id": user_id},
        ttl=0
    )
    return df.to_dict('records')[0] if not df.empty else None


def get_open_activity(conn, user_id, date):
    """
    Obtiene la actividad abierta del usuario (si existe) para la fecha indicada.
    Retorna DataFrame (puede venir vacío).
    """
    df = conn.query(
        """
        SELECT 
            r.id, 
            r.actividad_id, 
            r.hora_inicio, 
            r.subactividad_id,
            s.nombre_subactividad as subactividad,
            r.observaciones as comentario,
            a.nombre_actividad
        FROM registro_actividades r
        JOIN actividades a ON r.actividad_id = a.id
        LEFT JOIN subactividades s ON r.subactividad_id = s.id
        WHERE r.usuario_id = :user_id 
          AND r.hora_fin IS NULL
          AND r.fecha = :date
        ORDER BY r.hora_inicio DESC
        LIMIT 1
        """,
        params={"user_id": user_id, "date": date},
        ttl=0
    )
    return df if not df.empty else pd.DataFrame()


def start_activity(conn, user_id, actividad_id, hora_inicio, subactividad_id=None, comentario=None):
    """
    Inserta un nuevo registro en registro_actividades y retorna el id del registro creado.
    Usa engine (SQLAlchemy) para asegurar commit y RETURNING.
    CORREGIDO: Usa CURRENT_TIMESTAMP de PostgreSQL en lugar de hora de Python
    """
    engine = get_engine()
    try:
        with engine.begin() as connection:
            result = connection.execute(
                text("""
                    INSERT INTO public.registro_actividades
                    (usuario_id, actividad_id, fecha, hora_inicio, subactividad_id, observaciones, estado)
                    VALUES (:user_id, :actividad_id, CURRENT_DATE, CURRENT_TIMESTAMP, :subactividad_id, :comentario, 'Iniciado')
                    RETURNING id
                """),
                {
                    "user_id": int(user_id),
                    "actividad_id": int(actividad_id),
                    "subactividad_id": int(subactividad_id) if subactividad_id is not None else None,
                    "comentario": comentario if comentario else None
                }
            )
            registro_id = result.scalar()
            return int(registro_id) if registro_id is not None else None
    except Exception as e:
        raise Exception(f"Error en start_activity: {e}")


def stop_activity(conn, registro_id, hora_fin):
    """
    Actualiza registro para asignar hora_fin y calcular duración en segundos y formato hms.
    Usa engine para garantizar transacción.
    CORREGIDO: Usa CURRENT_TIMESTAMP de PostgreSQL en lugar de hora de Python
    """
    engine = get_engine()
    try:
        with engine.begin() as connection:
            connection.execute(
                text("""
                    UPDATE public.registro_actividades
                    SET 
                        hora_fin = CURRENT_TIMESTAMP,
                        estado = 'Finalizado',
                        duracion_seg = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - hora_inicio)),
                        duracion_hms = (CURRENT_TIMESTAMP - hora_inicio)
                    WHERE id = :registro_id
                      AND hora_fin IS NULL
                """),
                {"registro_id": registro_id}
            )
    except Exception as e:
        raise Exception(f"Error en stop_activity: {e}")


def close_previous_day_activities(conn, user_id, previous_date):
    """
    Cierra todas las actividades abiertas de previous_date estableciendo hora_fin a 23:59:59.
    Retorna cantidad de filas afectadas.
    """
    engine = get_engine()
    try:
        with engine.begin() as connection:
            end_time = datetime.combine(previous_date, dt_time(23, 59, 59))
            result = connection.execute(
                text("""
                    UPDATE public.registro_actividades
                    SET 
                        hora_fin = :hora_fin,
                        estado = 'Cerrado Automático',
                        duracion_seg = EXTRACT(EPOCH FROM (:hora_fin - hora_inicio)),
                        duracion_hms = (:hora_fin - hora_inicio)
                    WHERE usuario_id = :user_id
                      AND hora_fin IS NULL
                      AND fecha = :fecha
                """),
                {"user_id": user_id, "fecha": previous_date, "hora_fin": end_time}
            )
            return result.rowcount
    except Exception as e:
        raise Exception(f"Error en close_previous_day_activities: {e}")


def get_today_summary(conn, user_id, date):
    """
    Resumen del día con totales por actividad (incluye actividades en curso sumando NOW()).
    Retorna DataFrame con columnas: nombre_actividad, total_segundos
    CORREGIDO: Agrupa correctamente y ordena por tiempo descendente
    """
    df = conn.query(
        """
        SELECT 
            a.nombre_actividad,
            SUM(
                CASE 
                    WHEN r.hora_fin IS NULL THEN 
                        EXTRACT(EPOCH FROM (
                            CURRENT_TIMESTAMP - 
                            (r.hora_inicio AT TIME ZONE 'America/Lima')
                        ))
                    ELSE 
                        COALESCE(r.duracion_seg, 0)
                END
            ) as total_segundos
        FROM registro_actividades r 
        JOIN actividades a ON r.actividad_id = a.id 
        WHERE r.usuario_id = :user_id 
          AND r.fecha = :date
        GROUP BY a.nombre_actividad
        ORDER BY total_segundos DESC
        """,
        params={"user_id": user_id, "date": date},
        ttl=0  # Sin caché para datos en tiempo real
    )
    return df


def get_today_log(conn, user_id, date):
    """
    Log detallado del día. Columnas devueltas:
    nombre_actividad, subactividad, comentario, inicio (HH24:MI), duracion (HH24:MI:SS o 'En curso')
    """
    df = conn.query(
        """
        SELECT 
            a.nombre_actividad,
            s.nombre_subactividad as subactividad,
            r.observaciones as comentario,
            to_char(r.hora_inicio, 'HH24:MI') as inicio,
            CASE 
                WHEN r.hora_fin IS NULL THEN 'En curso'
                ELSE to_char(r.duracion_hms, 'HH24:MI:SS')
            END as duracion
        FROM registro_actividades r 
        JOIN actividades a ON r.actividad_id = a.id 
        LEFT JOIN subactividades s ON r.subactividad_id = s.id
        WHERE r.usuario_id = :user_id 
          AND r.fecha = :date
        ORDER BY r.hora_inicio DESC
        """,
        params={"user_id": user_id, "date": date},
        ttl=0
    )
    return df


def get_activity_stats(conn, user_id, start_date, end_date):
    """
    Estadísticas por actividad/subactividad en rango de fechas.
    Devuelve: nombre_actividad, subactividad, cantidad_registros, total_horas, promedio_minutos
    CORREGIDO: Fuerza timezone en cálculos
    """
    df = conn.query(
        """
        SELECT 
            a.nombre_actividad,
            s.nombre_subactividad as subactividad,
            COUNT(*) as cantidad_registros,
            SUM(
                CASE 
                    WHEN r.hora_fin IS NULL THEN 
                        EXTRACT(EPOCH FROM (
                            CURRENT_TIMESTAMP - 
                            (r.hora_inicio AT TIME ZONE 'America/Lima')
                        ))
                    ELSE 
                        COALESCE(r.duracion_seg, 0)
                END
            ) / 3600 as total_horas,
            AVG(
                CASE 
                    WHEN r.hora_fin IS NULL THEN 
                        EXTRACT(EPOCH FROM (
                            CURRENT_TIMESTAMP - 
                            (r.hora_inicio AT TIME ZONE 'America/Lima')
                        ))
                    ELSE 
                        COALESCE(r.duracion_seg, 0)
                END
            ) / 60 as promedio_minutos
        FROM registro_actividades r
        JOIN actividades a ON r.actividad_id = a.id
        LEFT JOIN subactividades s ON r.subactividad_id = s.id
        WHERE r.usuario_id = :user_id
        AND r.fecha BETWEEN :start_date AND :end_date
        GROUP BY a.nombre_actividad, s.nombre_subactividad
        ORDER BY total_horas DESC
        """,
        params={"user_id": user_id, "start_date": start_date, "end_date": end_date},
        ttl=0
    )
    return df


def get_user_productivity_summary(conn, user_id, date):
    """
    Resumen de productividad del usuario en una fecha.
    Retorna: total_actividades, tipos_actividad, primera_actividad, ultima_actividad, horas_trabajadas
    CORREGIDO: Fuerza timezone en cálculos
    """
    df = conn.query(
        """
        SELECT 
            COUNT(*) as total_actividades,
            COUNT(DISTINCT actividad_id) as tipos_actividad,
            MIN(hora_inicio) as primera_actividad,
            MAX(COALESCE(hora_fin, CURRENT_TIMESTAMP)) as ultima_actividad,
            SUM(
                CASE 
                    WHEN hora_fin IS NULL THEN 
                        EXTRACT(EPOCH FROM (
                            CURRENT_TIMESTAMP - 
                            (hora_inicio AT TIME ZONE 'America/Lima')
                        ))
                    ELSE 
                        COALESCE(duracion_seg, 0)
                END
            ) / 3600 as horas_trabajadas
        FROM registro_actividades
        WHERE usuario_id = :user_id
        AND fecha = :date
        """,
        params={"user_id": user_id, "date": date},
        ttl=0
    )
    return df


# --------------------------------
# === SUPERVISOR (Reportes) ===
# --------------------------------

def get_supervisor_dashboard(conn, campaña_id, fecha):
    """
    Dashboard de supervisor para una campaña y fecha dada.
    Devuelve: nombre_completo, ingreso, salida, tiempo_total_jornada, tiempo_efectivo, estado_actual
    CORREGIDO: Fuerza timezone en cálculos
    """
    df = conn.query(
        """
        WITH Jornadas AS (
            SELECT
                r.usuario_id,
                MIN(r.hora_inicio) as hora_ingreso,
                MAX(COALESCE(r.hora_fin, CURRENT_TIMESTAMP)) as hora_salida
            FROM registro_actividades r
            WHERE r.fecha = :fecha
            GROUP BY r.usuario_id
        ),
        Efectivo AS (
            SELECT
                r.usuario_id,
                SUM(
                    CASE 
                        WHEN r.hora_fin IS NULL THEN 
                            EXTRACT(EPOCH FROM (
                                CURRENT_TIMESTAMP - 
                                (r.hora_inicio AT TIME ZONE 'America/Lima')
                            ))
                        ELSE 
                            COALESCE(r.duracion_seg, 0)
                    END
                ) as segundos_efectivos
            FROM registro_actividades r
            WHERE r.fecha = :fecha
            AND r.actividad_id NOT IN (
                SELECT id FROM actividades WHERE nombre_actividad IN ('Break Salida', 'Regreso Break')
            )
            GROUP BY r.usuario_id
        )
        SELECT 
            u.nombre_completo,
            to_char(j.hora_ingreso, 'HH24:MI:SS') as ingreso,
            to_char(j.hora_salida, 'HH24:MI:SS') as salida,
            (j.hora_salida - j.hora_ingreso) as tiempo_total_jornada,
            to_char((e.segundos_efectivos * interval '1 second'), 'HH24:MI:SS') as tiempo_efectivo,
            CASE 
                WHEN EXISTS (
                    SELECT 1 FROM registro_actividades 
                    WHERE usuario_id = u.id 
                    AND fecha = :fecha 
                    AND hora_fin IS NULL
                ) THEN 'En curso'
                ELSE 'Finalizado'
            END as estado_actual
        FROM usuarios u
        LEFT JOIN Jornadas j ON u.id = j.usuario_id
        LEFT JOIN Efectivo e ON u.id = e.usuario_id
        WHERE u.campaña_id = :campaña_id
          AND u.rol_id = (SELECT id FROM roles WHERE nombre = 'Asesor')
        ORDER BY u.nombre_completo;
        """,
        params={"campaña_id": campaña_id, "fecha": fecha},
        ttl=60
    )
    return df


def get_team_activity_breakdown(conn, campaña_id, fecha):
    """
    Desglose de actividades por asesor dentro de una campaña en una fecha.
    CORREGIDO: Fuerza timezone en cálculos
    """
    df = conn.query(
        """
        SELECT 
            u.nombre_completo,
            a.nombre_actividad,
            s.nombre_subactividad,
            COUNT(*) as cantidad,
            SUM(
                CASE 
                    WHEN r.hora_fin IS NULL THEN 
                        EXTRACT(EPOCH FROM (
                            CURRENT_TIMESTAMP - 
                            (r.hora_inicio AT TIME ZONE 'America/Lima')
                        ))
                    ELSE 
                        COALESCE(r.duracion_seg, 0)
                END
            ) / 3600 as horas_totales
        FROM registro_actividades r
        JOIN usuarios u ON r.usuario_id = u.id
        JOIN actividades a ON r.actividad_id = a.id
        LEFT JOIN subactividades s ON r.subactividad_id = s.id
        WHERE u.campaña_id = :campaña_id
        AND r.fecha = :fecha
        GROUP BY u.nombre_completo, a.nombre_actividad, s.nombre_subactividad
        ORDER BY u.nombre_completo, horas_totales DESC
        """,
        params={"campaña_id": campaña_id, "fecha": fecha},
        ttl=60
    )
    return df


# --------------------------------
# === ADMINISTRADOR (CRUD + Reportes) ===
# --------------------------------

def get_all_users_admin(conn):
    """
    Lista de usuarios con rol y campaña para la vista admin.
    """
    return conn.query("""
        SELECT u.id, u.nombre_usuario, u.nombre_completo, r.nombre as rol, c.nombre as campaña, u.estado 
        FROM usuarios u 
        LEFT JOIN roles r ON u.rol_id = r.id 
        LEFT JOIN campañas c ON u.campaña_id = c.id
        ORDER BY u.nombre_completo
    """, ttl=10)


def get_dropdown_data(conn):
    """
    Devuelve listas de roles y campañas para dropdowns.
    CORREGIDO: Removido filtro por 'activo' que no existe en las tablas
    """
    roles = conn.query("SELECT id, nombre FROM roles ORDER BY nombre", ttl=3600)
    campañas = conn.query("SELECT id, nombre FROM campañas ORDER BY nombre", ttl=3600)
    return roles.to_dict('records'), campañas.to_dict('records')


def check_username_exists(conn, username: str) -> bool:
    df = conn.query(
        "SELECT id FROM usuarios WHERE nombre_usuario = :username",
        params={"username": username},
        ttl=0
    )
    return not df.empty


def update_user_admin(conn, user_id, nombre_completo, rol_id, campaña_id, estado):
    """
    Actualiza datos básicos de usuario (para admin). Usa engine (transactional).
    """
    engine = get_engine()
    try:
        with engine.begin() as connection:
            connection.execute(
                text("""
                    UPDATE public.usuarios 
                    SET nombre_completo = :nc, rol_id = :ri, campaña_id = :ci, estado = :e
                    WHERE id = :uid
                """),
                {"nc": nombre_completo, "ri": rol_id, "ci": campaña_id, "e": estado, "uid": user_id}
            )
    except Exception as e:
        raise Exception(f"Error en update_user_admin: {e}")


def create_user_admin(conn, username, password, nombre_completo, rol_id, campaña_id):
    """
    Crea un usuario (admin). Usa engine.
    """
    engine = get_engine()
    try:
        with engine.begin() as connection:
            connection.execute(
                text("""
                    INSERT INTO public.usuarios 
                    (nombre_usuario, contraseña, nombre_completo, rol_id, campaña_id, estado)
                    VALUES (:u, :p, :nc, :ri, :ci, TRUE)
                """),
                {"u": username, "p": password, "nc": nombre_completo, "ri": rol_id, "ci": campaña_id}
            )
    except Exception as e:
        raise Exception(f"Error en create_user_admin: {e}")


def get_all_subactivities(conn):
    """
    Obtiene todas las subactividades con el nombre de su actividad padre.
    """
    df = conn.query(
        """
        SELECT 
            s.id,
            s.nombre_subactividad,
            a.nombre_actividad,
            s.activo,
            s.orden
        FROM subactividades s
        JOIN actividades a ON s.actividad_id = a.id
        ORDER BY a.nombre_actividad, s.orden
        """,
        ttl=300
    )
    return df


def create_subactivity(conn, actividad_id, nombre_subactividad, orden=0):
    """
    Inserta subactividad (usa engine).
    """
    engine = get_engine()
    try:
        with engine.begin() as connection:
            connection.execute(
                text("""
                    INSERT INTO public.subactividades 
                    (actividad_id, nombre_subactividad, orden, activo)
                    VALUES (:aid, :nombre, :orden, TRUE)
                """),
                {"aid": actividad_id, "nombre": nombre_subactividad, "orden": orden}
            )
    except Exception as e:
        raise Exception(f"Error en create_subactivity: {e}")


def update_subactivity(conn, subactivity_id, nombre_subactividad, activo, orden):
    engine = get_engine()
    try:
        with engine.begin() as connection:
            connection.execute(
                text("""
                    UPDATE public.subactividades 
                    SET nombre_subactividad = :nombre, activo = :activo, orden = :orden
                    WHERE id = :sid
                """),
                {"nombre": nombre_subactividad, "activo": activo, "orden": orden, "sid": subactivity_id}
            )
    except Exception as e:
        raise Exception(f"Error en update_subactivity: {e}")


def delete_subactivity(conn, subactivity_id):
    engine = get_engine()
    try:
        with engine.begin() as connection:
            connection.execute(
                text("UPDATE public.subactividades SET activo = FALSE WHERE id = :sid"),
                {"sid": subactivity_id}
            )
    except Exception as e:
        raise Exception(f"Error en delete_subactivity: {e}")


# --------------------------------
# === UTILIDADES / REPORTES VARIOS
# --------------------------------

def get_user_log(conn, user_id, start_date, end_date):
    """
    Histórico de actividades de un usuario en un rango.
    """
    df = conn.query(
        """
        SELECT 
            DATE(r.hora_inicio) AS fecha,
            a.nombre_actividad,
            COALESCE(s.nombre_subactividad, '-') AS subactividad,
            COALESCE(r.observaciones, '-') AS comentario,
            TO_CHAR(r.hora_inicio, 'HH24:MI') AS inicio,
            TO_CHAR(r.hora_fin, 'HH24:MI') AS fin,
            CASE
                WHEN r.hora_fin IS NULL THEN 'En curso'
                ELSE TO_CHAR((r.hora_fin - r.hora_inicio), 'HH24:MI:SS')
            END AS duracion
        FROM registro_actividades r
        JOIN actividades a ON a.id = r.actividad_id
        LEFT JOIN subactividades s ON s.id = r.subactividad_id
        WHERE r.usuario_id = :user_id
          AND r.fecha BETWEEN :start_date AND :end_date
        ORDER BY r.hora_inicio DESC
        """,
        params={"user_id": user_id, "start_date": start_date, "end_date": end_date},
        ttl=300
    )
    return df


def get_all_activities_admin(conn):
    """
    Lista de actividades (para CRUD admin).
    """
    return conn.query("SELECT * FROM actividades ORDER BY orden, id", ttl=300)


def validate_connection(conn):
    """
    Simple check de conexión (devuelve True/False).
    """
    try:
        _ = conn.query("SELECT 1", ttl=0)
        return True
    except Exception:
        return False
    
# ================================
# === FUNCIONES PARA ADMINISTRADOR ===
# ================================

# -------------------------------------
# Dashboard General - Queries de Reportes
# -------------------------------------

def get_admin_kpis(conn, start_date=None, end_date=None, campaña_id=None):
    """
    Obtiene KPIs generales para el dashboard del administrador
    """
    where_clauses = []
    params = {}
    
    if start_date and end_date:
        where_clauses.append("r.fecha BETWEEN :start_date AND :end_date")
        params["start_date"] = start_date
        params["end_date"] = end_date
    
    if campaña_id:
        where_clauses.append("u.campaña_id = :campaña_id")
        params["campaña_id"] = campaña_id
    
    where_sql = " AND " + " AND ".join(where_clauses) if where_clauses else ""
    
    df = conn.query(
        f"""
        SELECT 
            COUNT(DISTINCT u.id) as total_asesores,
            COUNT(DISTINCT u.campaña_id) as total_campañas,
            COUNT(DISTINCT r.id) as total_registros,
            SUM(COALESCE(r.duracion_seg, 0)) / 3600 as horas_totales,
            AVG(COALESCE(r.duracion_seg, 0)) / 60 as promedio_minutos
        FROM usuarios u
        LEFT JOIN registro_actividades r ON u.id = r.usuario_id
        WHERE u.rol_id = (SELECT id FROM roles WHERE nombre = 'Asesor')
        {where_sql}
        """,
        params=params,
        ttl=60
    )
    return df


def get_admin_dashboard_by_campaign(conn, start_date, end_date):
    """
    Resumen de actividades por campaña
    """
    df = conn.query(
        """
        SELECT 
            c.nombre as campaña,
            COUNT(DISTINCT u.id) as total_asesores,
            COUNT(DISTINCT r.id) as total_registros,
            SUM(COALESCE(r.duracion_seg, 0)) / 3600 as horas_totales,
            AVG(COALESCE(r.duracion_seg, 0)) / 60 as promedio_actividad_min
        FROM campañas c
        LEFT JOIN usuarios u ON c.id = u.campaña_id 
            AND u.rol_id = (SELECT id FROM roles WHERE nombre = 'Asesor')
        LEFT JOIN registro_actividades r ON u.id = r.usuario_id 
            AND r.fecha BETWEEN :start_date AND :end_date
        GROUP BY c.nombre
        ORDER BY horas_totales DESC
        """,
        params={"start_date": start_date, "end_date": end_date},
        ttl=60
    )
    return df


def get_admin_dashboard_by_asesor(conn, start_date, end_date, campaña_id=None):
    """
    Resumen de actividades por asesor
    """
    where_clause = ""
    params = {"start_date": start_date, "end_date": end_date}
    
    if campaña_id:
        where_clause = "AND u.campaña_id = :campaña_id"
        params["campaña_id"] = campaña_id
    
    df = conn.query(
        f"""
        SELECT 
            u.nombre_completo as asesor,
            c.nombre as campaña,
            COUNT(DISTINCT DATE(r.hora_inicio)) as dias_trabajados,
            COUNT(r.id) as total_actividades,
            SUM(COALESCE(r.duracion_seg, 0)) / 3600 as horas_totales,
            MIN(DATE(r.hora_inicio)) as primera_actividad,
            MAX(DATE(r.hora_inicio)) as ultima_actividad
        FROM usuarios u
        JOIN campañas c ON u.campaña_id = c.id
        LEFT JOIN registro_actividades r ON u.id = r.usuario_id 
            AND r.fecha BETWEEN :start_date AND :end_date
        WHERE u.rol_id = (SELECT id FROM roles WHERE nombre = 'Asesor')
        {where_clause}
        GROUP BY u.nombre_completo, c.nombre
        ORDER BY horas_totales DESC
        """,
        params=params,
        ttl=60
    )
    return df


def get_admin_activity_breakdown(conn, start_date, end_date, campaña_id=None):
    """
    Desglose de tiempo por tipo de actividad
    """
    where_clause = ""
    params = {"start_date": start_date, "end_date": end_date}
    
    if campaña_id:
        where_clause = "AND u.campaña_id = :campaña_id"
        params["campaña_id"] = campaña_id
    
    df = conn.query(
        f"""
        SELECT 
            a.nombre_actividad,
            COUNT(r.id) as cantidad,
            SUM(COALESCE(r.duracion_seg, 0)) / 3600 as horas_totales,
            AVG(COALESCE(r.duracion_seg, 0)) / 60 as promedio_minutos
        FROM actividades a
        LEFT JOIN registro_actividades r ON a.id = r.actividad_id 
            AND r.fecha BETWEEN :start_date AND :end_date
        LEFT JOIN usuarios u ON r.usuario_id = u.id
        WHERE 1=1 {where_clause}
        GROUP BY a.nombre_actividad
        ORDER BY horas_totales DESC
        """,
        params=params,
        ttl=60
    )
    return df


# -------------------------------------
# Gestión de Campañas
# -------------------------------------

def get_all_campaigns(conn):
    """Lista todas las campañas"""
    return conn.query("SELECT * FROM campañas ORDER BY nombre", ttl=300)


def create_campaign(conn, nombre):
    """Crea una nueva campaña"""
    engine = get_engine()
    try:
        with engine.begin() as connection:
            connection.execute(
                text("INSERT INTO campañas (nombre) VALUES (:nombre)"),
                {"nombre": nombre}
            )
        return True
    except Exception as e:
        raise Exception(f"Error al crear campaña: {e}")


def update_campaign(conn, campaign_id, nombre):
    """Actualiza una campaña"""
    engine = get_engine()
    try:
        with engine.begin() as connection:
            connection.execute(
                text("UPDATE campañas SET nombre = :nombre WHERE id = :id"),
                {"nombre": nombre, "id": campaign_id}
            )
        return True
    except Exception as e:
        raise Exception(f"Error al actualizar campaña: {e}")


def delete_campaign(conn, campaign_id):
    """Elimina una campaña (verifica que no tenga usuarios)"""
    engine = get_engine()
    try:
        with engine.begin() as connection:
            # Verificar si tiene usuarios
            result = connection.execute(
                text("SELECT COUNT(*) FROM usuarios WHERE campaña_id = :id"),
                {"id": campaign_id}
            )
            count = result.scalar()
            
            if count > 0:
                raise Exception(f"No se puede eliminar. Hay {count} usuarios asociados a esta campaña.")
            
            connection.execute(
                text("DELETE FROM campañas WHERE id = :id"),
                {"id": campaign_id}
            )
        return True
    except Exception as e:
        raise Exception(f"Error al eliminar campaña: {e}")


# -------------------------------------
# Gestión de Actividades
# -------------------------------------

def create_activity(conn, nombre_actividad, descripcion, orden):
    """Crea una nueva actividad"""
    engine = get_engine()
    try:
        with engine.begin() as connection:
            connection.execute(
                text("""
                    INSERT INTO actividades (nombre_actividad, descripcion, orden, activo)
                    VALUES (:nombre, :desc, :orden, TRUE)
                """),
                {"nombre": nombre_actividad, "desc": descripcion, "orden": orden}
            )
        return True
    except Exception as e:
        raise Exception(f"Error al crear actividad: {e}")


def update_activity(conn, activity_id, nombre_actividad, descripcion, orden, activo):
    """Actualiza una actividad"""
    engine = get_engine()
    try:
        with engine.begin() as connection:
            connection.execute(
                text("""
                    UPDATE actividades 
                    SET nombre_actividad = :nombre, 
                        descripcion = :desc, 
                        orden = :orden, 
                        activo = :activo
                    WHERE id = :id
                """),
                {"nombre": nombre_actividad, "desc": descripcion, "orden": orden, 
                 "activo": activo, "id": activity_id}
            )
        return True
    except Exception as e:
        raise Exception(f"Error al actualizar actividad: {e}")


def delete_activity(conn, activity_id):
    """Desactiva una actividad (soft delete)"""
    engine = get_engine()
    try:
        with engine.begin() as connection:
            connection.execute(
                text("UPDATE actividades SET activo = FALSE WHERE id = :id"),
                {"id": activity_id}
            )
        return True
    except Exception as e:
        raise Exception(f"Error al desactivar actividad: {e}")


# -------------------------------------
# Gestión de Roles
# -------------------------------------

def get_all_roles(conn):
    """Lista todos los roles"""
    return conn.query("SELECT * FROM roles ORDER BY nombre", ttl=300)


def create_role(conn, nombre):
    """Crea un nuevo rol"""
    engine = get_engine()
    try:
        with engine.begin() as connection:
            connection.execute(
                text("INSERT INTO roles (nombre) VALUES (:nombre)"),
                {"nombre": nombre}
            )
        return True
    except Exception as e:
        raise Exception(f"Error al crear rol: {e}")


def update_role(conn, role_id, nombre):
    """Actualiza un rol"""
    engine = get_engine()
    try:
        with engine.begin() as connection:
            connection.execute(
                text("UPDATE roles SET nombre = :nombre WHERE id = :id"),
                {"nombre": nombre, "id": role_id}
            )
        return True
    except Exception as e:
        raise Exception(f"Error al actualizar rol: {e}")


def delete_role(conn, role_id):
    """Elimina un rol (verifica que no tenga usuarios)"""
    engine = get_engine()
    try:
        with engine.begin() as connection:
            # Verificar si tiene usuarios
            result = connection.execute(
                text("SELECT COUNT(*) FROM usuarios WHERE rol_id = :id"),
                {"id": role_id}
            )
            count = result.scalar()
            
            if count > 0:
                raise Exception(f"No se puede eliminar. Hay {count} usuarios con este rol.")
            
            connection.execute(
                text("DELETE FROM roles WHERE id = :id"),
                {"id": role_id}
            )
        return True
    except Exception as e:
        raise Exception(f"Error al eliminar rol: {e}")
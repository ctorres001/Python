import streamlit as st
from datetime import datetime, time as dt_time
from sqlalchemy import text
import pandas as pd
from core.engine_connection import get_engine


# ======================================================
# 🧩 CONSULTAS DE USUARIO Y AUTENTICACIÓN
# ======================================================

def get_user_by_username(conn, username):
    """Obtiene la información completa de un usuario, su rol y campaña."""
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


# ======================================================
# 🧩 CONSULTAS DEL ASESOR
# ======================================================

def get_active_activities(conn):
    """Obtiene todas las actividades activas."""
    return conn.query("SELECT * FROM actividades WHERE activo = TRUE ORDER BY id", ttl=600) # CAMBIO AQUÍ


def get_subactivities(conn, activity_id):
    """
    Obtiene las subactividades disponibles para una actividad específica.
    """
    df = conn.query(
        """
        SELECT id, nombre_subactividad
        FROM subactividades
        WHERE actividad_id = :activity_id
        AND activo = TRUE
        ORDER BY orden
        """,
        params={"activity_id": activity_id},
        ttl=600
    )
    return df


def get_activity_by_name(conn, activity_name):
    """
    Obtiene el ID de una actividad por su nombre.
    """
    df = conn.query(
        "SELECT id FROM actividades WHERE nombre_actividad = :name",
        params={"name": activity_name},
        ttl=600
    )
    return df['id'].iloc[0] if not df.empty else None


def get_last_activity_status(conn, user_id):
    """Obtiene el último registro de actividad de un usuario."""
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


def get_open_activity(conn, user_id):
    """
    Obtiene la actividad abierta del usuario (si existe).
    Solo busca actividades del día actual sin hora_fin.
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
        AND r.fecha = CURRENT_DATE
        ORDER BY r.hora_inicio DESC
        LIMIT 1
        """,
        params={"user_id": user_id},
        ttl=0
    )
    return df if not df.empty else pd.DataFrame()


def start_activity(conn, user_id, actividad_id, hora_inicio, subactividad_id=None, comentario=None):
    """
    Inicia una nueva actividad con subactividad (ID) y comentario opcionales.
    """
    import sys
    engine = get_engine()
    try:
        with engine.begin() as connection:
            result = connection.execute(
                text("""
                    INSERT INTO public.registro_actividades 
                    (usuario_id, actividad_id, fecha, hora_inicio, subactividad_id, observaciones, estado) -- CAMBIO AQUÍ
                    VALUES (:user_id, :actividad_id, CURRENT_DATE, :hora_inicio, :subactividad_id, :comentario, 'Iniciado')
                    RETURNING id
                """),
                {
                    "user_id": user_id, 
                    "actividad_id": actividad_id, 
                    "hora_inicio": hora_inicio,
                    "subactividad_id": subactividad_id,
                    "comentario": comentario
                }
            )
            registro_id = result.scalar()
            print(f"DEBUG: Actividad iniciada con ID: {registro_id}, Subactividad ID: {subactividad_id}", file=sys.stderr)
            return registro_id
    except Exception as e:
        print(f"Error en start_activity: {e}", file=sys.stderr)
        raise


def stop_activity(conn, registro_id, hora_fin):
    """
    Finaliza una actividad y calcula duración.
    """
    engine = get_engine()
    try:
        with engine.begin() as connection:
            connection.execute(
                text("""
                    UPDATE public.registro_actividades 
                    SET 
                        hora_fin = :hora_fin, 
                        estado = 'Finalizado',
                        duracion_seg = EXTRACT(EPOCH FROM (:hora_fin - hora_inicio)),
                        duracion_hms = (:hora_fin - hora_inicio)
                    WHERE id = :registro_id
                """),
                {"registro_id": registro_id, "hora_fin": hora_fin}
            )
    except Exception as e:
        raise Exception(f"Error en stop_activity: {str(e)}")


def close_previous_day_activities(conn, user_id, previous_date):
    """
    Cierra todas las actividades abiertas de un día anterior a las 23:59:59.
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
                {
                    "user_id": user_id,
                    "fecha": previous_date,
                    "hora_fin": end_time
                }
            )
            return result.rowcount
    except Exception as e:
        raise Exception(f"Error en close_previous_day_activities: {str(e)}")


def get_today_summary(conn, user_id):
    """
    Obtiene el resumen consolidado del día con totales por actividad.
    Incluye actividades en curso calculando hasta el momento actual.
    """
    df = conn.query(
        """
        SELECT 
            a.nombre_actividad,
            SUM(
                CASE 
                    WHEN r.hora_fin IS NULL THEN 
                        EXTRACT(EPOCH FROM (NOW() - r.hora_inicio))
                    ELSE 
                        r.duracion_seg
                END
            ) as total_segundos
        FROM registro_actividades r 
        JOIN actividades a ON r.actividad_id = a.id 
        WHERE r.usuario_id = :user_id 
          AND r.fecha = CURRENT_DATE
        GROUP BY a.nombre_actividad
        ORDER BY total_segundos DESC
        """,
        params={"user_id": user_id},
        ttl=0
    )
    return df


# Archivo: queries.py
# Función: get_today_log

def get_today_log(conn, user_id):
    """
    Obtiene el log detallado del día con todas las actividades.
    Incluye subactividad y comentario.
    """
    df = conn.query(
        """
        SELECT 
            a.nombre_actividad,
            s.nombre_subactividad as subactividad,
            r.observaciones as comentario, -- <<-- CAMBIO REALIZADO AQUÍ
            to_char(r.hora_inicio, 'HH24:MI') as inicio,
            CASE 
                WHEN r.hora_fin IS NULL THEN 'En curso'
                ELSE to_char(r.duracion_hms, 'HH24:MI:SS')
            END as duracion
        FROM registro_actividades r 
        JOIN actividades a ON r.actividad_id = a.id 
        LEFT JOIN subactividades s ON r.subactividad_id = s.id
        WHERE r.usuario_id = :user_id 
          AND r.fecha = CURRENT_DATE
        ORDER BY r.hora_inicio DESC
        """,
        params={"user_id": user_id},
        ttl=0
    )
    return df


def get_activity_stats(conn, user_id, start_date, end_date):
    """
    Obtiene estadísticas de actividades en un rango de fechas.
    Útil para reportes semanales o mensuales.
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
                        EXTRACT(EPOCH FROM (NOW() - r.hora_inicio))
                    ELSE 
                        r.duracion_seg
                END
            ) / 3600 as total_horas,
            AVG(
                CASE 
                    WHEN r.hora_fin IS NULL THEN 
                        EXTRACT(EPOCH FROM (NOW() - r.hora_inicio))
                    ELSE 
                        r.duracion_seg
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
    Obtiene un resumen de productividad del usuario para una fecha específica.
    """
    df = conn.query(
        """
        SELECT 
            COUNT(*) as total_actividades,
            COUNT(DISTINCT actividad_id) as tipos_actividad,
            MIN(hora_inicio) as primera_actividad,
            MAX(COALESCE(hora_fin, NOW())) as ultima_actividad,
            SUM(
                CASE 
                    WHEN hora_fin IS NULL THEN 
                        EXTRACT(EPOCH FROM (NOW() - hora_inicio))
                    ELSE 
                        duracion_seg
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


# ======================================================
# 🧩 CONSULTAS DEL SUPERVISOR
# ======================================================

def get_supervisor_dashboard(conn, campaña_id, fecha):
    """
    Obtiene el resumen de asesores de una campaña para una fecha.
    Ahora incluye el cálculo de actividades en curso.
    """
    df = conn.query(
        """
        WITH Jornadas AS (
            SELECT
                usuario_id,
                MIN(hora_inicio) as hora_ingreso,
                MAX(COALESCE(hora_fin, NOW())) as hora_salida
            FROM registro_actividades r
            JOIN actividades a ON r.actividad_id = a.id
            WHERE r.fecha = :fecha
            GROUP BY r.usuario_id
        ),
        Efectivo AS (
            SELECT
                usuario_id,
                SUM(
                    CASE 
                        WHEN hora_fin IS NULL THEN 
                            EXTRACT(EPOCH FROM (NOW() - hora_inicio))
                        ELSE 
                            duracion_seg
                    END
                ) as segundos_efectivos
            FROM registro_actividades
            WHERE fecha = :fecha
              AND actividad_id NOT IN (
                  SELECT id FROM actividades 
                  WHERE nombre_actividad IN ('Pausa', 'Break')
              )
            GROUP BY usuario_id
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
    Obtiene el desglose de actividades por asesor en una campaña.
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
                        EXTRACT(EPOCH FROM (NOW() - r.hora_inicio))
                    ELSE 
                        r.duracion_seg
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


# ======================================================
# 🧩 CONSULTAS DEL ADMINISTRADOR
# ======================================================

def get_all_users_admin(conn):
    """Obtiene todos los usuarios para administración."""
    return conn.query("""
        SELECT u.id, u.nombre_usuario, u.nombre_completo, r.nombre as rol, c.nombre as campaña, u.estado 
        FROM usuarios u 
        LEFT JOIN roles r ON u.rol_id = r.id 
        LEFT JOIN campañas c ON u.campaña_id = c.id
        ORDER BY u.nombre_completo
    """, ttl=10)


def get_dropdown_data(conn):
    """Obtiene roles y campañas para dropdowns."""
    roles = conn.query("SELECT id, nombre FROM roles", ttl=3600)
    campañas = conn.query("SELECT id, nombre FROM campañas", ttl=3600)
    return roles.to_dict('records'), campañas.to_dict('records')


def check_username_exists(conn, username: str) -> bool:
    """Verifica si un usuario ya existe."""
    df = conn.query(
        "SELECT id FROM usuarios WHERE nombre_usuario = :username",
        params={"username": username},
        ttl=0
    )
    return not df.empty


def update_user_admin(conn, user_id, nombre_completo, rol_id, campaña_id, estado):
    """Actualiza información de un usuario."""
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
        raise Exception(f"Error en update_user_admin: {str(e)}")


def create_user_admin(conn, username, password, nombre_completo, rol_id, campaña_id):
    """Crea un nuevo usuario."""
    engine = get_engine()
    try:
        with engine.begin() as connection:
            connection.execute(
                text("""
                    INSERT INTO public.usuarios 
                    (nombre_usuario, contraseña, nombre_completo, rol_id, campaña_id)
                    VALUES (:u, :p, :nc, :ri, :ci)
                """),
                {"u": username, "p": password, "nc": nombre_completo, "ri": rol_id, "ci": campaña_id}
            )
    except Exception as e:
        raise Exception(f"Error en create_user_admin: {str(e)}")


# ======================================================
# 🧩 CONSULTAS DE SUBACTIVIDADES (ADMIN)
# ======================================================

def get_all_subactivities(conn):
    """
    Obtiene todas las subactividades con sus actividades relacionadas.
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
    Crea una nueva subactividad.
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
        raise Exception(f"Error en create_subactivity: {str(e)}")


def update_subactivity(conn, subactivity_id, nombre_subactividad, activo, orden):
    """
    Actualiza una subactividad existente.
    """
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
        raise Exception(f"Error en update_subactivity: {str(e)}")


def delete_subactivity(conn, subactivity_id):
    """
    Elimina (desactiva) una subactividad.
    """
    engine = get_engine()
    try:
        with engine.begin() as connection:
            connection.execute(
                text("UPDATE public.subactividades SET activo = FALSE WHERE id = :sid"),
                {"sid": subactivity_id}
            )
    except Exception as e:
        raise Exception(f"Error en delete_subactivity: {str(e)}")
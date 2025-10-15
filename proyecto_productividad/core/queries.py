import streamlit as st
from datetime import datetime
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
    return conn.query("SELECT * FROM actividades WHERE activo = TRUE ORDER BY id", ttl=600)


def get_last_activity_status(conn, user_id):
    """Obtiene el último registro de actividad de un usuario."""
    df = conn.query(
        """
        SELECT r.*, a.nombre_actividad
        FROM registro_actividades r
        JOIN actividades a ON r.actividad_id = a.id
        WHERE r.usuario_id = :user_id 
        ORDER BY r.id DESC 
        LIMIT 1
        """,
        params={"user_id": user_id},
        ttl=0
    )
    return df.to_dict('records')[0] if not df.empty else None


def start_activity(conn, user_id, actividad_id, hora_inicio):
    """Inicia una nueva actividad y devuelve el ID."""
    import sys
    engine = get_engine()
    try:
        with engine.begin() as connection:
            result = connection.execute(
                text("""
                    INSERT INTO public.registro_actividades 
                    (usuario_id, actividad_id, fecha, hora_inicio, estado) 
                    VALUES (:user_id, :actividad_id, CURRENT_DATE, :hora_inicio, 'Iniciado')
                    RETURNING id
                """),
                {"user_id": user_id, "actividad_id": actividad_id, "hora_inicio": hora_inicio}
            )
            registro_id = result.scalar()
            print(f"DEBUG: INSERT confirmado con engine, ID: {registro_id}", file=sys.stderr)
            return registro_id
    except Exception as e:
        print(f"Error en start_activity (engine): {e}", file=sys.stderr)
        raise


def stop_activity(conn, registro_id, hora_fin):
    """Finaliza una actividad y calcula duración."""
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


def get_today_summary(conn, user_id):
    """Obtiene el resumen de tiempo por actividad para hoy."""
    return conn.query(
        """
        SELECT 
            a.nombre_actividad, 
            SUM(r.duracion_seg) as total_segundos
        FROM registro_actividades r 
        JOIN actividades a ON r.actividad_id = a.id 
        WHERE r.usuario_id = :user_id 
          AND r.fecha = CURRENT_DATE 
          AND r.duracion_seg IS NOT NULL
        GROUP BY a.nombre_actividad
        ORDER BY total_segundos DESC
        """,
        params={"user_id": user_id},
        ttl=0
    )


def get_today_log(conn, user_id):
    """Obtiene el historial de actividades del día."""
    return conn.query(
        """
        SELECT 
            a.nombre_actividad, 
            to_char(r.hora_inicio, 'HH24:MI:SS') as inicio, 
            to_char(r.hora_fin, 'HH24:MI:SS') as fin, 
            to_char(r.duracion_hms, 'HH24:MI:SS') as duracion
        FROM registro_actividades r 
        JOIN actividades a ON r.actividad_id = a.id 
        WHERE r.usuario_id = :user_id 
          AND r.fecha = CURRENT_DATE
        ORDER BY r.hora_inicio DESC
        """,
        params={"user_id": user_id},
        ttl=0
    )


# ======================================================
# 🧩 CONSULTAS DEL SUPERVISOR
# ======================================================

def get_supervisor_dashboard(conn, campaña_id, fecha):
    """Obtiene el resumen de asesores de una campaña para una fecha."""
    return conn.query(
        """
        WITH Jornadas AS (
            SELECT
                usuario_id,
                MIN(CASE WHEN a.nombre_actividad = 'Ingreso' THEN r.hora_inicio END) as hora_ingreso,
                MAX(CASE WHEN a.nombre_actividad = 'Salida' THEN r.hora_fin END) as hora_salida
            FROM registro_actividades r
            JOIN actividades a ON r.actividad_id = a.id
            WHERE r.fecha = :fecha
            GROUP BY r.usuario_id
        ),
        Efectivo AS (
            SELECT
                usuario_id,
                SUM(duracion_seg) as segundos_efectivos
            FROM registro_actividades
            WHERE fecha = :fecha
              AND duracion_seg IS NOT NULL
              AND actividad_id NOT IN (SELECT id FROM actividades WHERE nombre_actividad LIKE '%%Break%%')
            GROUP BY usuario_id
        )
        SELECT 
            u.nombre_completo,
            to_char(j.hora_ingreso, 'HH24:MI:SS') as ingreso,
            to_char(j.hora_salida, 'HH24:MI:SS') as salida,
            (j.hora_salida - j.hora_ingreso) as tiempo_total_jornada,
            to_char((e.segundos_efectivos * interval '1 second'), 'HH24:MI:SS') as tiempo_efectivo
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

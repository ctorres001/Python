"""
Script para probar si el INSERT funciona directamente
"""

import toml
from sqlalchemy import create_engine, text
from datetime import datetime

# Leer secrets.toml
with open(".streamlit/secrets.toml", "r") as f:
    secrets = toml.load(f)

database_url = secrets["connections"]["neon_db"]["url"]

# Conectar
engine = create_engine(database_url)
conn = engine.connect()

print("✅ Conectado a BD\n")

try:
    # Insertar un registro de prueba
    user_id = 5  # asesor1
    activity_id = 5  # Bandeja de Correo
    now = datetime.now()
    
    print(f"📝 Intentando insertar:")
    print(f"   usuario_id: {user_id}")
    print(f"   actividad_id: {activity_id}")
    print(f"   hora_inicio: {now}\n")
    
    # Insertar
    insert_query = text("""
        INSERT INTO registro_actividades (usuario_id, actividad_id, fecha, hora_inicio, estado) 
        VALUES (:user_id, :actividad_id, CURRENT_DATE, :hora_inicio, 'Iniciado')
        RETURNING id
    """)
    
    result = conn.execute(insert_query, {
        "user_id": user_id,
        "actividad_id": activity_id,
        "hora_inicio": now
    })
    
    conn.commit()
    
    registro_id = result.fetchone()[0]
    print(f"✅ INSERT exitoso! ID del registro: {registro_id}\n")
    
    # Verificar que se insertó
    verify_query = text("SELECT COUNT(*) FROM registro_actividades WHERE id = :id")
    verify_result = conn.execute(verify_query, {"id": registro_id})
    count = verify_result.fetchone()[0]
    
    print(f"✅ Verificación: Registro existe en BD? {count > 0}\n")
    
    # Ver el registro
    select_query = text("""
        SELECT r.id, r.usuario_id, r.actividad_id, a.nombre_actividad, r.hora_inicio, r.estado
        FROM registro_actividades r
        LEFT JOIN actividades a ON r.actividad_id = a.id
        WHERE r.id = :id
    """)
    
    result = conn.execute(select_query, {"id": registro_id})
    row = result.fetchone()
    
    if row:
        print(f"📋 Registro insertado:")
        print(f"   ID: {row[0]}")
        print(f"   Usuario: {row[1]}")
        print(f"   Actividad: {row[4]} ({row[3]})")
        print(f"   Hora: {row[4]}")
        print(f"   Estado: {row[5]}")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
    conn.rollback()

finally:
    conn.close()
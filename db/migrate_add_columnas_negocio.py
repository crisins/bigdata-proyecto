"""
Migración: agrega las columnas de negocio a component_prices en Neon
sin borrar los datos que ya se cargaron.
Uso: python db/migrate_add_columnas_negocio.py
"""
import sys
from pathlib import Path
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parent))
from db_utils import engine, DATABASE_URL

COLUMNAS_NUEVAS = [
    ("cantidad", "INTEGER"),
    ("monto", "DOUBLE PRECISION"),
    ("forma_pago", "VARCHAR(50)"),
    ("cliente", "VARCHAR(200)"),
]

with engine.begin() as conn:
    for nombre, tipo in COLUMNAS_NUEVAS:
        if DATABASE_URL.startswith("sqlite"):
            tipo_sqlite = tipo.replace("DOUBLE PRECISION", "REAL")
            try:
                conn.execute(text(f"ALTER TABLE component_prices ADD COLUMN {nombre} {tipo_sqlite}"))
                print(f"[OK] Columna '{nombre}' agregada.")
            except Exception as e:
                print(f"[OMITIDO] {nombre}: {e}")
        else:
            conn.execute(text(f"ALTER TABLE component_prices ADD COLUMN IF NOT EXISTS {nombre} {tipo}"))
            print(f"[OK] Columna '{nombre}' verificada/agregada.")

print("[LISTO] Migración completada.")
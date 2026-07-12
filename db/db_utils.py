import os
from sqlalchemy import create_engine, text
from datetime import datetime, timezone

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./data_lake/gold/proyecto_bigdata.db")

if DATABASE_URL.startswith("sqlite"):
    os.makedirs("./data_lake/gold", exist_ok=True)

engine = create_engine(DATABASE_URL, future=True)


def init_db():
    """Crea las tablas del modelo final (Gold) y de control si no existen."""
    with open(
    os.path.join(os.path.dirname(__file__), "schema.sql"),"r",encoding="utf-8") as f:schema_sql = f.read()

    if DATABASE_URL.startswith("sqlite"):
        schema_sql = (
            schema_sql.replace("SERIAL", "INTEGER")
            .replace("JSONB", "TEXT")
            .replace("TIMESTAMPTZ", "TIMESTAMP")
        )

    with engine.begin() as conn:
        for statement in schema_sql.split(";"):
            statement = statement.strip()
            if statement:
                conn.execute(text(statement))


def log_ejecucion(proceso: str, tipo: str, estado: str, leidos: int = 0,
                   cargados: int = 0, rechazados: int = 0, duplicados: int = 0,
                   detalle: str = ""):

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO control_ejecucion
                (proceso, tipo, fecha_ejecucion, estado, registros_leidos,
                 registros_cargados, registros_rechazados, registros_duplicados, detalle)
                VALUES (:proceso, :tipo, :fecha, :estado, :leidos, :cargados, :rechazados, :duplicados, :detalle)
            """),
            {
                "proceso": proceso, "tipo": tipo,
                "fecha": datetime.now(timezone.utc).isoformat(),
                "estado": estado, "leidos": leidos, "cargados": cargados,
                "rechazados": rechazados, "duplicados": duplicados, "detalle": detalle,
            },
        )


def proceso_ya_ejecutado(proceso: str) -> bool:

    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT COUNT(*) FROM control_ejecucion
                WHERE proceso = :proceso AND estado = 'EXITOSO'
            """),
            {"proceso": proceso},
        ).scalar()
    return row > 0

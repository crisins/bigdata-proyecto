"""
API de ingesta streaming - Proyecto Big Data
=============================================
Expone un endpoint POST público que la plataforma de Duoc
(bdrealtimeescuelait.duoc.cl) invoca cada vez que hay una fluctuación
de precio en la subasta de componentes electrónicos.

Diseño (justificación para el informe):
  - El endpoint guarda el evento crudo en Bronze (trazabilidad) y responde
    rápido (200 OK), y dispara EN SEGUNDO PLANO la limpieza + carga a Gold
    (Neon), reutilizando la misma lógica de transform/transform_streaming.py.
    Así no dependemos de correr nada manual desde otra máquina: todo el
    ciclo bronze -> silver -> gold ocurre dentro del mismo servicio en Render.
  - Control de errores: si el payload no cumple el esquema mínimo, se
    guarda igual en una carpeta de "rechazados" con el motivo.
  - Acepta tanto un solo evento como una LISTA de eventos.
"""
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ValidationError

app = FastAPI(
    title="API Ingesta Streaming - Big Data (Subasta de Componentes)",
    description="Recibe eventos POST de la plataforma Duoc, los deja en Bronze y los carga a Gold (Neon).",
    version="1.1.0",
)

BASE_DIR = Path(__file__).resolve().parent.parent
BRONZE_STREAMING = BASE_DIR / "data_lake" / "bronze" / "streaming"
REJECTED_DIR = BASE_DIR / "data_lake" / "bronze" / "streaming_rechazados"
BRONZE_STREAMING.mkdir(parents=True, exist_ok=True)
REJECTED_DIR.mkdir(parents=True, exist_ok=True)

# Reutilizamos la lógica de limpieza/carga ya probada en transform/transform_streaming.py
# en vez de duplicarla -> un solo lugar con la verdad sobre cómo se limpia un evento.
sys.path.append(str(BASE_DIR / "db"))
sys.path.append(str(BASE_DIR / "transform"))
from db_utils import init_db, log_ejecucion  # noqa: E402
from transform_streaming import _limpiar_y_transformar, _cargar_a_gold  # noqa: E402


@app.on_event("startup")
def _startup():
    """Se asegura de que las tablas existan en Neon apenas arranca el servicio en Render."""
    try:
        init_db()
        print("[STARTUP] Base de datos inicializada correctamente.")
    except Exception as e:
        print(f"[STARTUP][ADVERTENCIA] No se pudo inicializar la base de datos: {e}")


class EventoSubastaFlexible(BaseModel):
    """
    Esquema mínimo esperado. Se deja flexible a propósito porque no
    controlamos el formato exacto que envía la plataforma de Duoc.
    """
    class Config:
        extra = "allow"  # acepta campos adicionales sin fallar


def _partition_path(base: Path) -> Path:
    """Particiona los archivos por fecha/hora, como en un data lake real."""
    now = datetime.now(timezone.utc)
    path = base / f"dt={now:%Y-%m-%d}" / f"hour={now:%H}"
    path.mkdir(parents=True, exist_ok=True)
    return path / "events.jsonl"


@app.get("/")
def health():
    """Health check simple (Render lo usa para saber si el servicio está vivo)."""
    return {"status": "ok", "service": "ingesta-streaming-bigdata"}


def _procesar_y_cargar_a_gold(registros_bronze: list):
    """
    Tarea en segundo plano: limpia y carga a Gold (Neon) el lote recién
    recibido. Corre DESPUÉS de responder 200 a Duoc, para no hacerlos
    esperar mientras limpiamos/insertamos en la base de datos.
    """
    proceso = f"streaming_api_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
    leidos = len(registros_bronze)
    cargados = rechazados = duplicados = 0

    try:
        df, rechazados, duplicados = _limpiar_y_transformar(registros_bronze)
        if len(df) > 0:
            cargados = _cargar_a_gold(df)
        log_ejecucion(proceso, "streaming", "EXITOSO", leidos, cargados, rechazados, duplicados,
                      detalle="Carga automática desde la API")
        print(f"[GOLD] {cargados} eventos cargados a component_prices (de {leidos} recibidos).")
    except Exception as e:
        log_ejecucion(proceso, "streaming", "ERROR", leidos, cargados, rechazados, duplicados, detalle=str(e))
        print(f"[ERROR] Falló la carga automática a Gold: {e}")


@app.post("/api/subasta/ingesta")
async def recibir_evento(request: Request, background_tasks: BackgroundTasks):
    """
    Endpoint que se registra en https://bdrealtimeescuelait.duoc.cl
    (campo "URL", con nota "El endpoint debe ser un POST").

    Acepta dos formas de payload:
      - Un solo evento:  {"component_id": "...", "price": 123, ...}
      - Una lista de eventos: [{"component_id": "...", ...}, {...}, ...]

    Flujo: guarda crudo en Bronze (trazabilidad) y dispara en segundo
    plano la limpieza + carga a Gold/Neon, para que quede disponible
    en el dashboard sin pasos manuales.
    """
    event_id = str(uuid.uuid4())
    received_at = datetime.now(timezone.utc).isoformat()

    try:
        raw_body = await request.json()
    except Exception as e:
        _guardar_rechazado(event_id, received_at, {}, f"JSON inválido: {e}")
        return JSONResponse(status_code=400, content={"status": "error", "detail": "JSON inválido"})

    if isinstance(raw_body, list):
        eventos = raw_body
    elif isinstance(raw_body, dict):
        eventos = [raw_body]
    else:
        _guardar_rechazado(event_id, received_at, raw_body, f"Tipo de payload no soportado: {type(raw_body).__name__}")
        return JSONResponse(status_code=400, content={"status": "error", "detail": "Payload debe ser un objeto o una lista de objetos"})

    procesados = []
    registros_bronze = []
    for item in eventos:
        item_event_id = str(uuid.uuid4())

        if not isinstance(item, dict):
            _guardar_rechazado(item_event_id, received_at, item, f"Elemento no es un objeto JSON: {type(item).__name__}")
            continue

        try:
            EventoSubastaFlexible(**item)
            registro = {"event_id": item_event_id, "received_at": received_at, "payload": item}
            path = _partition_path(BRONZE_STREAMING)
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(registro, ensure_ascii=False) + "\n")
            registros_bronze.append(registro)
            procesados.append(item_event_id)

        except ValidationError as e:
            _guardar_rechazado(item_event_id, received_at, item, str(e))
            procesados.append(item_event_id)

    # Disparamos la limpieza + carga a Neon EN SEGUNDO PLANO, ya con la
    # respuesta lista para Duoc (no los hacemos esperar).
    if registros_bronze:
        background_tasks.add_task(_procesar_y_cargar_a_gold, registros_bronze)

    return {"status": "ok", "batch_id": event_id, "eventos_procesados": len(procesados), "event_ids": procesados}


def _guardar_rechazado(event_id, received_at, payload, motivo):
    path = _partition_path(REJECTED_DIR)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps({
            "event_id": event_id, "received_at": received_at,
            "payload": payload, "motivo": motivo,
        }, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), reload=True)
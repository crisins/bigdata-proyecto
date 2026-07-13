"""
API de ingesta streaming - Proyecto Big Data
=============================================
Expone un endpoint POST público que la plataforma de Duoc
(bdrealtimeescuelait.duoc.cl) invoca cada vez que hay una fluctuación
de precio en la subasta de componentes electrónicos.

Diseño (justificación para el informe):
  - El endpoint NO transforma ni valida en profundidad: solo valida el
    esquema mínimo, guarda el evento crudo en la capa Bronze (archivo)
    y responde rápido (200 OK). Esto evita bloquear al emisor y separa
    responsabilidades (ingesta vs. transformación), patrón estándar en
    arquitecturas de streaming.
  - La limpieza, deduplicación y carga al modelo final ocurre después,
    en un proceso aparte (transform/transform_streaming.py), que se
    puede ejecutar cada cierto tiempo (ej. cada 5 minutos) o bajo demanda.
  - Control de errores: si el payload no cumple el esquema mínimo, se
    guarda igual en una carpeta de "rechazados" con el motivo, en vez
    de perder el dato (trazabilidad completa).
  - Acepta tanto un solo evento como una LISTA de eventos, ya que no
    controlamos el formato exacto que envía la plataforma externa.
"""
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ValidationError

app = FastAPI(
    title="API Ingesta Streaming - Big Data (Subasta de Componentes)",
    description="Recibe eventos POST de la plataforma Duoc y los deja en la capa Bronze del data lake.",
    version="1.0.0",
)

BASE_DIR = Path(__file__).resolve().parent.parent
BRONZE_STREAMING = BASE_DIR / "data_lake" / "bronze" / "streaming"
REJECTED_DIR = BASE_DIR / "data_lake" / "bronze" / "streaming_rechazados"
BRONZE_STREAMING.mkdir(parents=True, exist_ok=True)
REJECTED_DIR.mkdir(parents=True, exist_ok=True)


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


@app.post("/api/subasta/ingesta")
async def recibir_evento(request: Request):
    """
    Endpoint que se registra en https://bdrealtimeescuelait.duoc.cl
    (campo "URL", con nota "El endpoint debe ser un POST").

    Acepta dos formas de payload:
      - Un solo evento:  {"component_id": "...", "price": 123, ...}
      - Una lista de eventos: [{"component_id": "...", ...}, {...}, ...]
    """
    event_id = str(uuid.uuid4())
    received_at = datetime.now(timezone.utc).isoformat()

    # Control de errores: si el body no es JSON válido, no se cae la API
    try:
        raw_body = await request.json()
    except Exception as e:
        _guardar_rechazado(event_id, received_at, {}, f"JSON inválido: {e}")
        return JSONResponse(status_code=400, content={"status": "error", "detail": "JSON inválido"})

    # Normalizamos siempre a una lista de eventos, sea cual sea la forma que llegó
    if isinstance(raw_body, list):
        eventos = raw_body
    elif isinstance(raw_body, dict):
        eventos = [raw_body]
    else:
        _guardar_rechazado(event_id, received_at, raw_body, f"Tipo de payload no soportado: {type(raw_body).__name__}")
        return JSONResponse(status_code=400, content={"status": "error", "detail": "Payload debe ser un objeto o una lista de objetos"})

    procesados = []
    for item in eventos:
        item_event_id = str(uuid.uuid4())

        if not isinstance(item, dict):
            _guardar_rechazado(item_event_id, received_at, item, f"Elemento no es un objeto JSON: {type(item).__name__}")
            continue

        try:
            EventoSubastaFlexible(**item)
            registro = {
                "event_id": item_event_id,
                "received_at": received_at,
                "payload": item,
            }
            path = _partition_path(BRONZE_STREAMING)
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(registro, ensure_ascii=False) + "\n")
            procesados.append(item_event_id)

        except ValidationError as e:
            _guardar_rechazado(item_event_id, received_at, item, str(e))
            # Igual lo registramos como recibido: el dato queda trazado, no se pierde,
            # pero no bloqueamos al emisor por un problema nuestro de esquema.
            procesados.append(item_event_id)

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

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
    class Config:
        extra = "allow"


def _partition_path(base: Path) -> Path:
    now = datetime.now(timezone.utc)
    path = base / f"dt={now:%Y-%m-%d}" / f"hour={now:%H}"
    path.mkdir(parents=True, exist_ok=True)
    return path / "events.jsonl"


@app.get("/")
def health():
    return {"status": "ok", "service": "ingesta-streaming-bigdata"}


@app.post("/api/subasta/ingesta")
async def recibir_evento(request: Request):
    event_id = str(uuid.uuid4())
    received_at = datetime.now(timezone.utc).isoformat()

    try:
        raw_body = await request.json()
    except Exception as e:
        _guardar_rechazado(event_id, received_at, {}, f"JSON inválido: {e}")
        return JSONResponse(status_code=400, content={"status": "error", "detail": "JSON inválido"})

    try:
        EventoSubastaFlexible(**raw_body)
        registro = {
            "event_id": event_id,
            "received_at": received_at,
            "payload": raw_body,
        }
        path = _partition_path(BRONZE_STREAMING)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(registro, ensure_ascii=False) + "\n")

        return {"status": "ok", "event_id": event_id}

    except ValidationError as e:
        _guardar_rechazado(event_id, received_at, raw_body, str(e))
        return {"status": "recibido_con_advertencia", "event_id": event_id}


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

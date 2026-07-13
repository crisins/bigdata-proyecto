"""
Pipeline STREAMING - Subasta de componentes electrónicos
============================================================
Lee los eventos crudos que la API (api/main.py) fue dejando en la
capa Bronze (archivos .jsonl particionados por fecha/hora), los
limpia/valida/deduplica, y los carga al modelo final (Gold).

Uso manual (además de la carga automática que hace la API):
    python transform/transform_streaming.py
"""
import glob
import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parent.parent / "db"))
from db_utils import engine, init_db, log_ejecucion  # noqa: E402

BASE_DIR = Path(__file__).resolve().parent.parent
BRONZE_STREAMING = BASE_DIR / "data_lake" / "bronze" / "streaming"
SILVER_STREAMING = BASE_DIR / "data_lake" / "silver" / "streaming"
PROCESADOS_LOG = BASE_DIR / "data_lake" / "bronze" / ".archivos_procesados.txt"
SILVER_STREAMING.mkdir(parents=True, exist_ok=True)

# Nombres de campo reales detectados en el payload de Duoc (id_producto,
# producto, precio, fecreg), más variantes comunes por si cambian.
CAMPOS_ID_POSIBLES = [
    "id_producto", "component_id", "id", "componentId", "component_name",
    "producto", "nombre", "name", "codigo", "code", "sku", "product",
    "product_id", "item", "item_id", "componente",
]
CAMPOS_PRECIO_POSIBLES = [
    "precio", "price", "valor", "value", "monto", "amount", "current_price",
    "precio_actual", "cost", "costo", "bid", "oferta", "importe",
]


def _buscar_campo(payload: dict, candidatos: list):
    """Prueba una lista de nombres de campo posibles y devuelve el primero que exista."""
    for nombre in candidatos:
        if nombre in payload and payload[nombre] not in (None, ""):
            return payload[nombre]
    return None


def _archivos_pendientes():
    todos = sorted(glob.glob(str(BRONZE_STREAMING / "dt=*" / "hour=*" / "events.jsonl")))
    procesados = set()
    if PROCESADOS_LOG.exists():
        procesados = set(PROCESADOS_LOG.read_text().splitlines())
    return [f for f in todos if f not in procesados]


def _marcar_procesado(archivos):
    with open(PROCESADOS_LOG, "a") as f:
        for a in archivos:
            f.write(a + "\n")


def run():
    proceso = f"streaming_batch_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
    pendientes = _archivos_pendientes()

    if not pendientes:
        print("[INFO] No hay archivos nuevos de streaming para procesar.")
        return

    leidos = cargados = rechazados = duplicados = 0

    try:
        registros = []
        for archivo in pendientes:
            with open(archivo, encoding="utf-8") as f:
                for linea in f:
                    linea = linea.strip()
                    if linea:
                        import json
                        registros.append(json.loads(linea))

        leidos = len(registros)
        print(f"[INFO] Eventos leídos: {leidos}")

        df, rechazados, duplicados = _limpiar_y_transformar(registros)

        if len(df) > 0:
            silver_path = SILVER_STREAMING / f"component_prices_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}.parquet"
            df.to_parquet(silver_path, index=False)
            print(f"[SILVER] Datos limpios guardados en: {silver_path}")

            cargados = _cargar_a_gold(df)
            print(f"[GOLD] Registros insertados en component_prices: {cargados}")

        _marcar_procesado(pendientes)
        log_ejecucion(proceso, "streaming", "EXITOSO", leidos, cargados, rechazados, duplicados,
                      detalle=f"{len(pendientes)} archivos bronze procesados")
        print("[OK] Proceso streaming finalizado con éxito.")

    except Exception as e:
        log_ejecucion(proceso, "streaming", "ERROR", leidos, cargados, rechazados, duplicados, detalle=str(e))
        print(f"[ERROR] El proceso streaming falló: {e}")
        raise


def _limpiar_y_transformar(registros: list):
    filas = []
    rechazados = 0
    motivos_rechazo = []

    for r in registros:
        payload = r.get("payload", {})
        try:
            component_id = _buscar_campo(payload, CAMPOS_ID_POSIBLES)
            price = _buscar_campo(payload, CAMPOS_PRECIO_POSIBLES)

            if component_id is None or price is None:
                rechazados += 1
                motivos_rechazo.append(f"Campos no reconocidos. Payload recibido: {payload}")
                continue

            try:
                price = float(price)
            except (TypeError, ValueError):
                rechazados += 1
                motivos_rechazo.append(f"Precio no numérico: {price!r}")
                continue

            if price < 0:
                rechazados += 1
                continue

            event_ts = (
                payload.get("fecreg") or payload.get("timestamp") or payload.get("event_timestamp")
                or payload.get("fecha") or payload.get("date") or r.get("received_at")
            )

            fila = {
                "component_id": str(component_id),
                "component_name": (
                    payload.get("producto") or payload.get("component_name")
                    or payload.get("nombre") or payload.get("name") or str(component_id)
                ),
                "price": price,
                "currency": payload.get("currency", payload.get("moneda", "CLP")),
                "event_timestamp": event_ts,
                "received_at": r.get("received_at"),
            }
            raw_key = f"{fila['component_id']}|{fila['event_timestamp']}|{fila['price']}"
            fila["event_key"] = hashlib.md5(raw_key.encode()).hexdigest()
            filas.append(fila)

        except Exception as e:
            rechazados += 1
            motivos_rechazo.append(f"Error inesperado: {e}")
            continue

    if rechazados > 0 and len(filas) == 0:
        print(f"[DIAGNÓSTICO] Se rechazaron {rechazados} eventos. Primeros motivos:")
        for m in motivos_rechazo[:3]:
            print(f"   - {m}")

    if not filas:
        return pd.DataFrame(), rechazados, 0

    df = pd.DataFrame(filas)
    antes = len(df)
    df = df.drop_duplicates(subset=["event_key"])
    duplicados = antes - len(df)

    return df, rechazados, duplicados


def _cargar_a_gold(df: pd.DataFrame) -> int:
    df = df.copy()
    df["load_ts"] = datetime.now(timezone.utc).isoformat()

    with engine.begin() as conn:
        existentes = {row[0] for row in conn.execute(text("SELECT event_key FROM component_prices")).fetchall()}

    nuevos = df[~df["event_key"].isin(existentes)]
    if len(nuevos) == 0:
        return 0

    with engine.begin() as conn:
        nuevos.to_sql("component_prices", conn, if_exists="append", index=False, chunksize=2000)

    return len(nuevos)


if __name__ == "__main__":
    init_db()
    run()

import argparse
import hashlib
import shutil
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parent.parent / "db"))
from db_utils import engine, init_db, log_ejecucion, proceso_ya_ejecutado  

BASE_DIR = Path(__file__).resolve().parent.parent
BRONZE = BASE_DIR / "data_lake" / "bronze" / "batch"
SILVER = BASE_DIR / "data_lake" / "silver" / "batch"
BRONZE.mkdir(parents=True, exist_ok=True)
SILVER.mkdir(parents=True, exist_ok=True)


def _trip_key(row) -> str:
    raw = f"{row.VendorID}|{row.tpep_pickup_datetime}|{row.PULocationID}|{row.DOLocationID}|{row.fare_amount}"
    return hashlib.md5(raw.encode()).hexdigest()


def run(file_path: str, periodo: str):
    proceso = f"batch_taxi_{periodo}"

    if proceso_ya_ejecutado(proceso):
        print(f"[INFO] El proceso '{proceso}' ya se ejecutó con éxito antes. Nada que hacer.")
        print("       (Para forzar reproceso, borra su fila en control_ejecucion)")
        return

    leidos = cargados = rechazados = duplicados = 0

    try:
        origen = Path(file_path)
        destino_bronze = BRONZE / origen.name
        if not destino_bronze.exists():
            shutil.copy(origen, destino_bronze)
        print(f"[BRONZE] Archivo crudo resguardado en: {destino_bronze}")

        df = pd.read_parquet(destino_bronze)
        leidos = len(df)
        print(f"[INFO] Registros leídos: {leidos}")

        df_silver, rechazados, duplicados = _limpiar_y_transformar(df, periodo)
        silver_path = SILVER / f"taxi_trips_{periodo}.parquet"
        df_silver.to_parquet(silver_path, index=False)
        print(f"[SILVER] Datos limpios guardados en: {silver_path} ({len(df_silver)} filas)")

        cargados = _cargar_a_gold(df_silver)
        print(f"[GOLD] Registros insertados en taxi_trips: {cargados}")

        log_ejecucion(proceso, "batch", "EXITOSO", leidos, cargados, rechazados, duplicados,
                      detalle=f"Archivo: {origen.name}")
        print("[OK] Proceso batch finalizado con éxito.")

    except Exception as e:
        log_ejecucion(proceso, "batch", "ERROR", leidos, cargados, rechazados, duplicados, detalle=str(e))
        print(f"[ERROR] El proceso batch falló: {e}")
        raise


def _limpiar_y_transformar(df: pd.DataFrame, periodo: str):
    total_inicial = len(df)

    anio, mes = map(int, periodo.split("-"))
    df = df[df["tpep_pickup_datetime"].dt.year == anio]
    df = df[df["tpep_pickup_datetime"].dt.month == mes]
    df = df[df["trip_distance"] >= 0]
    df = df[df["fare_amount"] >= 0]
    df = df[df["passenger_count"].fillna(0) >= 0]
    df = df[df["tpep_dropoff_datetime"] > df["tpep_pickup_datetime"]]

    df["passenger_count"] = df["passenger_count"].fillna(1).astype(int)
    df = df.dropna(subset=["PULocationID", "DOLocationID", "tpep_pickup_datetime"])

    df = df.rename(columns={
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "PULocationID": "pu_location_id",
        "DOLocationID": "do_location_id",
    })
    df["vendor_id"] = df["vendor_id"].astype(int)
    df["pu_location_id"] = df["pu_location_id"].astype(int)
    df["do_location_id"] = df["do_location_id"].astype(int)

    df["trip_duration_min"] = (df["dropoff_datetime"] - df["pickup_datetime"]).dt.total_seconds() / 60
    df["pickup_hour"] = df["pickup_datetime"].dt.hour
    df["pickup_dow"] = df["pickup_datetime"].dt.dayofweek  # 0=lunes

    df["trip_key"] = df.apply(
        lambda r: hashlib.md5(
            f"{r.vendor_id}|{r.pickup_datetime}|{r.pu_location_id}|{r.do_location_id}|{r.fare_amount}".encode()
        ).hexdigest(), axis=1
    )
    antes = len(df)
    df = df.drop_duplicates(subset=["trip_key"])
    duplicados = antes - len(df)

    columnas_finales = [
        "trip_key", "vendor_id", "pickup_datetime", "dropoff_datetime",
        "passenger_count", "trip_distance", "pu_location_id", "do_location_id",
        "payment_type", "fare_amount", "tip_amount", "total_amount",
        "trip_duration_min", "pickup_hour", "pickup_dow",
    ]
    df = df[columnas_finales]

    rechazados = total_inicial - total_inicial + (total_inicial - len(df) - duplicados)
    rechazados = max(rechazados, 0)

    return df, rechazados, duplicados


def _cargar_a_gold(df: pd.DataFrame, lote: int = 20000) -> int:
    """
    Carga a Postgres/SQLite evitando duplicar (por trip_key), en lotes,
    para no agotar memoria con millones de filas de una sola vez.
    """
    from datetime import datetime, timezone
    df = df.copy()
    df["load_ts"] = datetime.now(timezone.utc).isoformat()

    with engine.begin() as conn:
        existentes = {row[0] for row in conn.execute(text("SELECT trip_key FROM taxi_trips")).fetchall()}

    nuevos = df[~df["trip_key"].isin(existentes)]
    total = len(nuevos)
    if total == 0:
        return 0

    for inicio in range(0, total, lote):
        chunk = nuevos.iloc[inicio:inicio + lote]
        with engine.begin() as conn:
            chunk.to_sql("taxi_trips", conn, if_exists="append", index=False, chunksize=2000)
        print(f"[GOLD] Insertado lote {inicio}-{inicio + len(chunk)} / {total}")

    return total


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="Ruta al archivo parquet de taxis")
    parser.add_argument("--periodo", required=True, help="Periodo en formato YYYY-MM, ej: 2022-01")
    args = parser.parse_args()

    init_db()
    run(args.file, args.periodo)

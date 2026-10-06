
"""Paso 2: limpieza distribuida con Dask y carga por lotes a MongoDB.

Flujo:
  CSV crudo --(Dask, particionado)--> limpieza --> Parquet limpio (/data/clean)
  Parquet limpio --(Dask, por partición)--> MongoDB (GeoJSON Point) --> índice 2dsphere

Deja un reporte con cuántos registros se descartaron y por qué
(/data/reportes/limpieza.json) para justificar las decisiones en el informe.
"""
import json
import os
import time
from pathlib import Path

import dask
import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client
from pymongo import ASCENDING, GEOSPHERE, MongoClient

CSV_GLOB = os.environ["CSV_GLOB"]
LAT, LON, TIME = os.environ["LAT_COL"], os.environ["LON_COL"], os.environ["TIME_COL"]
TIME_FORMAT = os.environ.get("TIME_FORMAT") or "mixed"
KEEP = [c.strip() for c in os.environ.get("KEEP_COLS", "").split(",") if c.strip()]
BBOX = os.environ.get("BBOX", "").strip()
MAX_PARTITIONS = os.environ.get("MAX_PARTITIONS", "").strip()

MONGO_URI = os.environ["MONGO_URI"]
DB = os.environ.get("MONGO_DB", "geodb")
COLL = os.environ.get("MONGO_COLLECTION", "eventos")
BATCH = int(os.environ.get("BATCH_SIZE", "5000"))

CLEAN_DIR = "/data/clean"
REPORT = Path("/data/reportes/limpieza.json")


def leer_y_limpiar():
    """Devuelve (ddf_limpio, conteos_lazy)."""
    columnas = [LAT, LON, TIME] + KEEP
    # dtype=str evita los errores de tipos mixtos entre particiones; luego convertimos.
    ddf = dd.read_csv(CSV_GLOB, usecols=columnas, dtype=str, blocksize="64MB")
    if MAX_PARTITIONS:
        ddf = ddf.partitions[: int(MAX_PARTITIONS)]

    ddf = ddf.rename(columns={LAT: "lat", LON: "lon", TIME: "ts"})
    ddf["lat"] = dd.to_numeric(ddf["lat"], errors="coerce").astype("float64")
    ddf["lon"] = dd.to_numeric(ddf["lon"], errors="coerce").astype("float64")
    ddf["ts"] = dd.to_datetime(ddf["ts"], errors="coerce", format=TIME_FORMAT)

    # Reglas de limpieza, evaluadas en orden (cada registro cae en UNA sola regla).
    nulo = ddf.lat.isna() | ddf.lon.isna()
    fuera_rango = ~nulo & ((ddf.lat < -90) | (ddf.lat > 90) | (ddf.lon < -180) | (ddf.lon > 180))
    cero = ~nulo & ~fuera_rango & (ddf.lat == 0) & (ddf.lon == 0)
    descartado = nulo | fuera_rango | cero

    if BBOX:
        mnlon, mnlat, mxlon, mxlat = map(float, BBOX.split(","))
        fuera_bbox = ~descartado & ~(ddf.lon.between(mnlon, mxlon) & ddf.lat.between(mnlat, mxlat))
    else:
        fuera_bbox = ddf.lat.isna() & False
    descartado = descartado | fuera_bbox

    ts_nulo = ~descartado & ddf.ts.isna()
    descartado = descartado | ts_nulo

    conteos = {
        "total_leidos": ddf["lat"].size,  # incluye NaN
        "coord_nulas": nulo.sum(),
        "coord_fuera_de_rango": fuera_rango.sum(),
        "punto_0_0": cero.sum(),
        "fuera_de_bbox": fuera_bbox.sum(),
        "fecha_nula_o_invalida": ts_nulo.sum(),
    }
    return ddf[~descartado], conteos


def cargar_particion(pdf, uri, db, coll, batch):
    """Se ejecuta EN CADA WORKER. El cliente de Mongo se crea aquí adentro
    porque no se puede serializar y enviar desde el proceso principal."""
    if pdf.empty:
        return pd.DataFrame({"insertados": [0]})

    cli = MongoClient(uri)
    col = cli[db][coll]
    registros = pdf.astype(object).where(pdf.notna(), None).to_dict("records")

    docs, n = [], 0
    for r in registros:
        lon, lat, ts = r.pop("lon"), r.pop("lat"), r.pop("ts", None)
        doc = {
            # GeoJSON: el orden es [LONGITUD, LATITUD]
            "location": {"type": "Point", "coordinates": [float(lon), float(lat)]},
            "ts": ts.to_pydatetime() if ts is not None else None,
        }
        # Mongo no admite "." ni "$" al inicio en nombres de campo
        doc.update({k.replace(".", "_").lstrip("$"): v for k, v in r.items()})
        docs.append(doc)
        if len(docs) >= batch:
            col.insert_many(docs, ordered=False)
            n += len(docs)
            docs = []
    if docs:
        col.insert_many(docs, ordered=False)
        n += len(docs)
    cli.close()
    return pd.DataFrame({"insertados": [n]})


def main():
    client = Client(os.environ["DASK_SCHEDULER"])
    print(f"[clean] Conectado a Dask: {client.dashboard_link}")
    t0 = time.perf_counter()

    limpio, conteos = leer_y_limpiar()

    # 1) Conteos de limpieza en una sola pasada sobre los datos
    (conteos_calc,) = dask.compute(conteos)
    conteos_calc = {k: int(v) for k, v in conteos_calc.items()}

    # 2) Guardar Parquet limpio (lo usan la carga y la comparación Dask vs Spark).
    #    Timestamps en ms porque Spark no lee nanosegundos en Parquet.
    limpio.to_parquet(CLEAN_DIR, write_index=False, overwrite=True, schema="infer",
                      coerce_timestamps="ms", allow_truncated_timestamps=True)

    # 3) Carga a MongoDB por particiones (idempotente: se reemplaza la colección)
    mongo = MongoClient(MONGO_URI)
    col = mongo[DB][COLL]
    col.drop()
    pq = dd.read_parquet(CLEAN_DIR)
    insertados = int(
        pq.map_partitions(cargar_particion, MONGO_URI, DB, COLL, BATCH,
                          meta={"insertados": "int64"}).compute()["insertados"].sum()
    )

    # 4) Índices: el 2dsphere es obligatorio; falla si hay geometrías inválidas
    col.create_index([("location", GEOSPHERE)])
    col.create_index([("ts", ASCENDING)])

    conteos_calc["insertados_en_mongo"] = insertados
    conteos_calc["segundos"] = round(time.perf_counter() - t0, 1)
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(conteos_calc, indent=2))
    print("[clean] Reporte de limpieza:\n" + json.dumps(conteos_calc, indent=2))


if __name__ == "__main__":
    main()
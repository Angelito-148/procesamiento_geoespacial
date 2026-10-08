# archivo: ingest/bench_dask.py
"""Benchmark Dask: agregación por grilla sobre el Parquet limpio.
Imprime una línea JSON por repetición (la recoge scripts/benchmark.sh)."""
import argparse
import json
import os
import time

import dask.dataframe as dd
from dask.distributed import Client

p = argparse.ArgumentParser()
p.add_argument("--tag", required=True)
p.add_argument("--repeat", type=int, default=3)
p.add_argument("--cell", type=float, default=0.01)   # grados (~1.1 km)
a = p.parse_args()

client = Client(os.environ["DASK_SCHEDULER"])
workers = len(client.scheduler_info()["workers"])

for i in range(a.repeat):
    t0 = time.perf_counter()
    df = dd.read_parquet("/data/clean", columns=["lon", "lat"])
    df = df.assign(cx=(df.lon // a.cell).astype("int64"), cy=(df.lat // a.cell).astype("int64"))
    res = df.groupby(["cx", "cy"]).size().compute()
    t = time.perf_counter() - t0
    print(json.dumps({"motor": "dask", "tag": a.tag, "workers": workers, "rep": i,
                      "segundos": round(t, 3), "celdas": int(len(res)), "registros": int(res.sum())}))
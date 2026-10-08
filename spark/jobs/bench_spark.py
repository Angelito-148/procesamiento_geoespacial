# archivo: spark/jobs/bench_spark.py
"""Benchmark Spark: la MISMA agregación por grilla que bench_dask.py,
sobre el MISMO Parquet limpio, para que la comparación sea justa."""
import argparse
import json
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

p = argparse.ArgumentParser()
p.add_argument("--tag", required=True)
p.add_argument("--repeat", type=int, default=3)
p.add_argument("--cell", type=float, default=0.01)
a = p.parse_args()

spark = SparkSession.builder.appName("bench-grilla").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
workers = spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1  # menos el driver

for i in range(a.repeat):
    t0 = time.perf_counter()
    df = spark.read.parquet("/data/clean").select("lon", "lat")
    res = (df.withColumn("cx", F.floor(F.col("lon") / a.cell))
             .withColumn("cy", F.floor(F.col("lat") / a.cell))
             .groupBy("cx", "cy").count().collect())
    t = time.perf_counter() - t0
    print(json.dumps({"motor": "spark", "tag": a.tag, "workers": workers, "rep": i,
                      "segundos": round(t, 3), "celdas": len(res),
                      "registros": int(sum(r["count"] for r in res))}))

spark.stop()
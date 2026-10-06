# archivo: spark/jobs/aggregations.py
#!/usr/bin/env python3
"""Procesamiento distribuido con Spark: lee desde MongoDB (MongoDB Spark Connector 10.x)
y guarda agregaciones espaciales y temporales en colecciones nuevas.
"""
import os

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

URI = os.environ["MONGO_URI"]
DB = os.environ.get("MONGO_DB", "geodb")
COLL = os.environ.get("MONGO_COLLECTION", "eventos")
CELDA = float(os.environ.get("GRID_CELL_DEG", "0.01"))   # ~1.1 km
PERCENTIL_HOTSPOT = float(os.environ.get("HOTSPOT_PCT", "0.99"))

spark = (SparkSession.builder.appName("agregaciones-geo")
         .config("spark.sql.session.timeZone", "UTC")   # mismas horas que MongoDB
         .getOrCreate())


def leer(coleccion):
    return (spark.read.format("mongodb")
            .option("connection.uri", URI)
            .option("database", DB)
            .option("collection", coleccion)
            .load())


def guardar(df, coleccion):
    (df.write.format("mongodb").mode("overwrite")
       .option("connection.uri", URI)
       .option("database", DB)
       .option("collection", coleccion)
       .save())
    print(f"[spark] guardado {coleccion}")


def punto(lon_col, lat_col):
    """GeoJSON Point para que los resultados se puedan mapear o consultar."""
    return F.struct(F.lit("Point").alias("type"),
                    F.array(lon_col.cast("double"), lat_col.cast("double")).alias("coordinates"))


eventos = leer(COLL).select(
    F.col("location.coordinates").getItem(0).alias("lon"),
    F.col("location.coordinates").getItem(1).alias("lat"),
    F.col("ts"),
).cache()
total = eventos.count()

# ---------- 1. Conteo por celda de grilla ----------
grilla = (eventos
          .withColumn("cx", F.floor(F.col("lon") / CELDA))
          .withColumn("cy", F.floor(F.col("lat") / CELDA))
          .groupBy("cx", "cy").agg(F.count("*").alias("conteo"))
          .withColumn("location", punto((F.col("cx") + 0.5) * CELDA, (F.col("cy") + 0.5) * CELDA))
          .withColumn("celda_grados", F.lit(CELDA))
          .cache())
guardar(grilla, "agg_grilla")

# ---------- 2. Zonas de alta concentración (hotspots) ----------
umbral = grilla.approxQuantile("conteo", [PERCENTIL_HOTSPOT], 0.001)[0]
hotspots = grilla.filter(F.col("conteo") >= umbral).withColumn("umbral", F.lit(umbral))
guardar(hotspots, "agg_hotspots")

# ---------- 3. Comportamiento temporal ----------
con_fecha = eventos.filter(F.col("ts").isNotNull())
guardar(con_fecha.groupBy(F.hour("ts").alias("hora")).count()
        .withColumnRenamed("count", "conteo"), "agg_por_hora")
guardar(con_fecha.groupBy(F.dayofweek("ts").alias("dia_semana")).count()
        .withColumnRenamed("count", "conteo"), "agg_por_dia")      # 1 = domingo ... 7 = sábado
guardar(con_fecha.groupBy(F.year("ts").alias("anio"), F.month("ts").alias("mes")).count()
        .withColumnRenamed("count", "conteo"), "agg_por_mes")

# ---------- 4. Verificación (criterio "resultados verificables") ----------
suma_grilla = grilla.agg(F.sum("conteo")).first()[0]
meta = spark.createDataFrame([Row(**{
    "total_eventos": int(total),
    "suma_conteos_grilla": int(suma_grilla),
    "cuadra": bool(total == suma_grilla),
    "celdas": int(grilla.count()),
    "hotspots": int(hotspots.count()),
    "umbral_hotspot": float(umbral),
    "celda_grados": CELDA,
})])
guardar(meta, "agg_meta")
print(f"[spark] total={total} suma_grilla={suma_grilla} cuadra={total == suma_grilla}")

spark.stop()
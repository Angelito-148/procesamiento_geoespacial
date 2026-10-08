# archivo: scripts/benchmark.sh
#!/usr/bin/env bash
# Comparación Dask vs Spark: misma agregación por grilla, mismo Parquet,
# dos configuraciones de workers por motor, 3 repeticiones cada una.
# Requiere haber corrido la ingesta antes (crea /data/clean).
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash: no convertir /rutas en rutas de Windows
OUT=bench_results
mkdir -p "$OUT"
TIEMPOS="$OUT/tiempos.jsonl"
MEMORIA="$OUT/memoria.csv"
: > "$TIEMPOS"
echo "epoch,tag,contenedor,memoria" > "$MEMORIA"

muestrear_memoria() {   # guarda docker stats cada segundo mientras corre una prueba
  local tag=$1
  while true; do
    docker stats --no-stream --format '{{.Name}},{{.MemUsage}}' \
      | sed "s|^|$(date +%s),$tag,|" >> "$MEMORIA"
    sleep 1
  done
}

correr() {   # $1 = tag, resto = comando
  local tag=$1; shift
  muestrear_memoria "$tag" & local pid=$!
  "$@" | grep '^{' | tee -a "$TIEMPOS"
  kill "$pid" 2>/dev/null || true
}

for n in 2 4; do
  echo "== Dask con $n workers =="
  docker compose up -d --scale dask-worker=$n dask-scheduler dask-worker
  sleep 10
  correr "dask_w$n" docker compose run --rm ingest python bench_dask.py --tag "dask_w$n" --repeat 3
done
docker compose up -d --scale dask-worker=2 dask-scheduler dask-worker

for n in 1 2; do
  echo "== Spark con $n workers =="
  docker compose up -d --scale spark-worker=$n spark-master spark-worker
  sleep 15
  correr "spark_w$n" docker compose exec -T spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 --conf spark.driver.host=spark-master \
      /opt/jobs/bench_spark.py --tag "spark_w$n" --repeat 3
done
docker compose up -d --scale spark-worker=1 spark-master spark-worker

python3 scripts/resumen_bench.py || python scripts/resumen_bench.py
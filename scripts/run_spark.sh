#!/usr/bin/env bash
# archivo: scripts/run_spark.sh
# Ejecuta las agregaciones de Spark dentro del contenedor spark-master.
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash: no convertir /rutas en rutas de Windows
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.driver.host=spark-master \
  /opt/jobs/aggregations.py
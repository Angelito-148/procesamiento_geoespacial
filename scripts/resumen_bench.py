# archivo: scripts/resumen_bench.py
"""Resume bench_results/: mediana de tiempo y pico de memoria por configuración.
Copia la tabla resultante al informe."""
import csv
import json
import re
import statistics
from collections import defaultdict

UNIDADES = {"B": 1 / 2**20, "KIB": 1 / 1024, "MIB": 1, "GIB": 1024,
            "KB": 1 / 1000, "MB": 1, "GB": 1000}


def a_mib(texto):
    m = re.match(r"([\d.]+)\s*([A-Za-z]+)", texto.split("/")[0].strip())
    return float(m.group(1)) * UNIDADES.get(m.group(2).upper(), 1) if m else 0.0


tiempos = defaultdict(list)
workers = {}
with open("bench_results/tiempos.jsonl") as f:
    for linea in f:
        d = json.loads(linea)
        tiempos[d["tag"]].append(d["segundos"])
        workers[d["tag"]] = d["workers"]

# Memoria del motor = suma de sus contenedores en cada instante; se toma el pico.
por_instante = defaultdict(float)
with open("bench_results/memoria.csv") as f:
    for fila in csv.DictReader(f):
        motor = fila["tag"].split("_")[0]
        if motor in fila["contenedor"]:
            por_instante[(fila["tag"], fila["epoch"])] += a_mib(fila["memoria"])
pico = defaultdict(float)
for (tag, _), mib in por_instante.items():
    pico[tag] = max(pico[tag], mib)

print(f"{'config':<12}{'workers':>8}{'mediana_s':>12}{'min_s':>9}{'max_s':>9}{'pico_MiB':>11}")
for tag, ts in tiempos.items():
    print(f"{tag:<12}{workers[tag]:>8}{statistics.median(ts):>12.2f}"
          f"{min(ts):>9.2f}{max(ts):>9.2f}{pico[tag]:>11.0f}")
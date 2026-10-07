# archivo: api/tests/smoke.py
"""Pruebas básicas contra la API ya levantada (se ejecuta dentro del contenedor api).
Si algo falla, termina con código 1 y Jenkins NO despliega."""
import json
import sys
import time
import urllib.request

BASE = "http://localhost:5000"
LAT, LON = 40.7580, -73.9855
fallos = []


def pedir(ruta, cuerpo=None):
    datos = json.dumps(cuerpo).encode() if cuerpo is not None else None
    req = urllib.request.Request(BASE + ruta, data=datos,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.status, json.loads(r.read())


def revisar(nombre, condicion):
    print(("OK   " if condicion else "FALLA") + " " + nombre)
    if not condicion:
        fallos.append(nombre)


for _ in range(30):  # esperar a que la API responda
    try:
        if pedir("/health")[0] == 200:
            break
    except Exception:
        time.sleep(2)
else:
    sys.exit("La API nunca respondió en /health")

s, d = pedir(f"/api/cercanos?lat={LAT}&lon={LON}&radio=1000")
revisar("cercanos devuelve resultados", s == 200 and d["total"] >= 1)

poligono = {"type": "Polygon", "coordinates": [[[-74.0, 40.75], [-73.97, 40.75],
                                                [-73.97, 40.77], [-74.0, 40.77], [-74.0, 40.75]]]}
s, d = pedir("/api/poligono", poligono)
revisar("poligono devuelve resultados", s == 200 and d["total_en_poligono"] >= 1)

s, d = pedir(f"/api/resumen-cercania?lat={LAT}&lon={LON}&radio=1000")
revisar("resumen-cercania ($geoNear) responde", s == 200 and d["resumen"][0]["total"] >= 1)

s, d = pedir("/api/estadisticas/grilla")
revisar("estadisticas/grilla responde", s == 200 and len(d) >= 1)

if fallos:
    sys.exit(f"Fallaron {len(fallos)} pruebas: {fallos}")
print("Todas las pruebas de humo pasaron.")
# archivo: api/app.py
"""API Flask del sistema geoespacial.

Endpoints:
  GET  /health
  GET  /api/cercanos?lat=..&lon=..&radio=..&limite=..      -> $near
  POST /api/poligono?limite=..   (body: GeoJSON Polygon)    -> $geoWithin
  GET  /api/resumen-cercania?lat=..&lon=..&radio=..         -> agregación con $geoNear
  GET  /api/estadisticas/<nombre>?limite=..                 -> resultados de Spark
"""
import os

from bson import json_util
from flask import Flask, Response, request
from pymongo import DESCENDING, MongoClient
from pymongo.errors import PyMongoError

from validators import (ValidationError, parse_limite, parse_poligono,
                        parse_punto, parse_radio)

# Dentro de Docker, MONGO_URI apunta a "mongo"; fuera, por defecto a localhost
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB = os.environ.get("MONGO_DB", "geodb")
COLL = os.environ.get("MONGO_COLLECTION", "eventos")

# Colecciones que produce Spark (lista blanca: no se expone cualquier colección)
ESTADISTICAS = {
    "grilla": ("agg_grilla", "conteo"),
    "hotspots": ("agg_hotspots", "conteo"),
    "hora": ("agg_por_hora", "hora"),
    "dia": ("agg_por_dia", "dia_semana"),
    "mes": ("agg_por_mes", "mes"),
    "meta": ("agg_meta", None),
}

app = Flask(__name__)
_cliente = None


def db():
    """Crea la conexión la primera vez que se necesita y la reutiliza."""
    global _cliente
    if _cliente is None:
        _cliente = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    return _cliente[DB]


def respuesta(obj, status=200):
    # json_util convierte fechas y tipos de Mongo a JSON
    return Response(json_util.dumps(obj, ensure_ascii=False), status=status,
                    mimetype="application/json")


@app.errorhandler(ValidationError)
def error_validacion(e):
    return respuesta({"error": str(e)}, 400)


@app.errorhandler(PyMongoError)
def error_mongo(e):
    return respuesta({"error": "Base de datos no disponible", "detalle": str(e)}, 503)


@app.get("/health")
def health():
    db().command("ping")
    return respuesta({"estado": "ok"})


@app.get("/api/cercanos")
def cercanos():
    punto = parse_punto(request.args)
    radio = parse_radio(request.args)
    limite = parse_limite(request.args)
    filtro = {"location": {"$near": {"$geometry": punto, "$maxDistance": radio}}}  # metros
    docs = list(db()[COLL].find(filtro, {"_id": 0}).limit(limite))
    return respuesta({"total": len(docs), "resultados": docs})


@app.post("/api/poligono")
def en_poligono():
    poligono = parse_poligono(request.get_json(silent=True))
    limite = parse_limite(request.args)
    filtro = {"location": {"$geoWithin": {"$geometry": poligono}}}
    col = db()[COLL]
    docs = list(col.find(filtro, {"_id": 0}).limit(limite))
    return respuesta({"total_en_poligono": col.count_documents(filtro),
                      "devueltos": len(docs), "resultados": docs})


@app.get("/api/resumen-cercania")
def resumen_cercania():
    """Agregación con $geoNear (debe ser la PRIMERA etapa del pipeline)."""
    punto = parse_punto(request.args)
    radio = parse_radio(request.args)
    pipeline = [
        {"$geoNear": {"near": punto, "distanceField": "distancia_m",
                      "maxDistance": radio, "spherical": True, "key": "location"}},
        {"$facet": {
            "resumen": [{"$group": {"_id": None, "total": {"$sum": 1},
                                    "distancia_promedio_m": {"$avg": "$distancia_m"}}},
                        {"$project": {"_id": 0}}],
            "por_hora": [{"$match": {"ts": {"$ne": None}}},
                         {"$group": {"_id": {"$hour": "$ts"}, "conteo": {"$sum": 1}}},
                         {"$sort": {"_id": 1}}],
        }},
    ]
    resultado = list(db()[COLL].aggregate(pipeline))
    return respuesta(resultado[0] if resultado else {})


@app.get("/api/estadisticas/<nombre>")
def estadisticas(nombre):
    if nombre not in ESTADISTICAS:
        return respuesta({"error": f"No existe '{nombre}'",
                          "disponibles": sorted(ESTADISTICAS)}, 404)
    coleccion, orden = ESTADISTICAS[nombre]
    limite = parse_limite(request.args)
    cursor = db()[coleccion].find({}, {"_id": 0})
    if orden:
        direccion = DESCENDING if orden == "conteo" else 1
        cursor = cursor.sort(orden, direccion)
    return respuesta(list(cursor.limit(limite)))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
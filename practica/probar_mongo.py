# archivo: practica/probar_mongo.py
"""Ejercicio: conectarse a MongoDB desde Python y hacer una consulta geoespacial.
Se ejecuta DESDE TU COMPUTADOR, por eso usa localhost (no "mongo")."""
from pathlib import Path

from pymongo import GEOSPHERE, MongoClient

# Leer usuario y contraseña del .env (sin escribirlos en el código)
env = {}
for linea in Path(".env").read_text().splitlines():
    if "=" in linea and not linea.startswith("#"):
        clave, valor = linea.split("=", 1)
        env[clave.strip()] = valor.strip()

uri = f"mongodb://{env['MONGO_USER']}:{env['MONGO_PASSWORD']}@localhost:27017/?authSource=admin"
cliente = MongoClient(uri, serverSelectionTimeoutMS=5000)
print("Ping:", cliente.admin.command("ping"))   # {'ok': 1.0} si la conexión funciona

col = cliente["practica"]["lugares"]
col.drop()
col.insert_many([
    # GeoJSON: el orden es [LONGITUD, LATITUD]
    {"nombre": "A", "location": {"type": "Point", "coordinates": [-73.9855, 40.7580]}},
    {"nombre": "B", "location": {"type": "Point", "coordinates": [-73.9800, 40.7600]}},
    {"nombre": "C", "location": {"type": "Point", "coordinates": [-74.0445, 40.6892]}},
])
col.create_index([("location", GEOSPHERE)])     # índice 2dsphere

cerca = col.find({"location": {"$near": {
    "$geometry": {"type": "Point", "coordinates": [-73.9855, 40.7580]},
    "$maxDistance": 1000,                        # metros
}}})
print("A menos de 1 km:", [d["nombre"] for d in cerca])
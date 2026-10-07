# archivo: api/tests/seed.py
"""Siembra datos mínimos en el entorno de PRUEBA de Jenkins (proyecto geo-ci).
No toca los datos reales."""
import os
from datetime import datetime

from pymongo import GEOSPHERE, MongoClient

db = MongoClient(os.environ["MONGO_URI"])[os.environ.get("MONGO_DB", "geodb")]
col = db[os.environ.get("MONGO_COLLECTION", "eventos")]
col.drop()
base_lon, base_lat = -73.9855, 40.7580
col.insert_many([
    {"location": {"type": "Point", "coordinates": [base_lon + i * 0.001, base_lat + i * 0.001]},
     "ts": datetime(2024, 1, 1, i), "prueba": True}
    for i in range(5)
])
col.create_index([("location", GEOSPHERE)])
db["agg_grilla"].drop()
db["agg_grilla"].insert_one({"cx": -7399, "cy": 4075, "conteo": 5,
                             "location": {"type": "Point", "coordinates": [base_lon, base_lat]}})
print("[seed] datos de prueba listos")
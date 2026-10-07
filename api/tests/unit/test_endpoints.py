# archivo: api/tests/unit/test_endpoints.py
"""Pruebas de la API que no necesitan MongoDB: la validación ocurre antes de consultar."""
import pytest

from app import app


@pytest.fixture
def cliente():
    app.config["TESTING"] = True
    return app.test_client()


def test_cercanos_sin_parametros_da_400(cliente):
    assert cliente.get("/api/cercanos").status_code == 400


def test_cercanos_lat_invalida_da_400(cliente):
    r = cliente.get("/api/cercanos?lat=200&lon=0&radio=100")
    assert r.status_code == 400
    assert "lat" in r.get_json()["error"]


def test_poligono_mal_formado_da_400(cliente):
    r = cliente.post("/api/poligono", json={"type": "Polygon", "coordinates": []})
    assert r.status_code == 400


def test_estadistica_inexistente_da_404(cliente):
    r = cliente.get("/api/estadisticas/no-existe")
    assert r.status_code == 404
    assert "grilla" in r.get_json()["disponibles"]
# archivo: api/validators.py
"""Validación de parámetros. Separada de app.py para poder probarla con pytest
sin necesidad de tener MongoDB corriendo."""


class ValidationError(ValueError):
    pass


def _es_numero(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def parse_float(args, nombre, minimo, maximo, default=None):
    valor = args.get(nombre)
    if valor is None or valor == "":
        if default is not None:
            return default
        raise ValidationError(f"Falta el parámetro '{nombre}'")
    try:
        v = float(valor)
    except (TypeError, ValueError):
        raise ValidationError(f"'{nombre}' debe ser numérico")
    if not (minimo <= v <= maximo):
        raise ValidationError(f"'{nombre}' debe estar entre {minimo} y {maximo}")
    return v


def parse_punto(args):
    lat = parse_float(args, "lat", -90, 90)
    lon = parse_float(args, "lon", -180, 180)
    # GeoJSON: [longitud, latitud]
    return {"type": "Point", "coordinates": [lon, lat]}


def parse_radio(args, maximo=50_000):
    return parse_float(args, "radio", 1, maximo)  # metros


def parse_limite(args, default=100, maximo=1000):
    return int(parse_float(args, "limite", 1, maximo, default=default))


def parse_poligono(cuerpo):
    """Acepta un GeoJSON Polygon o un Feature cuyo geometry sea Polygon."""
    if not isinstance(cuerpo, dict):
        raise ValidationError("El cuerpo debe ser un objeto GeoJSON")
    if cuerpo.get("type") == "Feature":
        cuerpo = cuerpo.get("geometry") or {}
    if cuerpo.get("type") != "Polygon":
        raise ValidationError("Se espera un GeoJSON de tipo 'Polygon'")

    anillos = cuerpo.get("coordinates")
    if not isinstance(anillos, list) or not anillos:
        raise ValidationError("'coordinates' debe ser una lista de anillos")
    for anillo in anillos:
        if not isinstance(anillo, list) or len(anillo) < 4:
            raise ValidationError("Cada anillo necesita al menos 4 posiciones")
        for pos in anillo:
            if (not isinstance(pos, list) or len(pos) < 2
                    or not _es_numero(pos[0]) or not _es_numero(pos[1])):
                raise ValidationError("Cada posición debe ser [lon, lat]")
            if not (-180 <= pos[0] <= 180 and -90 <= pos[1] <= 90):
                raise ValidationError(f"Posición fuera de rango: {pos}")
        if anillo[0][:2] != anillo[-1][:2]:
            raise ValidationError("El polígono debe estar cerrado (primer punto = último)")
    return {"type": "Polygon", "coordinates": anillos}
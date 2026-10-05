# archivo: ingest/Dockerfile
FROM python:3.11-slim
WORKDIR /app
# Primero las librerías: Docker reutiliza esta capa si requirements.txt no cambia
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Después el código (cambia seguido, así que va al final)
COPY . .
# archivo: spark/Dockerfile
# Imagen oficial de Apache Spark 3.5.1 (Scala 2.12, Java 17, Python 3)
FROM apache/spark:3.5.1
USER root
COPY jobs /opt/jobs
RUN chmod -R a+r /opt/jobs
USER spark
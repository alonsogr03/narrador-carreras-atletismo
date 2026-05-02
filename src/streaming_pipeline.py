from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# TODO: Iniciar SparkSession con el paquete de Kafka.
# TODO: Definir el esquema (StructType) exacto de vuestro evento.
# TODO: Leer el stream desde Kafka (df_raw).
# TODO: Parsear el JSON de Kafka a columnas de Spark.

# --- LÓGICA DE NEGOCIO ---
# TODO: Crear rama Coach (filtrar por el id del corredor elegido).
# TODO: Crear rama Narrador (usar window/watermark para agrupar por parcial y detectar roturas > 5s).

# --- PERSISTENCIA ---
# TODO: Escribir el resultado de la rama Coach en SQLite (ej. tabla 'coach_events').
# TODO: Escribir el resultado de la rama Narrador en SQLite (ej. tabla 'broadcaster_events').
# TODO: Hacer un .start() y .awaitTermination().
import os
import json
import pandas as pd
import sqlite3
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.streaming.state import GroupStateTimeout

# =====================================================================
# CONFIGURACIÓN GENERAL
# =====================================================================
KAFKA_TOPIC = "race_events"
KAFKA_BROKER = "localhost:9092"

CORREDOR_PROTAGONISTA = "Marta GARCÍA"
TOTAL_CORREDORES_ESPERADOS = 8  
MAX_HISTORIAL_POR_PARCIAL = 10  


def crear_spark_session():

    return SparkSession.builder \
        .appName("SparkAtletismoApp") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
        .getOrCreate()


# =====================================================================
# 1. DEFINICIÓN DE ESQUEMAS
# =====================================================================
esquema_kafka = StructType([
    StructField("schema_version", StringType(), True),
    StructField("id_evento", StringType(), True),
    StructField("nombre_corredor", StringType(), True),
    StructField("timestamp", FloatType(), True),
    StructField("distancia_metros", IntegerType(), True)
])

esquema_memoria_coach = StructType([
    StructField("ultimo_tiempo_s", FloatType(), True),
    StructField("ultima_distancia_m", IntegerType(), True)
])

esquema_salida_coach = StructType([
    StructField("nombre_corredor", StringType(), True),
    StructField("distancia_actual_m", IntegerType(), True),
    StructField("tiempo_parcial_s", FloatType(), True),
    StructField("tiempo_total_s", FloatType(), True)
])

# Esquemas Rama Comentarista
esquema_memoria_comentarista = StructType([  
])

esquema_salida_comentarista = StructType([
])

# =====================================================================
# 2. MOTOR LÓGICO 1: EL COACH (Estado Nativo Simple)
# =====================================================================


# Creamos el motor de conexión (punto de acceso al archivo)
engine = create_engine("sqlite:///data/telemetria.db")

def guardar_en_db_coach(batch_df, batch_id):
    """Función que ejecuta Spark en cada micro-batch"""
    # Convertimos el lote de Spark a Pandas
    pdf = batch_df.toPandas()
    
    if not pdf.empty:
        # Escribimos en la tabla 'coach'. 
        # if_exists='append' hace que no borre lo anterior.
        pdf.to_sql("tabla_coach", con=engine, if_exists="append", index=False)
        print(f"📥 Batch {batch_id}: {len(pdf)} filas guardadas en SQLite.")

        

def logica_coach(clave, pdfs_iter, estado):
    # Cogemos el nombre del corredor
    corredor = clave[0]
    
    # TODO 2: Consultar la memoria pasada (estado.exists, estado.get)
    # Si existe, extrae ultimo_tiempo y ultima_distancia. Si no, inicialízalos a 0.
    
    if estado.exists:
        memoria = estado.get
        ultimo_acumulado = memoria[0]
        ultima_distancia = memoria[1]

    else:
        ultimo_acumulado = 0.0
        ultima_distancia = 0

    
    # Unificamos los datos entrantes del micro-batch
    pdf_nuevos = pd.concat(list(pdfs_iter)).sort_values("timestamp")
    filas = []

    # TODO 3: Iterar sobre pdf_nuevos, calcular el 'tiempo_parcial' (tiempo actual - ultimo_tiempo)
    # y rellenar la lista 'filas' con diccionarios que coincidan con esquema_salida_coach.
    # Recuerda actualizar ultimo_tiempo en cada vuelta.

    for i in range(len(pdf_nuevos)):
        # Me leo la fila
        row = pdf_nuevos.iloc[i]

        # Construyo la nueva fila a partir de la última: 
        
        
        diccionario = {
            "nombre_corredor": corredor,
            "distancia_actual_m": int(row['distancia_metros']),
            "tiempo_parcial_s" : float(row['timestamp'] - ultimo_acumulado),
            "tiempo_total_s" : float(row['timestamp'])
        }

        filas.append(diccionario)

        ultimo_acumulado = row['timestamp']
        ultima_distancia = int(row['distancia_metros'])


    

    # TODO 4: Sobrescribir la memoria de Spark para el futuro (estado.update)
    estado.update((ultimo_acumulado, ultima_distancia))
    
    yield pd.DataFrame(filas)

# =====================================================================
# 3. MOTOR LÓGICO 2: EL COMENTARISTA (Recorte, Gaps y Timeout)
# =====================================================================
def logica_comentarista(clave, pdfs_iter, estado):
    parcial_actual = clave[0]  
    
    # TODO 5: Gestionar el caso en el que el estado caduca por inactividad (estado.hasTimedOut)
    # Cargar el JSON, borrar el estado (estado.remove()) y devolver un DataFrame indicando el timeout.
    

    # TODO 6: Flujo normal (llegan datos). Cargar memoria anterior o crear un diccionario limpio.
    

    pdf_nuevos = pd.concat(list(pdfs_iter)).sort_values("timestamp")
    nuevas_alertas = []

    # TODO 7: Procesar llegadas, ordenar la lista por tiempo, aplicar recorte a los últimos MAX_HISTORIAL_POR_PARCIAL
    # y detectar si la diferencia entre un corredor y el anterior es > 5 segundos.
    

    # TODO 8: Control de completitud. Si ya llegaron todos (TOTAL_CORREDORES_ESPERADOS),
    # borra el estado. Si faltan, actualiza el JSON y pon un timeout de 60 segundos.
    
    
    # Retorno provisional para que no falle la sintaxis
    return pd.DataFrame()

# =====================================================================
# 4. ORQUESTACIÓN PRINCIPAL (BIFURCACIÓN DE FLUJOS)
# =====================================================================
def iniciar_consumidor():
    spark = crear_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Conectando a Kafka y bifurcando flujos...")

    # TODO 9: Leer de Kafka en la red de Docker
    broker = os.environ.get("KAFKA_BROKER", "kafka:29092")
    
    df_raw = (
        spark.readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', broker) 
        .option('subscribe', KAFKA_TOPIC)                    
        .option('startingOffsets', 'earliest') 
        .option('failOnDataLoss', 'false')
        .load()
    )
    

    # TODO 10: Deserializar el JSON bruto usando selectExpr y from_json con tu esquema_kafka
    df_json = (
        df_raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), esquema_kafka).alias("data"))
        .select("data.*")
    )

    # TODO 11: Crear el flujo df_coach filtrando a Marta García, agrupando y aplicando applyInPandasWithState
    df_coach = (
        df_json
        .filter(col("nombre_corredor") == CORREDOR_PROTAGONISTA)
        .groupBy("nombre_corredor")
        .applyInPandasWithState(
            logica_coach, 
            outputStructType=esquema_salida_coach,
            stateStructType=esquema_memoria_coach,
            outputMode="append",
            timeoutConf=GroupStateTimeout.NoTimeout
        )
    )

    # TODO 12: Escribir df_coach en Parquet usando writeStream con trigger de 5 segundos
    query_coach = (
        df_coach.writeStream
        .foreachBatch(guardar_en_db_coach)
        .option("checkpointLocation", "data/checkpoints/coach")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # TODO 13: (Opcional por ahora) Hacer lo mismo para df_comentarista agrupando por distancia_metros
    

    print(f"🚀 [EN DIRECTO] Esperando a que arranques los motores...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    iniciar_consumidor()
import os
import json
import pandas as pd
import sqlite3
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType
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

esquema_salida_comentarista = StructType([
    StructField("tiempo_cabeza", FloatType(), True),
    StructField("tiempo_cola", FloatType(), True),
    StructField("dist_parcial", IntegerType(), True),
    StructField("n_corredores", IntegerType(), True),
    StructField("n_grupo", IntegerType(), True),
    StructField("tipo_evento", StringType(), True),
    StructField("composicion_grupo", ArrayType(StringType()), True)   
])

# Esquemas Rama Comentarista
esquema_memoria_comentarista = StructType([
    StructField("grupo_en_cabeza", esquema_salida_comentarista, True),
    StructField("ultima_distancia_m", IntegerType(), True),
    StructField("grupos", ArrayType(esquema_salida_comentarista), True)
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

        
def guardar_en_db_comentarista(batch_df, batch_id):
    """Función que ejecuta Spark en cada micro-batch"""
    # Convertimos el lote de Spark a Pandas
    pdf = batch_df.toPandas()
    
    if not pdf.empty:
        # Escribimos en la tabla 'coach'. 
        # if_exists='append' hace que no borre lo anterior.
        pdf.to_sql("tabla_comentarista", con=engine, if_exists="append", index=False)
        print(f"📥 Batch {batch_id}: {len(pdf)} filas guardadas en SQLite para el comentarista.")

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
    corredores_nuevos = pd.concat(list(pdfs_iter)).sort_values("timestamp") 
    
    if estado.exists:
        memoria = estado.get
        grupo_en_cabeza = {
            tiempo_cabeza: memoria[0][0], 
            tiempo_cola: memoria[0][1], 
            dist_parcial: memoria[0][2],
            n_corredores: memoria[0][3],
            n_grupo: memoria[0][4],
            tipo_evento: memoria[0][5],
            composicion_grupo: memoria[0][6]
            }
        ultima_distancia_m = memoria[1]
        nombres_variables = list(grupos_en_cabeza.keys())
        grupos = [dict(zip(nombres_variables, tupla))for tupla in memoria[2]]

    else:
        grupos_en_cabeza = {}
        ultima_distancia_m = 0
        grupos = []    
    

    # CONDICION 1: LLEGAN LOS PRIMEROS CORREDORES
    if ultima_distancia_m == 0:
        corredores = {
            tiempo_cabeza: corredores_nuevos['timestamp'].min(),
            tiempo_cola: corredores_nuevos['timestamp'].max(),
            dist_parcial: corredores_nuevos['distancia_metros'].min(),
            n_corredores: len(corredores_nuevos),
            n_grupo: 1,
            tipo_evento: null,
            composicion_grupo: corredores_nuevos['nombre_corredor'].unique().tolist()
        }
        grupos.append(corredores)
        ultima_distancia_m = corredores.get(dist_parcial)
        filas = []
        campos_info = list(corredores.keys())
        grupos_en_cabeza = tuple(grupos_en_cabeza[campo] for campo in  campos_info)ç
        grupos = [ tuple(grupo[campo] for campo in campos_info) for grupo in grupos]

    estado.update((grupos_en_cabeza, ultima_distancia_m, grupos))




    yield pd.DataFrame(filas)

# =====================================================================
# 4. ORQUESTACIÓN PRINCIPAL (BIFURCACIÓN DE FLUJOS)
# =====================================================================
def iniciar_consumidor():
    spark = crear_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Conectando a Kafka y bifurcando flujos...")

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
    
    df_json = (
        df_raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), esquema_kafka).alias("data"))
        .select("data.*")
    )

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

    query_coach = (
        df_coach.writeStream
        .foreachBatch(guardar_en_db_coach)
        .option("checkpointLocation", "data/checkpoints/coach")
        .trigger(processingTime="10 seconds")
        .start()
    )

    df_comentarista = (
        df_json
        .groupBy("nombre_corredor")
        .applyInPandasWithState(
            logica_comentarista, 
            outputStructType=esquema_salida_comentarista,
            stateStructType=esquema_memoria_comentarista,
            outputMode="append",
            timeoutConf=GroupStateTimeout.NoTimeout
        )
    )

    query_comentarista = (
            df_comentarista.writeStream
            .foreachBatch(guardar_en_db_comentarista)
            .option("checkpointLocation", "data/checkpoints/comentarista")
            .trigger(processingTime="5 seconds")
            .start()
        )
    
    print(f"🚀 [EN DIRECTO] Esperando a que arranques los motores...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    iniciar_consumidor()
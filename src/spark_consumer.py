import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# =====================================================================
# CONFIGURACIÓN
# =====================================================================
TOPIC_KAFKA = "race_events"
KAFKA_BROKER = "localhost:9092"

# 🔑 TAREA PARA LUEGO: Aquí configuraréis vuestra API Key de OpenAI
# os.environ["OPENAI_API_KEY"] = "tu-clave-aqui"

def crear_spark_session():
    """Crea la sesión de Spark con el conector de Kafka incluido."""
    return SparkSession.builder \
        .appName("NarradorAtletismo") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

def procesar_narracion(batch_df, batch_id):
    """
    ZONA DE IA: Aquí es donde ocurre la magia.
    Este método se ejecuta cada vez que llega un grupo de datos.
    """
    # PASO 5: Integración con LangChain / OpenAI
    # TAREA: 
    # 1. Convertir el batch_df (Spark) a algo que la IA entienda (lista o Pandas).
    # 2. Crear un Prompt que diga: "Eres un narrador de atletismo, comenta esto..."
    # 3. Llamar a OpenAI y hacer un print() del resultado.
    
    if batch_df.count() > 0:
        print(f"\n--- 🎙️ Procesando micro-batch {batch_id} ---")
        batch_df.show() # Por ahora solo mostramos los datos por tabla
        # VUESTRO CÓDIGO AQUÍ...

def iniciar_consumidor():
    spark = crear_spark_session()
    spark.sparkContext.setLogLevel("WARN") # Para no volvernos locos con logs

    # PASO 1: Definir el esquema de los datos que vienen de Kafka
    # TAREA: Crear un StructType que coincida con lo que envía el Producer.
    schema = StructType([
        StructField("corredor", StringType(), True),
        StructField("distancia_m", IntegerType(), True),
        StructField("tiempo_acumulado_s", FloatType(), True)
    ])

    # PASO 2: Conectar Spark con el Stream de Kafka
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_KAFKA) \
        .load()

    # PASO 3: Parsear el JSON (Kafka envía bytes, hay que pasarlos a columnas)
    df_json = df_raw.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # PASO 4: Lanzar la consulta en tiempo real
    # TAREA: Usar foreachBatch para enviar los datos a nuestra función de IA.
    query = df_json.writeStream \
        .foreachBatch(procesar_narracion) \
        .start()

    print("📡 Escuchando eventos en tiempo real... (Pulsa Ctrl+C para parar)")
    query.awaitTermination()

if __name__ == "__main__":
    iniciar_consumidor()
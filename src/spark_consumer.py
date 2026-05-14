import os
import pandas as pd
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

CORREDOR_PROTAGONISTA = "Hellen OBIRI"
TOTAL_CORREDORES_ESPERADOS = 15  
GAP_SEGUNDOS = 5.0

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
    StructField("id_competicion", StringType(), True),
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
    StructField("composicion_grupo", ArrayType(StringType()), True), 
    StructField("n_corredores_leidos", IntegerType(), True)
])

esquema_memoria_comentarista = StructType([
    StructField("grupo_en_cabeza", esquema_salida_comentarista, True),
    StructField("ultima_distancia_m", IntegerType(), True),
    StructField("grupos", ArrayType(esquema_salida_comentarista), True)
])

# Motor de conexión a base de datos
engine = create_engine("sqlite:///data/telemetria.db")

def guardar_en_db_coach(batch_df, batch_id):
    pdf = batch_df.toPandas()
    if not pdf.empty:
        pdf['procesado'] = 0
        pdf.to_sql("tabla_coach", con=engine, if_exists="append", index=False)
        print(f"📥 Coach Batch {batch_id}: {len(pdf)} filas guardadas en SQLite.")

def guardar_en_db_comentarista(batch_df, batch_id):
    pdf = batch_df.toPandas()
    if not pdf.empty:
        pdf['procesado'] = 0
        # SQLite no soporta arrays nativos, convertimos la lista a string separado por comas
        pdf["composicion_grupo"] = pdf["composicion_grupo"].apply(
            lambda x: ", ".join(x) if isinstance(x, list) else x
        )
        pdf.to_sql("tabla_comentarista", con=engine, if_exists="append", index=False)
        print(f"🎙️ Comentarista Batch {batch_id}: {len(pdf)} eventos emitidos a SQLite.")

# =====================================================================
# 2. MOTOR LÓGICO 1: EL COACH
# =====================================================================
def logica_coach(clave, pdfs_iter, estado):
    corredor = clave[0]
    
    if estado.exists:
        memoria = estado.get
        ultimo_acumulado = memoria[0]
        ultima_distancia = memoria[1]
    else:
        ultimo_acumulado = 0.0
        ultima_distancia = 0

    pdf_nuevos = pd.concat(list(pdfs_iter)).sort_values("timestamp")
    filas = []

    for i in range(len(pdf_nuevos)):
        row = pdf_nuevos.iloc[i]
        diccionario = {
            "nombre_corredor": corredor,
            "distancia_actual_m": int(row['distancia_metros']),
            "tiempo_parcial_s": float(row['timestamp'] - ultimo_acumulado),
            "tiempo_total_s": float(row['timestamp'])
        }
        filas.append(diccionario)
        ultimo_acumulado = row['timestamp']
        ultima_distancia = int(row['distancia_metros'])

    estado.update((ultimo_acumulado, ultima_distancia))
    yield pd.DataFrame(filas)

# =====================================================================
# 3. MOTOR LÓGICO 2: EL COMENTARISTA
# =====================================================================
def logica_comentarista(clave, pdfs_iter, estado):
    campos_info = [
        "tiempo_cabeza", "tiempo_cola", "dist_parcial", 
        "n_corredores", "n_grupo", "tipo_evento", "composicion_grupo",
        "n_corredores_leidos" 
    ]

    # =====================================================================
    # RAMA A: TIMEOUT (Silencio en el streaming tras 5 segundos)
    # =====================================================================
    if estado.hasTimedOut:
        memoria = estado.get
        grupo_en_cabeza = dict(zip(campos_info, memoria[0])) if memoria[0] else {}
        ultima_distancia_m = memoria[1]
        grupos = [dict(zip(campos_info, tupla)) for tupla in memoria[2]]

        filas_a_enviar = []
        grupos_tras_evaluar = []

        for grupo in grupos: 
            if grupo["n_corredores"] > 0:
                grupo_cerrado = grupo.copy()
                dist = grupo_cerrado['dist_parcial']
                es_lider = (grupo_cerrado['n_grupo'] == 1)

                # 1. Evaluamos si hay cambio de atletas en la cabeza de carrera
                es_rotura = False
                if es_lider and grupo_en_cabeza:
                    atletas_actuales = set(grupo_cerrado['composicion_grupo'])
                    atletas_anteriores = set(grupo_en_cabeza.get('composicion_grupo', []))
                    if atletas_actuales != atletas_anteriores:
                        es_rotura = True

                # 2. Asignación estricta por Jerarquía de Prioridad
                if dist == 5000:
                    grupo_cerrado['tipo_evento'] = "Meta 5000m"
                    filas_a_enviar.append(grupo_cerrado)
                elif es_rotura:
                    grupo_cerrado['tipo_evento'] = "Rotura"
                    filas_a_enviar.append(grupo_cerrado)
                elif dist % 400 == 0:
                    grupo_cerrado['tipo_evento'] = "Cierre Parcial"
                    filas_a_enviar.append(grupo_cerrado)
                # Si no es meta, ni rotura, ni múltiplo de 400, el grupo se cierra en silencio 
                # (parcial intermedio irrelevante) y no se notifica a la BBDD.

                # Preparamos el relevo vacío
                nuevo_grupo = grupo.copy()
                nuevo_grupo['tiempo_cabeza'] = 0.0
                nuevo_grupo['tiempo_cola'] = 0.0
                nuevo_grupo['n_corredores'] = 0
                nuevo_grupo['n_grupo'] += 1
                nuevo_grupo['composicion_grupo'] = []
                grupos_tras_evaluar.append(nuevo_grupo)

                if es_lider: 
                    grupo_en_cabeza = grupo_cerrado.copy()
            else:
                grupos_tras_evaluar.append(grupo)

        tupla_cabeza = tuple(grupo_en_cabeza[c] for c in campos_info) if grupo_en_cabeza else None
        lista_grupos_tuplas = [tuple(g[c] for c in campos_info) for g in grupos_tras_evaluar]
        estado.update((tupla_cabeza, ultima_distancia_m, lista_grupos_tuplas))

        yield pd.DataFrame(filas_a_enviar, columns=campos_info)
        return

    # =====================================================================
    # RAMA B: LECTURA NORMAL (Llegan nuevos registros)
    # =====================================================================
    else:
        corredores_nuevos = pd.concat(list(pdfs_iter)).sort_values("timestamp")
        
        if estado.exists:
            memoria = estado.get
            grupo_en_cabeza = dict(zip(campos_info, memoria[0])) if memoria[0] else {}
            ultima_distancia_m = memoria[1]
            grupos = [dict(zip(campos_info, tupla)) for tupla in memoria[2]]
        else:
            grupo_en_cabeza = {}
            ultima_distancia_m = 0
            grupos = [] 

        filas_a_enviar = []
        parciales_en_batch = corredores_nuevos['distancia_metros'].unique()

        for dist_actual in sorted(parciales_en_batch):
            dist_actual = int(dist_actual)
            corredores_parcial = corredores_nuevos[corredores_nuevos['distancia_metros'] == dist_actual]

            if dist_actual > ultima_distancia_m:
                ultima_distancia_m = dist_actual

            grupo_actual = next((g for g in grupos if g["dist_parcial"] == dist_actual), None)

            for _, row in corredores_parcial.iterrows():
                t_corredor = float(row['timestamp'])
                nombre = row['nombre_corredor']

                if not grupo_actual:
                    grupo_actual = {
                        "tiempo_cabeza": t_corredor, "tiempo_cola": t_corredor,
                        "dist_parcial": dist_actual, "n_corredores": 1, "n_grupo": 1,
                        "tipo_evento": None, "composicion_grupo": [nombre], "n_corredores_leidos": 1
                    }
                    grupos.append(grupo_actual)
                    continue

                # CASUÍSTICA: Rotura por distancia de GAP (> 5.0s)
                if t_corredor - grupo_actual["tiempo_cola"] > GAP_SEGUNDOS:
                    grupo_cerrado = grupo_actual.copy()
                    es_lider = (grupo_cerrado["n_grupo"] == 1)
                    
                    # Verificamos si además hubo cambio de nombres en cabeza
                    es_rotura_cabeza = False
                    if es_lider and grupo_en_cabeza:
                        if set(grupo_cerrado["composicion_grupo"]) != set(grupo_en_cabeza.get("composicion_grupo", [])):
                            es_rotura_cabeza = True

                    # Asignación estricta excluyente
                    if dist_actual == 5000:
                        grupo_cerrado["tipo_evento"] = "Meta 5000m"
                        filas_a_enviar.append(grupo_cerrado)
                    elif es_rotura_cabeza or (grupo_cerrado["n_grupo"] > 1):
                        # Toda fragmentación generada por un gap superior a 5s se etiqueta como Rotura
                        grupo_cerrado["tipo_evento"] = "Rotura"
                        filas_a_enviar.append(grupo_cerrado)
                    elif dist_actual % 400 == 0:
                        grupo_cerrado["tipo_evento"] = "Cierre Parcial"
                        filas_a_enviar.append(grupo_cerrado)

                    if es_lider:
                        grupo_en_cabeza = grupo_cerrado.copy()

                    grupos.remove(grupo_actual)
                    
                    # Generamos el nuevo grupo para el corredor descolgado
                    total_leidos = grupo_cerrado["n_corredores_leidos"] + 1
                    grupo_actual = {
                        "tiempo_cabeza": t_corredor, "tiempo_cola": t_corredor,
                        "dist_parcial": dist_actual, "n_corredores": 1,
                        "n_grupo": grupo_cerrado["n_grupo"] + 1, "tipo_evento": None,
                        "composicion_grupo": [nombre], "n_corredores_leidos": total_leidos
                    }
                    grupos.append(grupo_actual)

                else:
                    grupo_actual["composicion_grupo"].append(nombre)
                    grupo_actual["tiempo_cola"] = t_corredor
                    grupo_actual["n_corredores"] += 1
                    grupo_actual["n_corredores_leidos"] += 1

                # CASUÍSTICA: Cierre natural por cuórum de participantes
                if grupo_actual["n_corredores_leidos"] >= TOTAL_CORREDORES_ESPERADOS:
                    grupo_cerrado = grupo_actual.copy()
                    es_lider = (grupo_cerrado["n_grupo"] == 1)
                    
                    es_rotura_cabeza = False
                    if es_lider and grupo_en_cabeza:
                        if set(grupo_cerrado["composicion_grupo"]) != set(grupo_en_cabeza.get("composicion_grupo", [])):
                            es_rotura_cabeza = True

                    # Asignación estricta excluyente
                    if dist_actual == 5000:
                        grupo_cerrado["tipo_evento"] = "Meta 5000m"
                        filas_a_enviar.append(grupo_cerrado)
                    elif es_rotura_cabeza:
                        grupo_cerrado["tipo_evento"] = "Rotura"
                        filas_a_enviar.append(grupo_cerrado)
                    elif dist_actual % 400 == 0:
                        grupo_cerrado["tipo_evento"] = "Cierre Parcial"
                        filas_a_enviar.append(grupo_cerrado)

                    if es_lider:
                        grupo_en_cabeza = grupo_cerrado.copy()

                    grupos.remove(grupo_actual)
                    grupo_actual = None

        estado.setTimeoutDuration(5000)
        grupos = [g for g in grupos if (ultima_distancia_m - g["dist_parcial"]) <= 800]
        
        tupla_cabeza = tuple(grupo_en_cabeza[c] for c in campos_info) if grupo_en_cabeza else None
        lista_grupos = [tuple(g[c] for c in campos_info) for g in grupos]
        estado.update((tupla_cabeza, ultima_distancia_m, lista_grupos))

        yield pd.DataFrame(filas_a_enviar, columns=campos_info)
# =====================================================================
# 4. ORQUESTACIÓN PRINCIPAL
# =====================================================================
def iniciar_consumidor():
    spark = crear_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Conectando a Kafka y bifurcando flujos...")
    broker = os.environ.get("KAFKA_BROKER", KAFKA_BROKER)
    
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
        .groupBy("id_competicion")
        .applyInPandasWithState(
            logica_comentarista, 
            outputStructType=esquema_salida_comentarista,
            stateStructType=esquema_memoria_comentarista,
            outputMode="append",
            timeoutConf=GroupStateTimeout.ProcessingTimeTimeout
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
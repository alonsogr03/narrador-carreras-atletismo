import csv
import pandas as pd
import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# =====================================================================
# CONFIGURACIÓN
# =====================================================================
ARCHIVO_CSV = "data/static/simulacion_carrera.csv"
TOPIC_KAFKA = "race_events"
FACTOR_VELOCIDAD = 0.1  # 0.1 = x10 de velocidad. 1.0 = Tiempo real.

def inicializar_productor():
    """Conecta con el servidor de Kafka en Docker."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        return producer
    except NoBrokersAvailable:
        print("❌ ERROR: No se encuentra Kafka. ¿Has ejecutado 'docker-compose up -d'?")
        exit(1)

def enviar_datos_simulados():
    producer = inicializar_productor()
    print("✅ Productor conectado a Kafka. ¡Arrancando motores!")
    
    # =====================================================================
    # 👩‍💻👨‍💻 ZONA DE PROGRAMACIÓN EN PAREJA (Vuestro turno)
    # =====================================================================
    
    # PASO 1: Cargar el CSV'
    eventos = pd.read_csv(ARCHIVO_CSV)
    # Cambiamos el tipo de dato de las columnas distancia y tiempo acumulado
    eventos['distancia_metros'] = eventos['distancia_metros'].astype(int)
    eventos['tiempo_acumulado_segundos'] = eventos['tiempo_acumulado_segundos'].astype(float)

    # PASO 2: Ordenar los datos cronológicamente
    eventos = eventos.sort_values(by='tiempo_acumulado_segundos').reset_index(drop=True)
    
    
    print(f"📊 Datos listos. Empezando retransmisión...")
    print("-" * 60)

    # PASO 3: El Reloj de la Carrera
    tiempo_anterior = 0.0

    # PASO 4: El Bucle del Directo
    for i in range(len(eventos)):
        split = eventos.iloc[i]
        tiempo = split["tiempo_acumulado_segundos"]
        diferencia = tiempo - tiempo_anterior
        if diferencia > 0:
            time.sleep(diferencia * FACTOR_VELOCIDAD)
        
        mensaje = {
                "schema_version": "1.0",
                "id_evento": str(split["id_evento"]),
                "nombre_corredor": str(split["nombre_corredor"]),
                "timestamp": float(tiempo),
                "distancia_metros": int(split['distancia_metros'])
            }
        
        producer.send(
                topic = TOPIC_KAFKA, 
                #key = player_id.encode('utf-8'), 
                value = mensaje
                #timestamp_ms = timestamp
            )
        print(f"Enviando: {mensaje['nombre_corredor']} - {mensaje['distancia_metros']}m")
        tiempo_anterior = tiempo
    
    producer.flush()
    print("-" * 60)
    print("🏁 FIN DE LA CARRERA. Todos los datos enviados.")

if __name__ == "__main__":
    enviar_datos_simulados()
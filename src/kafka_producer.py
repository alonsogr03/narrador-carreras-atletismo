import csv
import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# =====================================================================
# CONFIGURACIÓN
# =====================================================================
ARCHIVO_CSV = "data/static/carrera_5000m.csv"
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
    
    # PASO 1: Cargar el CSV
    # TAREA: Abrid ARCHIVO_CSV, leedlo con DictReader y guardad los eventos en una lista.
    # PISTA: Recordad convertir 'tiempo_acumulado_s' a float y 'distancia_m' a int.
    eventos = []
    
    
    # PASO 2: Ordenar los datos cronológicamente
    # TAREA: Aseguraos de que la lista esté ordenada por 'tiempo_acumulado_s'.
    
    
    print(f"📊 Datos listos. Empezando retransmisión...")
    print("-" * 60)

    # PASO 3: El Reloj de la Carrera
    # TAREA: Inicializad una variable 'tiempo_anterior' a 0.0.
    

    # PASO 4: El Bucle del Directo
    # TAREA: Recorred la lista de eventos. 
    # En cada vuelta:
    #   A) Calculad la diferencia de tiempo con el evento anterior.
    #   B) Si la diferencia > 0, haced un time.sleep(diferencia * FACTOR_VELOCIDAD).
    #   C) Enviad el evento a Kafka: producer.send(TOPIC_KAFKA, value=evento).
    #   D) Actualizad 'tiempo_anterior'.
    
    
    # =====================================================================
    
    producer.flush()
    print("-" * 60)
    print("🏁 FIN DE LA CARRERA. Todos los datos enviados.")

if __name__ == "__main__":
    enviar_datos_simulados()
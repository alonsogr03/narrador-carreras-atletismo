import os
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

def inicializar_topic():
    # Conecta al broker usando la misma variable de entorno que tus otros scripts
    broker = os.environ.get("KAFKA_BROKER", "kafka:29092")
    topic_name = "race_events"
    
    print(f"🔌 Conectando al broker {broker} para verificar clúster...")
    
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=[broker])
        
        # Definimos el topic (1 partición, factor de replicación 1 para local)
        nuevo_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        
        print(f"🔨 Creando topic '{topic_name}'...")
        admin_client.create_topics([nuevo_topic])
        print(f"✅ ¡Éxito! Topic '{topic_name}' creado correctamente.")
        
    except TopicAlreadyExistsError:
        print(f"👍 El topic '{topic_name}' ya existe. Vía libre.")
    except Exception as e:
        print(f"❌ Error inesperado al comunicar con Kafka: {e}")
        exit(1)

if __name__ == "__main__":
    inicializar_topic()
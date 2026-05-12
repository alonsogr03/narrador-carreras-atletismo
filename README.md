# 🏃‍♀️ Retransmisión de Carrera en Directo (Streaming con Spark & Kafka)

¡Bienvenida! Este proyecto simula la retransmisión en tiempo real de una carrera de atletismo. Utiliza contenedores de Docker para que no tengas que instalar nada complejo en tu ordenador; toda la magia de procesamiento de datos ocurre de forma invisible en segundo plano.

Sigue estos pasos para arrancar la carrera y ver las estadísticas en directo.

---

## 🚀 Guía Rápida de Ejecución

### 1. Preparar y Levantar el Entorno
Abre una terminal en la carpeta principal del proyecto y ejecuta el siguiente comando para encender la infraestructura:

```bash
docker compose up --build -d
```

(Nota: Espera unos 10-15 segundos para que los sistemas internos terminen de arrancar completamente).


### 2. Levantar el entorno en cualquier otro momento

```bash
docker compose up -d
```

### 3. Inicializar el canal de comunicación entre kafka y spark

Para ello, seguimos estos pasos en terminal dentro del localhost:8888

1º Inicializamos topic: python src/init_topic.py
2º Inicializamos al consumer para que se quede a la espera. python src/spark_consumer.py
3º Abrimos una nueva terminal (sin cerrar la anterior) y ejecutamos python src/kafka_producer.py



Y bingo! En la BBDD se van cargando las cositas :)
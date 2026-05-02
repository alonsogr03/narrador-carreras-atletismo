# 🏃‍♂️ Sistema de Narración y Coaching en Directo (5000m Atletismo)

Este proyecto implementa un sistema de análisis deportivo en tiempo real utilizando Big Data e Inteligencia Artificial Generativa. Simula una carrera de 5000m, procesando los tiempos de paso de los atletas, detectando roturas en el pelotón, y generando informes y comentarios en directo mediante un sistema multi-agente con LangGraph y RAG.

## 🛠️ Tecnologías Utilizadas
*   **Ingesta:** Apache Kafka
*   **Procesamiento:** Apache Spark (Structured Streaming)
*   **IA & Orquestación:** LangGraph, LangChain, OpenAI/Gemini (LLMs)
*   **RAG:** ChromaDB (Vector Store)
*   **Frontend:** Streamlit

---

## ⚙️ 1. Instalación y Configuración (Para Profesores/Evaluadores)

Siga estos pasos para levantar el proyecto en local de forma aislada:

### 1.1. Dependencia Crítica: Java (Para Apache Spark)
Dado que PySpark requiere la Máquina Virtual de Java (JVM) para funcionar, es obligatorio tener instalado Java (versión 8, 11 o 17).
*   Comprobar si está instalado: Abra su terminal y ejecute `java -version`.
*   Si no lo tiene, siga estas instrucciones según su sistema:
    *   Windows (Opción fácil): Descargue el instalador `.msi` desde Adoptium (https://adoptium.net/temurin/releases/?version=11). ¡Importante! Durante la instalación, asegúrese de marcar la casilla "Set JAVA_HOME variable".
    *   Windows (Vía terminal/Git Bash usando winget): Abra su terminal como administrador y ejecute `winget install Microsoft.OpenJDK.11`
    *   Mac: Ejecute `brew install openjdk@11`
    *   Linux: Ejecute `sudo apt install openjdk-11-jdk`

### 1.2. Entorno Virtual de Python (venv)
Se recomienda encarecidamente utilizar el entorno virtual nativo de Python para no generar conflictos de librerías.

1. Crear el entorno virtual:
python -m venv venv

2. Activar el entorno:
(Windows): .\venv\Scripts\activate
(Mac/Linux): source venv/bin/activate

3. Instalar dependencias:
pip install -r requirements.txt

### 1.3. Configurar Variables de Entorno
Cree un archivo `.env` en la raíz del proyecto y añada sus claves de API para los modelos generativos:
OPENAI_API_KEY="sk-tu-clave-aqui"

### 1.4. Levantar Servicios (Kafka)
Asegúrese de tener Docker Desktop iniciado y ejecute:
docker-compose up -d

---

## 🚀 2. Guía de Ejecución

Para ver el sistema funcionando en directo, es necesario abrir 4 terminales distintas (asegúrese de tener el entorno virtual activado en todas ellas).

*   Terminal 1 (Preparación e Indexación RAG):
    Genera los datos sintéticos de la carrera y carga los documentos históricos en la base de datos vectorial.
    python src/generate_data.py
    python src/rag_pipeline.py

*   Terminal 2 (Spark Streaming):
    Arranca el motor de procesamiento analítico. Se quedará escuchando a Kafka.
    python src/streaming_pipeline.py

*   Terminal 3 (La Interfaz de Streamlit):
    Arranca la aplicación web donde se visualizarán la carrera, los comentarios del narrador y los consejos del coach.
    streamlit run src/app.py

*   Terminal 4 (El Directo - Productor Kafka):
    Inicia la simulación de la carrera, enviando los eventos segundo a segundo.
    python src/kafka_producer.py

---

## 🗄️ 3. Arquitectura de Datos (Base de Datos Local)

El sistema utiliza SQLite como puente ultrarrápido y almacenamiento estructurado entre el procesamiento Big Data (Spark) y el cerebro de IA (LangGraph)[cite: 1]. Spark divide la información calculada en dos tablas principales:

1.  tabla_eventos (El Teletipo de Noticias):
    *   Almacena hitos puntuales en el tiempo. 
    *   Columnas: timestamp, tipo_evento (ROTURA, PASO_PARCIAL), detalles.
    *   Uso: Despierta al Agente Narrador para comentar el directo.
2.  tabla_metricas (El Leaderboard):
    *   Almacena el estado actual e histórico de cada corredor.
    *   Columnas: atleta_id, posicion_actual, split_100m, velocidad_media, etc.
    *   Uso: Usada por el Agente Coach para dar feedback, y por el Agente del Informe Final para tener la radiografía exacta de la carrera.

---

## 🔄 4. Flujo del Sistema (Casos de Uso Prácticos)

A continuación, se detalla qué hace cada componente cuando ocurren distintos escenarios en la carrera:

Ejemplo A: Paso normal por el parcial de 1000m
1.  Productor: Envía a Kafka que todos los corredores han pasado los 1000m.
2.  Spark: Calcula la posición, ritmos y actualiza la tabla_metricas[cite: 1]. Al ver que todos han pasado, inserta un evento PASO_PARCIAL_1000M en la tabla_eventos.
3.  LangGraph (Narrador): Lee el nuevo evento. Usa su Tool de BBDD para ver quién va primero en la tabla_metricas. Usa su Tool RAG para buscar el contexto táctico. Imprime: "¡Pasamos el primer kilómetro! El grupo se mantiene compacto..."

Ejemplo B: Katir se queda rezagado (>5 segundos)
1.  Productor: Envía el tiempo de Katir en los 3000m mucho más tarde que el líder.
2.  Spark: Compara tiempos. ¡Diferencia > 5s! Inserta un evento de ROTURA en tabla_eventos.
3.  LangGraph (Narrador): Detecta la alerta. Consulta el RAG buscando "Rotura de pelotón". Imprime: "¡Alarma en la pista! Katir pierde fuelle..."

Ejemplo C: Insight Individual (El Coach)
1.  Productor: Envía el paso por los 400m de nuestro atleta (Kipchoge).
2.  Spark: Registra el tiempo en la tabla_metricas.
3.  LangGraph (Coach): Detecta la actualización. Consulta la estrategia predefinida y la compara con la tabla de métricas. Imprime: "Bien Eliud, has pasado en 60s, vas 2s más rápido que el plan."

Ejemplo D: Fin de Carrera
1.  Productor: Se envían los tiempos de los 5000m.
2.  Spark: Inserta el evento FIN_CARRERA.
3.  LangGraph (Redactor Jefe): Ignora el directo. Lee TODA la tabla_metricas. Lee documentos del RAG. Redacta el informe automático final y lo guarda en Markdown[cite: 1].


# 🏎️ Narrador de Carreras de Atletismo (Real-Time)

Este proyecto simula y analiza eventos de carreras de atletismo en tiempo real utilizando una arquitectura de Big Data. Para asegurar la compatibilidad de todas las librerías (especialmente PySpark y LangChain), este proyecto ha sido desarrollado bajo un entorno específico de **Python 3.11.9** y **Java 11**, evitando así los errores de compilación comunes en versiones más recientes como la 3.13.

## 📥 Guía de Instalación Paso a Paso (Windows + Git Bash)

**1. Instalación de Java (OpenJDK 11):** Apache Spark requiere Java 11. Abre Git Bash y ejecuta `winget install Microsoft.OpenJDK.11`. Tras la instalación, reinicia Git Bash y comprueba que funciona con `java -version`.

**2. Instalación de Python 3.11.9:** Es fundamental para evitar el "infierno de dependencias". Ejecuta `winget install Python.Python.3.11`. Comprueba que está disponible con el comando `py -3.11 --version`.

**3. Configuración del Entorno Virtual:** En la carpeta raíz del proyecto, ejecuta los siguientes comandos para limpiar y crear un entorno estable:
`rm -rf venv` (borra intentos previos)
`py -3.11 -m venv venv` (crea el entorno con la versión correcta)
`source venv/Scripts/activate` (activa el entorno; verás el prefijo `(venv)` en la terminal).

**4. Instalación de Dependencias:** Con el entorno activado, actualiza las herramientas base e instala los requisitos evitando compilaciones locales de C++:
`python -m pip install --upgrade pip setuptools wheel`
`pip install -r requirements.txt --only-binary :all:`

**5. Configuración de Hadoop/Winutils:** Spark en Windows necesita un apoyo extra. Crea la carpeta `C:\hadoop\bin`, descarga el archivo `winutils.exe` de Hadoop 3.0.0 y colócalo dentro. Después, ejecuta en tu terminal:
`export HADOOP_HOME="C:/hadoop"`
`export PATH="$PATH:$HADOOP_HOME/bin"`

## 🚀 Ejecución del Proyecto

Sigue este orden para poner en marcha la arquitectura:
1. **Infraestructura:** Con Docker Desktop abierto, levanta Kafka con `docker-compose up -d`.
2. **Generación:** Crea el dataset estático con `python src/generate_data.py`.
3. **Producción:** Inicia el streaming de datos a Kafka con `python src/kafka_producer.py`.
4. **Consumo:** Lanza el motor de Spark para procesar los eventos en tiempo real.

---
**Nota para Evaluadores:** Debido a que librerías críticas como `pyspark`, `pandas` y `chromadb` requieren compiladores de C++ específicos para instalarse desde código fuente en Python 3.12+, este proyecto exige el uso de **Python 3.11** mediante el Python Launcher (`py`) para garantizar una replicación exitosa y el uso de paquetes binarios pre-compilados.
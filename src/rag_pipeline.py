import os
from dotenv import load_dotenv
from langchain_community.document_loaders import TextLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
from langchain_chroma import Chroma

load_dotenv()

DIRECTORIO_CHROMA = "./data/chroma_db"
RUTA_COACH = "./data/docs/umbrales_coach_obiri.txt"
RUTA_NARRADOR = "./data/docs/base_conocimiento_narrador.txt"

# =====================================================================
# 1. INICIALIZACIÓN DE CONEXIONES (LECCIÓN DE DISEÑO: EVITAR RE-CREAR)
# =====================================================================
def obtener_db_coach():
    """Devuelve el cliente de conexión a la colección del Coach."""
    embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
    return Chroma(
        persist_directory=DIRECTORIO_CHROMA, 
        embedding_function=embeddings,
        collection_name="coleccion_coach"
    )

def obtener_db_narrador():
    """Devuelve el cliente de conexión a la colección del Narrador."""
    embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
    return Chroma(
        persist_directory=DIRECTORIO_CHROMA, 
        embedding_function=embeddings,
        collection_name="coleccion_narrador"
    )

# =====================================================================
# 2. CONSTRUCCIÓN DE LA BBDD (EJECUTAR UNA VEZ)
# =====================================================================
def construir_rag():
    print("🧠 Creando colecciones en ChromaDB...")
    embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
    # chunk_size=800 asegura que cada sección del TXT quepa en un solo documento
    splitter1 = RecursiveCharacterTextSplitter(chunk_size=400, chunk_overlap=100)
    splitter2 = RecursiveCharacterTextSplitter(chunk_size=600, chunk_overlap=50)

    # Colección 1: Coach
    if os.path.exists(RUTA_COACH):
        docs = TextLoader(RUTA_COACH, encoding="utf-8").load()
        chunks = splitter1.split_documents(docs)
        Chroma.from_documents(chunks, embeddings, persist_directory=DIRECTORIO_CHROMA, collection_name="coleccion_coach")
        print("✅ RAG Coach listo.")
    else:
        print(f"⚠️ Archivo no encontrado: {RUTA_COACH}")

    # Colección 2: Narrador
    if os.path.exists(RUTA_NARRADOR):
        docs = TextLoader(RUTA_NARRADOR, encoding="utf-8").load()
        chunks = splitter2.split_documents(docs)
        Chroma.from_documents(chunks, embeddings, persist_directory=DIRECTORIO_CHROMA, collection_name="coleccion_narrador")
        print("✅ RAG Narrador listo.")
    else:
        print(f"⚠️ Archivo no encontrado: {RUTA_NARRADOR}")

# =====================================================================
# 3. INTERFAZ DE CONSULTA PARA LANGGRAPH (HERRAMIENTAS RAG)
# =====================================================================
def consultar_tactica_rag(distancia: int) -> str:
    """
    Inyecta semántica determinista basada en los tramos de la carrera
    para extraer exactamente la sección táctica correspondiente de Obiri.
    """
    # Mapeo directo para forzar un emparejamiento perfecto con las cabeceras del archivo
    if distancia <= 1200:
        query = "SECCIÓN 1 (100m - 1200m): FASE DE COLOCACIÓN Y AHORRO"
    elif distancia <= 2400:
        query = "SECCIÓN 2 (1300m - 2400m): TRANSICIÓN AL RITMO DE CRUCERO"
    elif distancia <= 4000:
        query = "SECCIÓN 3 (2500m - 4000m): RESISTENCIA AL \"HACHAZO\" DE AYANA"
    elif distancia <= 4600:
        query = "SECCIÓN 4 (4100m - 4600m): PREPARACIÓN DEL ZARPAZO"
    else:
        query = "SECCIÓN 5 (4700m - 5000m): EXPLOSIÓN Y CIERRE DE ORO"

    try:
        db = obtener_db_coach()
        # k=1 recupera el bloque completo del tramo gracias al tamaño del chunk
        docs = db.similarity_search(query, k=1)
        return docs[0].page_content if docs else "Sin directivas tácticas en el RAG."
    except Exception as e:
        print(f"❌ Error consultando ChromaDB (Coach): {e}")
        return "Error interno de base documental."


def consultar_historial_roturas(distancia: int, atletas_implicadas: str) -> str:
    """
    Recupera el contexto histórico combinando la distancia del incidente
    y las identidades de las atletas involucradas.
    """
    # Construcción de una consulta semántica densa
    query = (
        f"Precedentes históricos, explicaciones tácticas y paralelos en el tramo de [Metros: {distancia}m] "
        f"o rangos cercanos a los {distancia} metros. "
        f"Información, arquetipos y nacionalidades de las atletas: {atletas_implicadas}."
    )
    
    try:
        db = obtener_db_narrador()
        # Extraemos los 2 fragmentos más afines para fusionar biografía e historia
        docs = db.similarity_search(query, k=2)
        
        if not docs:
            return "Sin registros de precedentes para este tramo en la base de conocimiento."
            
        return "\n\n".join([doc.page_content for doc in docs])
        
    except Exception as e:
        print(f"❌ Error en la recuperación documental (Narrador): {e}")
        return "Error de conexión con el motor RAG."

if __name__ == "__main__":
    construir_rag()
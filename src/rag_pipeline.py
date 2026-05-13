import os
from dotenv import load_dotenv
from langchain_community.document_loaders import TextLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
from langchain_chroma import Chroma

load_dotenv()

DIRECTORIO_CHROMA = "./data/chroma_db"
RUTA_COACH = "./data/docs/umbrales_coach_marta.txt"
RUTA_NARRADOR = "./data/docs/base_conocimiento_narrador.txt"



# LO TENEMOS QUE AJUSTAR SEGÚN DEJEMOS HECHO LOS RAGS

def construir_rag():
    print("🧠 Creando colecciones en ChromaDB...")
    embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
    splitter = RecursiveCharacterTextSplitter(chunk_size=800, chunk_overlap=100)

    # Colección 1: Coach
    if os.path.exists(RUTA_COACH):
        docs = TextLoader(RUTA_COACH, encoding="utf-8").load()
        chunks = splitter.split_documents(docs)
        Chroma.from_documents(chunks, embeddings, persist_directory=DIRECTORIO_CHROMA, collection_name="coleccion_coach")
        print("✅ RAG Coach listo.")

    # Colección 2: Narrador
    if os.path.exists(RUTA_NARRADOR):
        docs = TextLoader(RUTA_NARRADOR, encoding="utf-8").load()
        chunks = splitter.split_documents(docs)
        Chroma.from_documents(chunks, embeddings, persist_directory=DIRECTORIO_CHROMA, collection_name="coleccion_narrador")
        print("✅ RAG Narrador listo.")

if __name__ == "__main__":
    construir_rag()
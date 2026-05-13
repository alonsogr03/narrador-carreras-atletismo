import pandas as pd
from sqlalchemy import create_engine
from langchain_core.tools import tool
from langchain_openai import OpenAIEmbeddings
from langchain_chroma import Chroma
from dotenv import load_dotenv

load_dotenv()

# Conexiones globales para las herramientas
engine = create_engine("sqlite:///data/telemetria.db")
embeddings = OpenAIEmbeddings(model="text-embedding-3-small")

# Conectamos a las colecciones generadas por rag_pipeline.py
db_coach = Chroma(persist_directory="./data/chroma_db", embedding_function=embeddings, collection_name="coleccion_coach")
db_narr = Chroma(persist_directory="./data/chroma_db", embedding_function=embeddings, collection_name="coleccion_narrador")

retriever_coach = db_coach.as_retriever(search_kwargs={"k": 2})
retriever_narr = db_narr.as_retriever(search_kwargs={"k": 2})

@tool
def consultar_metricas_spark(tipo: str) -> str:
    """Tool 1. Extrae métricas calculadas por Spark desde SQLite."""
    try:
        if tipo == "narrador":
            df = pd.read_sql("SELECT * FROM tabla_comentarista ORDER BY dist_parcial DESC LIMIT 1", con=engine)
        elif tipo == "coach":
            df = pd.read_sql("SELECT * FROM tabla_coach ORDER BY distancia_actual_m DESC LIMIT 1", con=engine)
        else:
            return "Tipo incorrecto."
        return df.to_json(orient="records") if not df.empty else "Sin datos."
    except Exception as e:
        return f"Error BBDD: {e}"

@tool
def consultar_rag_coach(query: str) -> str:
    """Tool 2A. Busca umbrales y tiempos objetivo en el RAG del Coach."""
    docs = retriever_coach.invoke(query)
    return "\n".join([d.page_content for d in docs]) if docs else "Sin contexto."

@tool
def consultar_rag_narrador(query: str) -> str:
    """Tool 2B. Busca historias y precedentes en el RAG del Narrador."""
    docs = retriever_narr.invoke(query)
    return "\n".join([d.page_content for d in docs]) if docs else "Sin contexto."
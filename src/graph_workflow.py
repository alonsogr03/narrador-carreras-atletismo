from typing import Annotated, TypedDict
from langchain_openai import ChatOpenAI
from langchain_core.messages import AnyMessage, SystemMessage, HumanMessage
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode, tools_condition
from langgraph.checkpoint.memory import MemorySaver

# Importamos estrictamente nuestras herramientas modulares
from tools import consultar_metricas_spark, consultar_rag_coach, consultar_rag_narrador

llm = ChatOpenAI(model="gpt-4o", temperature=0.3)

class AgentState(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]

# Vinculamos herramientas asignadas a cada cerebro para que no se crucen
tools_coach = [consultar_metricas_spark, consultar_rag_coach]
tools_narr = [consultar_metricas_spark, consultar_rag_narrador]

def nodo_coach(state: AgentState):
    messages = state["messages"]
    if not any(isinstance(m, SystemMessage) for m in messages):
        sys = SystemMessage("Eres el entrenador de Marta García. Usa tus tools para ver su tiempo real y compararlo con el RAG. Dale una orden corta y directa.")
        messages = [sys] + messages
    return {"messages": [llm.bind_tools(tools_coach).invoke(messages)]}

def nodo_narrador(state: AgentState):
    messages = state["messages"]
    if not any(isinstance(m, SystemMessage) for m in messages):
        sys = SystemMessage("Eres un narrador de atletismo. Usa tus tools para ver quién lidera y extraer anécdotas del RAG. Narra con pasión.")
        messages = [sys] + messages
    return {"messages": [llm.bind_tools(tools_narr).invoke(messages)]}

# Compilación de los dos flujos independientes
memory = MemorySaver()

def compilar(nodo, tools_list):
    g = StateGraph(AgentState)
    g.add_node("agent", nodo)
    g.add_node("tools", ToolNode(tools_list))
    g.add_edge(START, "agent")
    g.add_conditional_edges("agent", tools_condition)
    g.add_edge("tools", "agent")
    return g.compile(checkpointer=memory)

app_coach = compilar(nodo_coach, tools_coach)
app_narr = compilar(nodo_narrador, tools_narr)

# Salidas directas para Streamlit
def ejecutar_agente_coach(evento: dict, tid: str) -> str:
    res = app_coach.invoke({"messages": [HumanMessage(f"Dato pista: {evento}")]}, {"configurable": {"thread_id": tid}})
    return res["messages"][-1].content

def ejecutar_agente_comentarista(evento: dict, tid: str) -> str:
    res = app_narr.invoke({"messages": [HumanMessage(f"Dato pista: {evento}")]}, {"configurable": {"thread_id": tid}})
    return res["messages"][-1].content
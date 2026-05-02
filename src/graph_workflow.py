from langgraph.graph import StateGraph, END
# TODO: Definir el "Estado" (State) del grafo (mensajes, datos actuales, etc).

# --- NODOS (Los Agentes) ---
# TODO: Crear función/nodo 'agente_narrador'. Le pasamos el estado, usa la Tool 1 (Métricas) y Tool 2 (RAG) y llama al LLM.
# TODO: Crear función/nodo 'agente_coach'. Le pasamos el estado, usa Tool 1 y Tool 3 y llama al LLM.

# --- GRAFO ---
# TODO: Iniciar StateGraph.
# TODO: Añadir los nodos al grafo.
# TODO: Definir las conexiones (Edges). Ej: Inicio -> Narrador -> Fin.
# TODO: Compilar el grafo.
from typing import Annotated, TypedDict, List, Dict, Any
from langchain_openai import ChatOpenAI
from langchain_core.messages import AnyMessage, SystemMessage, HumanMessage
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode, tools_condition
from langgraph.checkpoint.memory import MemorySaver
from rag_pipeline import consultar_tactica_rag
from rag_pipeline import consultar_historial_roturas


# =====================================================================
# 1. ESTADOS 
# =====================================================================
class CoachState(TypedDict):
    eventos_raw: list[dict]                             # El batch que llega de Streamlit
    metricas: dict                                      # Cálculo procesado en el Nodo 1
    contexto_rag: str                                   # Info recuperada en el Nodo 2
    comentario_final: str                               # Output de texto plano para la UI

class NarradorState(TypedDict):
    # Lista de eventos brutos enviados por Streamlit
    eventos_pendientes: List[Dict[str, Any]]
    
    # El evento individual que toca comentar en la iteración actual
    evento_actual: Dict[str, Any]
    
    # Texto formateado o datos recuperados por la Tool seleccionada
    contexto_herramienta: str
    
    # Memoria interna: lista de los últimos comentarios generados en este hilo
    historial_narrativa: List[str]
    
    # Lista acumulativa donde guardamos los párrafos finales de este lote
    comentarios_lote: List[str]


# Inicializamos el LLM (Ajusta el modelo según tus variables de entorno)
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.1)


# =====================================================================
# 2. NODO 1
# =====================================================================
def nodo_calcular_metricas(state: CoachState):
    eventos = state.get("eventos_raw", [])
    ultimo_evento = eventos[-1] 

    distancia_actual = ultimo_evento.get("distancia_actual_m", 0)
    
    # Redondeo a 2 decimales
    tiempo_total = round(ultimo_evento.get("tiempo_total_s", 0.0), 2)
    tiempo_parcial = round(ultimo_evento.get("tiempo_parcial_s", 0.0), 2)
    
    # 3. Cálculo directo del ritmo por km
    if tiempo_parcial > 0:
        segundos_por_km = tiempo_parcial * 10
        minutos = int(segundos_por_km // 60)
        segundos = int(segundos_por_km % 60)
        ritmo_str = f"{minutos}:{segundos:02d} min/km"
    else:
        ritmo_str = "N/A"

    # 4. Empaquetamos la salida limpia
    metricas_calculadas = {
        "corredor": ultimo_evento.get("nombre_corredor"),
        "distancia_m": distancia_actual,
        "tiempo_total_s": tiempo_total,
        "tiempo_parcial_s": tiempo_parcial,
        "ritmo_parcial": ritmo_str
    }
    
    return {"metricas": metricas_calculadas}


def nodo_despachar_lote(state: NarradorState):
    pendientes = state.get("eventos_pendientes", []).copy()
    if len(pendientes) > 1 and not state.get("evento_actual"):
        pendientes.sort(key=lambda x: float(x.get("tiempo_cola", 0.0)))
    
    evento_toca = pendientes.pop(0) if pendientes else {}
    return {
        "eventos_pendientes": pendientes,
        "evento_actual": evento_toca
    }

# =====================================================================
# 3. NODO 2: CONSULTA RAG DOCUMENTAL
# =====================================================================
def nodo_consultar_rag(state: CoachState):
    metricas = state.get("metricas", {})
    distancia = metricas.get("distancia_m", 0)
    
    # 2. Invocamos el RAG pasándole la distancia actual.
    try:
        contexto_recuperado = consultar_tactica_rag(distancia)
    except Exception as e:
        print(f"⚠️ Aviso: Fallo al comunicar con el módulo RAG: {e}")
        contexto_recuperado = "Error: No se pudo recuperar el contexto táctico para esta distancia."
    
    # 3. Volcamos el texto plano resultante en el Estado del grafo
    return {"contexto_rag": contexto_recuperado}


def enrutar_evento(state: NarradorState) -> str:
    evento = state.get("evento_actual", {})
    tipo = str(evento.get("tipo_evento", ""))
    
    if "Meta" in tipo:
        return "tool_meta"
    elif "Rotura" in tipo:
        return "tool_rotura"
    else:
        return "tool_cierre"

# =====================================================================
# 4. NODO 3: GENERACIÓN DE MENSAJE (COACH PROFESIONAL DIRECTO)
# =====================================================================
def nodo_generar_mensaje(state: CoachState):
    metricas = state.get("metricas", {})
    contexto_rag = state.get("contexto_rag", "Sin datos del plan.")
    
    corredor = metricas.get("corredor", "Marta GARCÍA")
    distancia = metricas.get("distancia_m", 0)
    parcial = metricas.get("tiempo_parcial_s", 0.0)

    # Prompt profesional con nomenclatura técnica inmersiva
    sys_msg = SystemMessage(content=f"""Eres el entrenador de pista de {corredor} en la final de 5000m. 
Genera una instrucción técnica de una sola frase, actuando en base a la telemetría en vivo y el plan preestablecido.

REQUISITO DE EVALUACIÓN: Para distinguir visualmente entre los datos del partido procesados en tiempo real y la información contextual recuperada, utiliza estrictamente el formato "(Real: Xs | Plan: Ys)".

FORMATO OBLIGATORIO DE SALIDA (Una sola línea):
Parcial [distancia]m (Real: [parcial]s | Plan: [tiempo_objetivo]s) -> [Instrucción técnica]

REGLAS PARA LA [Instrucción técnica]:
1. Si el parcial real es MENOR que el Umbral Inferior del plan (va demasiado rápido): Alerta del desgaste y pide regular/suavizar para clavar los [tiempo_objetivo]s.
2. Si el parcial real es MAYOR que el Umbral Superior del plan (va lenta): Alerta de la pérdida de ritmo y pide tensar/apretar para clavar los [tiempo_objetivo]s.
3. Si está dentro de los umbrales: Confirma el ritmo óptimo y pide mantener la zancada.

DATOS EN DIRECTO (Telemetría actual):
- Distancia: {distancia}m
- Parcial real: {parcial}s
- 

DATOS DEL PLAN TÁCTICO (Contexto Documental):
{contexto_rag}

EJEMPLOS DE SALIDA ESTRICTA:
Parcial 800m (Real: 17.2s | Plan: 18.0s) -> Ritmo demasiado elevado, afloja hasta los 18 segundos. Ritmo 3:02 min/km
Parcial 1300m (Real: 17.8s | Plan: 17.0s) -> Ritmo demasiado lento, aprieta hasta los 17 segundos. Ritmo 3:04 min/km
Parcial 2600m (Real: 18.4s | Plan: 18.5s) -> Ritmo perfecto, ¡sigue así!. Ritmo 3:06 min/km.
""")
    
    respuesta = llm.invoke([sys_msg])
    
    return {
        "comentario_final": respuesta.content
    }

def nodo_tool_cierre_parcial(state: NarradorState):
    ev = state.get("evento_actual", {})
    
    # 1. Extraemos las métricas básicas del evento
    dist = ev.get("dist_parcial", 0)
    grupo = ev.get("n_grupo", 1)
    t_cabeza = ev.get("tiempo_cabeza", 0.0)
    t_cola = ev.get("tiempo_cola", 0.0)
    n_corredores = ev.get("n_corredores", 0)
    n_leidos = ev.get("n_corredores_leidos", 0)
    
    # 2. Parseamos el string de SQLite a una lista nativa de Python
    corredores_str = ev.get("composicion_grupo", "")
    if isinstance(corredores_str, str) and corredores_str:
        # SQLite guarda "Juan, Luis, Pepe", dividimos por la coma
        lista_corredores = [c.strip() for c in corredores_str.split(",") if c.strip()]
    elif isinstance(corredores_str, list):
        lista_corredores = corredores_str
    else:
        lista_corredores = []

    # 3. Calculamos la posición global de inicio en la carrera para este pelotón
    pos_inicial = n_leidos - n_corredores + 1
    # Aseguramos un límite inferior seguro por si llega algún dato desajustado
    if pos_inicial < 1: 
        pos_inicial = 1
    
    # 4. Asignamos "el numerito" posicional a cada integrante
    integrantes_numerados = []
    for idx, nombre in enumerate(lista_corredores, start=pos_inicial):
        integrantes_numerados.append(f"{idx}. {nombre}")
        
    integrantes_str = ", ".join(integrantes_numerados)
    
    # 5. Formateamos un resumen estructurado, limpio y directo para la IA
    info = (
        f"[Datos de Paso Oficial - Parcial {dist}m]\n"
        f"- Grupo en pista: {grupo}\n"
        f"- Número de atletas en este pelotón: {n_corredores}\n"
        f"- Tiempo de cabeza (paso del líder del grupo): {round(t_cabeza, 2)}s\n"
        f"- Tiempo de cola (cierre del último del grupo): {round(t_cola, 2)}s\n"
        f"- Orden de carrera e integrantes: {integrantes_str}"
    )
    
    return {"contexto_herramienta": info}


def nodo_tool_rag_roturas(state: NarradorState):
    ev = state.get("evento_actual", {})
    
    dist = ev.get("dist_parcial", 0)
    grupo = ev.get("n_grupo", 2)
    t_cabeza = ev.get("tiempo_cabeza", 0.0)
    t_cola = ev.get("tiempo_cola", 0.0)
    n_corredores = ev.get("n_corredores", 0)
    
    corredores_str = ev.get("composicion_grupo", "")
    if isinstance(corredores_str, str) and corredores_str:
        lista_corredores = [c.strip() for c in corredores_str.split(",") if c.strip()]
    elif isinstance(corredores_str, list):
        lista_corredores = corredores_str
    else:
        lista_corredores = []
        
    implicadas_str = ", ".join(lista_corredores)

    # LE PASAMOS DISTANCIA Y NOMBRES AL RAG
    try:
        contexto_recuperado = consultar_historial_roturas(dist, implicadas_str)
    except Exception as e:
        print(f"⚠️ Aviso: Fallo al consultar el RAG histórico: {e}")
        contexto_recuperado = "En finales olímpicas, ceder terreno en este punto suele ser definitivo."

    info = (
        f"[¡ALERTA TÁCTICA: ROTURA DE CARRERA - PARCIAL {dist}m!]\n"
        f"- Situación en pista: Fractura confirmada. El Grupo {grupo} cruza descolgado con {n_corredores} atletas.\n"
        f"- Atletas implicadas en el corte: {implicadas_str}\n"
        f"- Tiempos de fractura (cabeza / cola de este grupo): {round(t_cabeza, 2)}s / {round(t_cola, 2)}s\n"
        f"- Contexto Histórico y Perfiles (RAG): {contexto_recuperado}"
    )
    
    return {"contexto_herramienta": info}


def nodo_tool_llegada_meta(state: NarradorState):
    ev = state.get("evento_actual", {})
    
    # 1. Extraemos las métricas definitivas de la llegada
    grupo = ev.get("n_grupo", 1)
    t_cabeza = ev.get("tiempo_cabeza", 0.0)
    t_cola = ev.get("tiempo_cola", 0.0)
    n_corredores = ev.get("n_corredores", 0)
    n_leidos = ev.get("n_corredores_leidos", 0)
    
    # 2. Parseamos el string de SQLite a una lista nativa
    corredores_str = ev.get("composicion_grupo", "")
    if isinstance(corredores_str, str) and corredores_str:
        lista_corredores = [c.strip() for c in corredores_str.split(",") if c.strip()]
    elif isinstance(corredores_str, list):
        lista_corredores = corredores_str
    else:
        lista_corredores = []

    # 3. Calculamos las posiciones finales definitivas en meta
    pos_inicial = n_leidos - n_corredores + 1
    if pos_inicial < 1: 
        pos_inicial = 1
    
    # 4. Asignamos el puesto oficial a cada atleta
    llegadas_numeradas = []
    for idx, nombre in enumerate(lista_corredores, start=pos_inicial):
        llegadas_numeradas.append(f"{idx}. {nombre}")
        
    llegadas_str = ", ".join(llegadas_numeradas)
    
    # 5. Formateamos el bloque épico de resultados para el LLM
    info = (
        f"[¡LLEGADA OFICIAL A META - FINAL 5000m!]\n"
        f"- Entrada en meta: Grupo {grupo}\n"
        f"- Número de atletas en este cruce: {n_corredores}\n"
        f"- Marca de cabeza (tiempo oficial de la líder del grupo): {round(t_cabeza, 2)}s\n"
        f"- Marca de cola (cierre del último puesto del grupo): {round(t_cola, 2)}s\n"
        f"- Clasificación final oficial de este bloque: {llegadas_str}"
    )
    
    return {"contexto_herramienta": info}


# =====================================================================
# 5. NODO 4: CEREBRO IA PARA COACH
# =====================================================================

def nodo_generar_cronica(state: NarradorState):
    contexto_tool = state.get("contexto_herramienta", "")
    
    # 1. Ampliamos la memoria a los últimos 6 sucesos para detectar cambios de grupo
    comentarios_previos = state.get("historial_narrativa", [])
    historial = "\n\n".join(comentarios_previos[-6:])
    
    # 2. Diseñamos un System Prompt de grado profesional con foco en deltas (cambios)
    sys_msg = SystemMessage(content=f"""Eres la voz principal de la retransmisión televisiva global de la final olímpica de 5000m femeninos.
Tu objetivo es narrar el suceso actual con un tono épico, técnico, vibrante y riguroso (máximo 3 o 4 líneas).

=== REQUISITO CRÍTICO DE EVALUACIÓN ===
Debes diferenciar visiblemente las fuentes de información en tu crónica integrando estos marcadores de forma natural:
- Usa "(Señal en directo)" cuando des los tiempos de paso oficiales, posiciones actuales o distancias.
- Usa "(Precedente RAG)" cuando justifiques el peligro de un corte o analices a una atleta basándote en el archivo histórico.

=== MEMORIA RECIENTE DE LA RETRANSMISIÓN ===
Revisa los últimos compases para dar continuidad y NO repetir frases. 
{historial if historial else "Inicio de la retransmisión. Las atletas toman la salida."}

=== PARTE TÉCNICO EN PISTA (SUCESO ACTUAL) ===
{contexto_tool}

=== INSTRUCCIONES DE ANÁLISIS TÁCTICO (EL TOQUE PROFESIONAL) ===
1. COMPARA la lista de atletas y el número de grupo actual con la MEMORIA RECIENTE. 
2. Si detectas que una atleta que antes iba en el Grupo 1 ahora aparece en el Grupo 2 (o posterior), NÁRRALO con asombro dramático destacando que "pierde contacto con la cabeza" o "empieza a ceder terreno".
3. Si el grupo se mantiene intacto en un paso oficial, elogia la consistencia y el ritmo impuesto por la líder.
4. Si es la llegada a meta, cambia a un tono triunfal y de veredicto definitivo para anunciar las medallas.

Redacta directamente la crónica final sin saludos ni introducciones.
""")
    
    # 3. Invocamos al modelo
    respuesta = llm.invoke([sys_msg]).content
    
    # 4. Actualizamos el estado de forma segura
    lote_actual = state.get("comentarios_lote", []).copy()
    lote_actual.append(respuesta)
    
    memoria_global = state.get("historial_narrativa", []).copy()
    memoria_global.append(respuesta)
    
    return {
        "comentarios_lote": lote_actual,
        "historial_narrativa": memoria_global
    }

def comprobar_cola(state: NarradorState) -> str:

    if len(state.get("eventos_pendientes", [])) > 0:
        return "despachar"
    return END

# =====================================================================
# 5. ORQUESTACIÓN DEL FLUJO LANGGRAPH COACH
# =====================================================================
builder = StateGraph(CoachState)
builder.add_node("calcular_metricas", nodo_calcular_metricas)
builder.add_node("consultar_rag", nodo_consultar_rag)
builder.add_node("generar_mensaje", nodo_generar_mensaje)
builder.add_edge(START, "calcular_metricas")
builder.add_edge("calcular_metricas", "consultar_rag")
builder.add_edge("consultar_rag", "generar_mensaje")
builder.add_edge("generar_mensaje", END)
grafo_coach = builder.compile()

builder2 = StateGraph(NarradorState)
builder2.add_node("despachar", nodo_despachar_lote)
builder2.add_node("tool_cierre", nodo_tool_cierre_parcial)
builder2.add_node("tool_rotura", nodo_tool_rag_roturas)
builder2.add_node("tool_meta", nodo_tool_llegada_meta)
builder2.add_node("ia_narrador", nodo_generar_cronica)
builder2.add_edge(START, "despachar")
builder2.add_conditional_edges("despachar", enrutar_evento)
builder2.add_edge("tool_cierre", "ia_narrador")
builder2.add_edge("tool_rotura", "ia_narrador")
builder2.add_edge("tool_meta", "ia_narrador")
builder2.add_conditional_edges(
    "ia_narrador", 
    comprobar_cola, 
    {"despachar": "despachar", END: END}
)

# Compilación sin checkpointer (gestionamos el historial inyectándolo en caliente)
grafo_narrador = builder2.compile()




# =====================================================================
# 6. INTERFAZ DE EJECUCIÓN
# =====================================================================
def ejecutar_agente_coach(batch_datos: list[dict], tid: str = None) -> str:
    """
    Recibe las nuevas filas no procesadas desde SQLite.
    (Nota: Recibe el parámetro 'tid' por compatibilidad con app.py, 
    pero se ignora internamente al ser un flujo sin memoria).
    Devuelve la instrucción formateada lista para pintar en Streamlit.
    """
    # 1. Empaquetamos la carga útil inicial
    inputs = {"eventos_raw": batch_datos}
    
    # 2. Invocamos el grafo de forma limpia y directa
    try:
        salida = grafo_coach.invoke(inputs)
        return salida.get("comentario_final", "⚠️ Error al generar el comentario del coach.")
    except Exception as e:
        print(f"❌ Error crítico en la ejecución del agente Coach: {e}")
        return "⚠️ Error interno del sistema al procesar la telemetría."


MEMORIA_GLOBAL_NARRACION = []

def ejecutar_agente_comentarista(batch_datos: list[dict], tid: str = None) -> str:
    global MEMORIA_GLOBAL_NARRACION
    
    inputs: NarradorState = {
        "eventos_pendientes": batch_datos,
        "evento_actual": {},
        "contexto_herramienta": "",
        "historial_narrativa": MEMORIA_GLOBAL_NARRACION,
        "comentarios_lote": []
    }
    
    salida = grafo_narrador.invoke(inputs)
    
    # Actualizamos la memoria global con las nuevas narraciones producidas
    MEMORIA_GLOBAL_NARRACION = salida.get("historial_narrativa", [])[-10:]
    
    # Unimos todos los comentarios generados en este lote separados por un doble salto de línea
    return "\n\n".join(salida.get("comentarios_lote", []))

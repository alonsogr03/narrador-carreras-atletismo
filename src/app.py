import streamlit as st
import subprocess
import time
import pandas as pd
from sqlalchemy import create_engine, text
import os

# Importación de agentes
try:
    from graph_workflow import ejecutar_agente_comentarista, ejecutar_agente_coach
except ImportError:
    def ejecutar_agente_comentarista(data, tid): return f"🎙️ Comentario IA: Procesando tramo..."
    def ejecutar_agente_coach(data, tid): return f"🏃‍♂️ Coach: Analizando ritmo..."

# ==========================================
# CONFIGURACIÓN Y ESTILOS
# ==========================================
st.set_page_config(page_title="Big Data Sports - 5000m", page_icon="🏃‍♀️", layout="wide")

st.markdown("""
    <style>
    .report-card { padding: 15px; border-radius: 10px; background-color: white; 
                   box-shadow: 0 2px 4px rgba(0,0,0,0.05); margin-bottom: 10px; border-left: 5px solid #ddd; }
    .coach-msg { border-left-color: #28a745; background-color: #f1f8f1; font-size: 0.95em; }
    .narrador-msg { border-left-color: #007bff; background-color: #f0f7ff; font-size: 0.95em; }
    </style>
    """, unsafe_allow_html=True)

# ==========================================
# CONEXIONES Y ESTADO
# ==========================================
DB_PATH = "data/telemetria.db"
engine = create_engine(f"sqlite:///{DB_PATH}")

if 'race_started' not in st.session_state: st.session_state.race_started = False
if 'narrador_history' not in st.session_state: st.session_state.narrador_history = []
if 'coach_history' not in st.session_state: st.session_state.coach_history = []

# ==========================================
# SIDEBAR: CONTROL Y DESCARGAS
# ==========================================
with st.sidebar:
    st.title("🛠️ Centro de Control")
    
    if not st.session_state.race_started:
        if st.button("🚀 INICIAR CARRERA", use_container_width=True):
            # Limpieza opcional de tablas previas o marcas
            st.session_state.narrador_history = []
            st.session_state.coach_history = []
            st.session_state.race_started = True
            
            # Lanzar productor
            prod_script = "src/kafka_producer.py" if os.path.exists("src/kafka_producer.py") else "kafka_producer.py"
            subprocess.Popen(["python", prod_script])
            st.rerun()
    else:
        if st.button("⏹️ PARAR / RESETEAR", use_container_width=True):
            st.session_state.race_started = False
            st.rerun()

    st.markdown("---")
    st.subheader("📊 Generación de Informes")
    st.info("Puedes descargar el estado actual de la narración en cualquier momento.")
    
    # Los botones siempre disponibles si hay contenido
    if st.session_state.narrador_history:
        # Unimos el historial (el historial está invertido, así que lo ponemos en orden cronológico)
        txt_n = "\n\n".join(reversed(st.session_state.narrador_history))
        st.download_button("📥 Informe Narrador (.txt)", txt_n, "retransmision_5000m.txt", use_container_width=True)
    
    if st.session_state.coach_history:
        txt_c = "\n\n".join(reversed(st.session_state.coach_history))
        st.download_button("📥 Informe Coach (.txt)", txt_c, "analisis_tecnico_marta.txt", use_container_width=True)

# ==========================================
# PANTALLA PRINCIPAL
# ==========================================
st.title("🏆 Final Olímpica: 5000m Femenina")
st.write(f"**Estado del Sistema:** {'🟢 EN DIRECTO' if st.session_state.race_started else '⚪ EN ESPERA'}")

col_coach, col_narrador = st.columns(2)

with col_coach:
    st.markdown("<h3 style='color:#28a745;'>🏃‍♀️ COACH: MARTA GARCÍA</h3>", unsafe_allow_html=True)
    coach_placeholder = st.container()

with col_narrador:
    st.markdown("<h3 style='color:#007bff;'>🎙️ NARRADOR CARRERA</h3>", unsafe_allow_html=True)
    narrador_placeholder = st.container()

# ==========================================
# LÓGICA DE POLLING (SEGURO POR ROWID)
# ==========================================
if st.session_state.race_started:
    try:
        with engine.begin() as conn:
            # ---------------------------------------------------------
            # 1. PROCESAR NARRADOR
            # ---------------------------------------------------------
            # Nota: Si Spark ya inicializa la columna al crear la tabla, 
            # este ALTER fallará silenciosamente de forma controlada.
            try: 
                conn.execute(text("ALTER TABLE tabla_comentarista ADD COLUMN procesado INTEGER DEFAULT 0"))
            except: 
                pass 

            df_n = pd.read_sql("SELECT rowid, * FROM tabla_comentarista WHERE procesado = 0", conn)
            
            if not df_n.empty:
                r_ids = df_n['rowid'].tolist()
                with st.spinner('Actualizando narración...'):
                    # Enviamos todo el bloque de nuevas filas excluyendo el rowid interno
                    batch = df_n.drop(columns=['rowid']).to_dict(orient='records')
                    res = ejecutar_agente_comentarista(batch, tid="hilo_narrador")
                    st.session_state.narrador_history.insert(0, res)
                
                # ¡CORREGIDO!: Actualizamos usando exactamente la columna 'procesado'
                conn.execute(text(f"UPDATE tabla_comentarista SET procesado = 1 WHERE rowid IN ({','.join(map(str, r_ids))})"))
                
                # Efecto visual si el grupo de cabeza o algún corredor llega a meta
                if any("Meta 5000m" in str(x) for x in df_n['tipo_evento']):
                    st.toast("¡Alguien ha cruzado la meta!", icon="🏁")

            # ---------------------------------------------------------
            # 2. PROCESAR COACH
            # ---------------------------------------------------------
            try: 
                conn.execute(text("ALTER TABLE tabla_coach ADD COLUMN procesado INTEGER DEFAULT 0"))
            except: 
                pass

            df_c = pd.read_sql("SELECT rowid, * FROM tabla_coach WHERE procesado = 0", conn)
            
            if not df_c.empty:
                r_ids_c = df_c['rowid'].tolist()
                with st.spinner('Coach evaluando parciales...'):
                    batch_c = df_c.drop(columns=['rowid']).to_dict(orient='records')
                    res_c = ejecutar_agente_coach(batch_c, tid="hilo_coach")
                    st.session_state.coach_history.insert(0, res_c)
                
                # ¡CORREGIDO!: Mantenemos coherencia en la tabla del coach
                conn.execute(text(f"UPDATE tabla_coach SET procesado = 1 WHERE rowid IN ({','.join(map(str, r_ids_c))})"))

    except Exception as e:
        # ¡CORREGIDO!: Registramos en la terminal si hay bloqueos (database is locked)
        print(f"⚠️ Aviso en el polling de DB: {e}") 
    
    # Pausa estratégica para dar respiro a SQLite y a los agentes antes del siguiente ciclo
    time.sleep(3)
    st.rerun()

# ==========================================
# RENDERIZADO DE HISTORIAL
# ==========================================
with coach_placeholder:
    for msg in st.session_state.coach_history:
        st.markdown(f'<div class="report-card coach-msg">{msg}</div>', unsafe_allow_html=True)

with narrador_placeholder:
    for msg in st.session_state.narrador_history:
        st.markdown(f'<div class="report-card narrador-msg">{msg}</div>', unsafe_allow_html=True)
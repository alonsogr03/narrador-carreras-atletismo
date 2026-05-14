import streamlit as st
import subprocess
import time
import pandas as pd
from sqlalchemy import create_engine, text, inspect
import os

# Importación de agentes
try:
    from graph_workflow import ejecutar_agente_comentarista, ejecutar_agente_coach
except ImportError:
    def ejecutar_agente_comentarista(data, tid): return "🎙️ Error: No se pudo cargar el agente."
    def ejecutar_agente_coach(data, tid): return "🏃‍♂️ Error: No se pudo cargar el agente."

# ==========================================
# CONFIGURACIÓN Y ESTILOS (FIX: TEXTO BLANCO)
# ==========================================
st.set_page_config(page_title="Big Data Sports - 5000m", page_icon="🏃‍♀️", layout="wide")

st.markdown("""
    <style>
    .report-card { 
        padding: 15px; 
        border-radius: 10px; 
        background-color: #ffffff; 
        box-shadow: 0 4px 6px rgba(0,0,0,0.1); 
        margin-bottom: 12px; 
        border-left: 6px solid #ddd;
        color: #1e1e1e; /* Texto oscuro para contraste */
    }
    .coach-msg { border-left-color: #28a745; background-color: #f8fff8; }
    .narrador-msg { border-left-color: #007bff; background-color: #f0f7ff; }
    .pos-tag { font-weight: bold; color: #555; }
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
# SIDEBAR
# ==========================================
with st.sidebar:
    st.title("🛠️ Centro de Control")
    
    if not st.session_state.race_started:
        if st.button("🚀 INICIAR CARRERA", use_container_width=True):
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
    st.subheader("📊 Descargas Live")
    
    if st.session_state.narrador_history:
        txt_n = "\n\n".join(st.session_state.narrador_history) # Ya vienen en orden de inserción
        st.download_button("📥 Narración Directo", txt_n, "narracion.txt", use_container_width=True)
    
    if st.session_state.coach_history:
        txt_c = "\n\n".join(st.session_state.coach_history)
        st.download_button("📥 Instrucciones Coach", txt_c, "coach.txt", use_container_width=True)

# ==========================================
# PANTALLA PRINCIPAL
# ==========================================
st.title("🏆 Final Olímpica: 5000m Femenina")
st.write(f"**Estado:** {'🟢 EN DIRECTO' if st.session_state.race_started else '⚪ EN ESPERA'}")

col_coach, col_narrador = st.columns(2)

with col_coach:
    st.markdown("<h3 style='color:#28a745;'>🏃‍♀️ COACH: MARTA GARCÍA</h3>", unsafe_allow_html=True)
    coach_container = st.empty()

with col_narrador:
    st.markdown("<h3 style='color:#007bff;'>🎙️ NARRADOR CARRERA</h3>", unsafe_allow_html=True)
    narrador_container = st.empty()

# ==========================================
# LÓGICA DE PROCESAMIENTO (POLLING)
# ==========================================
if st.session_state.race_started:
    try:
        # Usamos inspect para no lanzar errores si la tabla no existe aún
        inspector = inspect(engine)
        
        with engine.begin() as conn:
            
            # --- 1. PROCESAR COACH (Marta) ---
            if inspector.has_table("tabla_coach"):
                # Asegurar columna procesado
                try: conn.execute(text("ALTER TABLE tabla_coach ADD COLUMN procesado INTEGER DEFAULT 0"))
                except: pass

                df_c = pd.read_sql("SELECT rowid, * FROM tabla_coach WHERE procesado = 0 ORDER BY distancia_actual_m ASC", conn)
                
                if not df_c.empty:
                    for _, row in df_c.iterrows():
                        # Procesamos UNA A UNA para no saltarnos ningún parcial
                        res_c = ejecutar_agente_coach([row.to_dict()])
                        st.session_state.coach_history.insert(0, res_c)
                        # Marcar esta fila específica como procesada
                        conn.execute(text(f"UPDATE tabla_coach SET procesado = 1 WHERE rowid = {row['rowid']}"))

            # --- 2. PROCESAR NARRADOR (General) ---
            if inspector.has_table("tabla_comentarista"):
                try: conn.execute(text("ALTER TABLE tabla_comentarista ADD COLUMN procesado INTEGER DEFAULT 0"))
                except: pass

                df_n = pd.read_sql("SELECT rowid, * FROM tabla_comentarista WHERE procesado = 0", conn)
                
                if not df_n.empty:
                    r_ids = df_n['rowid'].tolist()
                    # El narrador ya tiene un bucle interno en su agente, le pasamos el batch
                    batch = df_n.drop(columns=['rowid']).to_dict(orient='records')
                    res_n = ejecutar_agente_comentarista(batch, tid="hilo_narrador")
                    
                    # El agente narrador puede devolver varios párrafos unidos por \n\n
                    st.session_state.narrador_history.insert(0, res_n)
                    
                    conn.execute(text(f"UPDATE tabla_comentarista SET procesado = 1 WHERE rowid IN ({','.join(map(str, r_ids))})"))

    except Exception as e:
        # Solo imprimimos errores reales, no los de "database locked" que son normales
        if "locked" not in str(e).lower():
            st.error(f"Error en base de datos: {e}")

    # --- Renderizado en los contenedores ---
    with coach_container.container():
        for msg in st.session_state.coach_history:
            st.markdown(f'<div class="report-card coach-msg">{msg}</div>', unsafe_allow_html=True)

    with narrador_container.container():
        for msg in st.session_state.narrador_history:
            st.markdown(f'<div class="report-card narrador-msg">{msg}</div>', unsafe_allow_html=True)

    # Pausa de 3 segundos y refresco
    time.sleep(3)
    st.rerun()
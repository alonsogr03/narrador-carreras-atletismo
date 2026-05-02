import csv
import json
import time
from kafka import KafkaProducer

# TODO: Configurar el KafkaProducer apuntando a localhost:9092
# TODO: Abrir el CSV de data/static/
# TODO: Bucle for para leer cada fila. 
# TODO: Calcular el tiempo de espera (sleep) restando el timestamp actual del anterior para simular directo.
# TODO: producer.send() al topic 'race_events'.
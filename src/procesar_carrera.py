import json
import csv

def tiempo_a_segundos(tiempo_str):
    """Convierte un string de tiempo (1:18.37 o 18.36) a segundos totales."""
    if not tiempo_str:
        return 0.0
    
    partes = tiempo_str.split(':')
    if len(partes) == 2:  # Formato M:SS.mm
        minutos = float(partes[0])
        segundos = float(partes[1])
        return round(minutos * 60 + segundos, 2)
    else:  # Formato SS.mm
        return round(float(partes[0]), 2)

def procesar_datos_carrera(input_file, output_file):
    # Cargar el JSON
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Preparar el archivo CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        escritor = csv.writer(f)
        # Escribir cabecera
        escritor.writerow(['id_evento', 'nombre_corredor', 'distancia_metros', 'tiempo_acumulado_segundos'])

        id_evento = 1
        
        # Iterar sobre cada atleta
        for atleta in data:
            nombre = atleta['nombre']
            
            # Iterar sobre cada split (parcial)
            for split in atleta['splits']:
                # Extraer distancia (quitando la 'm' y convirtiendo a int)
                distancia = int(split['distancia'].replace('m', ''))
                
                # Convertir tiempo acumulado a segundos
                tiempo_seg = tiempo_a_segundos(split['tiempo_acumulado'])
                
                # Escribir fila
                escritor.writerow([id_evento, nombre, distancia, tiempo_seg])
                
                # Incrementamos el ID para que cada medición sea un "evento" único en el tiempo
                id_evento += 1

    print(f"Proceso finalizado. Archivo guardado como: {output_file}")

# Ejecutar el script
if __name__ == "__main__":
    # Asegúrate de que el archivo JSON se llame 'w5000_f_clean.json' 
    # o cambia el nombre aquí abajo.
    procesar_datos_carrera('data/static/w5000_f_clean.json', 'simulacion_carrera.csv')
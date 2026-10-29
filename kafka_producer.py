import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer


def generar_datos_sensor():
    return {
        "sensor_id": random.randint(1, 10),
        "temperature": round(random.uniform(20, 35), 2),
        "humidity": round(random.uniform(30, 80), 2),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }


producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda dato: json.dumps(dato).encode("utf-8")
)

print("Productor iniciado. Presiona Ctrl + C para finalizar.")

try:
    while True:
        dato_sensor = generar_datos_sensor()

        producer.send("sensor_data", value=dato_sensor)
        producer.flush()

        print(f"Enviado: {dato_sensor}")
        time.sleep(1)

except KeyboardInterrupt:
    print("\nProductor detenido.")

finally:
    producer.close()

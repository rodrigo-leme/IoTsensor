import json
import time
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

KAFKA_BROKER = 'localhost:9092'  
KAFKA_TOPIC = 'iot_sensor_data' 

fake = Faker('pt_BR') 

def generate_sensor_data():
    """dicionário com dados falsos - sensor IoT."""
    return {
        "sensor_id": fake.uuid4(),  
        "timestamp": datetime.now().isoformat(), 

        "temperature": fake.pyfloat(min_value=20.0, max_value=35.0, right_digits=2),
        "humidity": fake.pyfloat(min_value=40.0, max_value=90.0, right_digits=2),

        "location": fake.city() + ", " + fake.country(), 
        "battery_level": fake.pyfloat(min_value=0.1, max_value=100.0, right_digits=2) 
    }

def produce_messages():
    producer = None
    try:
        
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"Produtor Kafka conectado a {KAFKA_BROKER}")
        print(f"Enviando dados para o tópico '{KAFKA_TOPIC}'...")

        while True:
            sensor_data = generate_sensor_data()
            producer.send(KAFKA_TOPIC, value=sensor_data)
            print(f"Mensagem enviada: {sensor_data}")
            time.sleep(1) 

    except Exception as e:
        print(f"Erro ao produzir mensagens: {e}")
    finally:
        if producer:
            producer.close() 
            print("Produtor Kafka encerrado.")

if __name__ == "__main__":
    produce_messages()
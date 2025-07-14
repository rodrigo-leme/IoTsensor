import json
import sqlite3
from kafka import KafkaConsumer

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'iot_sensor_data'
KAFKA_CONSUMER_GROUP = 'iot_data_processor_group' 

DB_FILE = 'iot_data.db'

def setup_database():

    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                temperature REAL,
                humidity REAL,
                location TEXT,
                battery_level REAL
            )
        ''')
        conn.commit()
        print(f"Banco de dados '{DB_FILE}' configurado. Tabela 'sensor_readings' pronta.")
    except sqlite3.Error as e:
        print(f"Erro ao configurar o banco de dados: {e}")
        if conn:
            conn.close()
        conn = None 
    return conn

def consume_messages():
    db_conn = setup_database()
    if not db_conn:
        return

    consumer = None
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            group_id=KAFKA_CONSUMER_GROUP,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest', 
            enable_auto_commit=True, 
            auto_commit_interval_ms=1000 
        )
        print(f"Consumidor Kafka conectado a {KAFKA_BROKER}, tópico '{KAFKA_TOPIC}' no grupo '{KAFKA_CONSUMER_GROUP}'")
        print("Aguardando mensagens...")

        cursor = db_conn.cursor()
        for message in consumer:
            sensor_data = message.value 
            print(f"Mensagem recebida: {sensor_data} (offset: {message.offset})")

            try:
                cursor.execute('''
                    INSERT INTO sensor_readings (sensor_id, timestamp, temperature, humidity, location, battery_level)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    sensor_data.get('sensor_id'),
                    sensor_data.get('timestamp'),
                    sensor_data.get('temperature'),
                    sensor_data.get('humidity'),
                    sensor_data.get('location'),
                    sensor_data.get('battery_level')
                ))
                db_conn.commit()
                print("Dados inseridos no banco de dados com sucesso!!")
            except sqlite3.Error as e:
                print(f"Erro ao inserir dados no banco de dados: {e}")

    except Exception as e:
        print(f"Erro ao consumir mensagens: {e}")
    finally:
        if consumer:
            consumer.close()
            print("Consumidor Kafka encerrado.")
        if db_conn:
            db_conn.close()
            print("Conexão com o banco de dados encerrada.")

if __name__ == "__main__":
    consume_messages()
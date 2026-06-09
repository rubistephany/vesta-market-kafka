import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer

# Configuración obligatoria del Productor (Fiabilidad Financiera)
config = {
    'bootstrap.servers': 'kafka-kraft:29092', # Usamos el nombre del contenedor de la red de Docker
    'acks': 'all',          
    'retries': 5,           
    'retry.backoff.ms': 500
}

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Error al entregar mensaje: {err}")
    else:
        tienda = msg.key().decode('utf-8')
        print(f"✅ Ticket enviado -> Tienda: {tienda} | Tópico: {msg.topic()} [Partición: {msg.partition()}] @ Offset: {msg.offset()}")

try:
    producer = Producer(config)
except Exception as e:
    print(f"❌ Error al conectar con el servidor de Kafka: {e}")
    exit(1)

tiendas = ['TIENDA_MADRID_01', 'TIENDA_BARCELONA_02', 'TIENDA_SEVILLA_03']
id_ticket = 1000

print("🚀 TPV de VestaMarket S.A. Iniciado... Enviando transacciones (Ctrl+C para detener)")

try:
    while True:
        tienda_actual = random.choice(tiendas)
        ticket_data = {
            "id_ticket": f"TKT-{id_ticket}",
            "id_tienda": tienda_actual,
            "importe_total": round(random.uniform(5.99, 450.50), 2),
            "timestamp_creacion": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        mensaje_json = json.dumps(ticket_data)
        
        producer.produce(
            topic='ventas-tpv',
            key=tienda_actual.encode('utf-8'),
            value=mensaje_json.encode('utf-8'),
            callback=delivery_report
        )
        
        producer.poll(0)
        id_ticket += 1
        time.sleep(2)

except KeyboardInterrupt:
    print("\n🛑 Deteniendo simulación de TPVs...")
finally:
    print("⏳ Vaciando buffer de mensajes pendientes en Kafka...")
    producer.flush()
    print("🔌 Conexión cerrada de manera segura.")

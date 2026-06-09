import json
import sys
from confluent_kafka import Consumer, KafkaError

# Configuración obligatoria del Consumidor
config = {
    'bootstrap.servers': 'kafka-kraft:29092', # Conexión interna de la red Docker
    'group.id': 'grupo-control-stock',        # ID de grupo para balanceo de carga
    'auto.offset.reset': 'earliest',          # Lee desde el principio si es un grupo nuevo
    'enable.auto.commit': True                # Confirmación automática de lectura
}

# Inicializar el consumidor
try:
    consumer = Consumer(config)
    # Nos suscribimos al tópico de las cajas registradoras
    consumer.subscribe(['ventas-tpv'])
except Exception as e:
    print(f"❌ Error al configurar el consumidor: {e}")
    sys.exit(1)

print("📦 Consumidor de Control de Stock Inicializado... Esperando ventas (Ctrl+C para salir)")

try:
    while True:
        # Esperar un mensaje de Kafka (tiempo de espera de 1 segundo)
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
            
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Llegó al final de la partición (normal)
                continue
            else:
                print(f"❌ Error en Kafka: {msg.error()}")
                break

        # 1. Recuperar los datos crudos del mensaje
        llave_tienda = msg.key().decode('utf-8')
        valor_json = msg.value().decode('utf-8')
        
        # 2. Parsear el JSON inmutable
        ticket = json.loads(valor_json)
        
        # 3. Procesar la lógica de negocio (Simulación de actualización de Stock)
        print(f"🛍️  [PROCESANDO] Tienda: {llave_tienda} | Ticket: {ticket['id_ticket']} | Total: {ticket['importe_total']}€")
        print(f"   📉 Stock Actualizado -> Mensaje leído de la [Partición: {msg.partition()}] @ Offset: {msg.offset()}")
        print("-" * 80)

except KeyboardInterrupt:
    print("\n🛑 Deteniendo el monitor de inventario...")
finally:
    # Cerrar la conexión limpiamente y liberar el consumidor del grupo
    consumer.close()
    print("🔌 Consumidor desconectado de forma segura.")

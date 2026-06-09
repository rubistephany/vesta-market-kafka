import json
import sys
from confluent_kafka import Consumer, KafkaError

# Configuración del Consumidor Financiero
config = {
    'bootstrap.servers': 'kafka-kraft:29092',
    'group.id': 'grupo-analiticas-financieras',  # ¡Un grupo NUEVO para que no choque con el de Stock!
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

try:
    consumer = Consumer(config)
    consumer.subscribe(['ventas-tpv'])
except Exception as e:
    print(f"❌ Error al configurar el consumidor financiero: {e}")
    sys.exit(1)

# Variables locales para llevar la contabilidad en tiempo real
total_sistema = 0.0
ventas_por_tienda = {}

print("📊 Monitor de Finanzas de VestaMarket S.A. Inicializado... (Ctrl+C para salir)\n")

try:
    while True:
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"❌ Error: {msg.error()}")
                break

        # Parsear los datos del ticket
        llave_tienda = msg.key().decode('utf-8')
        ticket = json.loads(msg.value().decode('utf-8'))
        importe = float(ticket['importe_total'])
        
        # Lógica Financiera: Acumular importes
        total_sistema += importe
        ventas_por_tienda[llave_tienda] = ventas_por_tienda.get(llave_tienda, 0.0) + importe
        
        # Mostrar el panel financiero en tiempo real
        print(f"💰 [NUEVA VENTA] {llave_tienda} facturó {importe}€ (Ticket: {ticket['id_ticket']})")
        print(f"📈 RECAUDACIÓN TOTAL VESTAMARKET: {round(total_sistema, 2)}€")
        print("   Detalle por sucursal:")
        for tienda, total in ventas_por_tienda.items():
            print(f"     📍 {tienda}: {round(total, 2)}€")
        print("-" * 60)

except KeyboardInterrupt:
    print("\n🛑 Cerrando panel financiero...")
finally:
    consumer.close()
    print("🔌 Sistema financiero desconectado.")

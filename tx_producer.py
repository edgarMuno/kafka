from confluent_kafka import Producer
import sys, time

BOOTSTRAP = "localhost:9094"
TOPIC = "ventas_in"
TX_ID = "tx-producer-001"  # único y estable por instancia

p = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "enable.idempotence": True,
    "acks": "all",
    "transactional.id": TX_ID,
    # Ajustes opcionales
    "linger.ms": 5,
    "batch.size": 64_000,
})

def delivery(err, msg):
    if err:
        print(f"❌ Error delivery: {err}")
    else:
        print(f"✅ Enviado a {msg.topic()}[{msg.partition()}]@{msg.offset()} key={msg.key()}")

print("Inicializando transacciones…")
p.init_transactions()

try:
    print("Comenzando transacción…")
    p.begin_transaction()

    for i in range(1, 11):
        key = f"cliente{i%3}".encode()
        val = f"pedido #{i}".encode()
        p.produce(TOPIC, key=key, value=val, on_delivery=delivery)
    p.flush()

    # Simula validar negocio
    time.sleep(0.2)

    print("Haciendo commit de la transacción…")
    p.commit_transaction()
    print("✅ Commit OK")
except Exception as e:
    print("⚠️ Error, abortando transacción:", e)
    p.abort_transaction()
    print("🧹 Abort OK")

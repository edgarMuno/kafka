from confluent_kafka import Producer, Consumer, TopicPartition
from confluent_kafka.error import KafkaException
import sys, time

BOOTSTRAP = "localhost:9094"
SRC_TOPIC = "ventas_in"
DST_TOPIC = "ventas_out"
GROUP_ID = "g_tx_pipeline"
TX_ID = "tx-pipeline-001"  # único y estable por instancia

# Consumer: lee solo mensajes confirmados (read_committed)
c = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": GROUP_ID,
    "enable.auto.commit": False,          # nosotros controlamos el commit
    "auto.offset.reset": "earliest",
    "isolation.level": "read_committed",  # ignora mensajes abortados
})

# Producer transaccional
p = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "enable.idempotence": True,
    "acks": "all",
    "transactional.id": TX_ID,
})

def process(record):
    """Tu lógica de negocio: transforma el valor de entrada."""
    key = record.key()
    val_in = (record.value() or b"").decode("utf-8")
    val_out = f"{val_in} -> procesado_ok"
    return key, val_out.encode()

print("Suscribiendo al tópico de entrada…")
c.subscribe([SRC_TOPIC])

print("Inicializando transacciones del productor…")
p.init_transactions()

try:
    while True:
        # Junta un mini-batch
        batch = []
        while len(batch) < 50:
            msg = c.poll(0.2)
            if msg is None:
                break
            if msg.error():
                raise KafkaException(msg.error())
            batch.append(msg)

        if not batch:
            # No hay mensajes por ahora
            continue

        # Comienza la transacción
        p.begin_transaction()

        # Produce mensajes transformados
        for m in batch:
            key, out = process(m)
            p.produce(DST_TOPIC, key=key, value=out)

        p.flush()

        # Calcula offsets “siguientes” por partición (offset + 1)
        # Usamos position() sobre la asignación actual para obtener el next-offset por partición
        assignment = c.assignment()
        next_offsets = c.position(assignment)  # lista[TopicPartition]

        # Ata offsets del consumidor a la transacción del productor
        # (si la escritura falla y abortamos, estos offsets NO se confirman)
        p.send_offsets_to_transaction(next_offsets, c.consumer_group_metadata())

        # Commit atómico: salen visibles los mensajes destino y se confirman offsets de origen
        p.commit_transaction()

        print(f"✅ Batch {len(batch)} procesado y confirmado")
except KeyboardInterrupt:
    print("Detenido por usuario.")
except Exception as e:
    print("⚠️ Error en pipeline, abortando transacción:", e)
    try:
        p.abort_transaction()
        print("🧹 Abort OK")
    except Exception as e2:
        print("Abort falló:", e2)
finally:
    c.close()

from pathlib import Path
import random
import time

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "orders"
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "order.avsc"

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]
INVALID_ORDER_PROBABILITY = 0.15
SEND_INTERVAL_SECONDS = 1.5


def order_to_dict(order: dict, ctx: SerializationContext) -> dict:
    return order


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"[DELIVERY FAILED] Error={err}")
    else:
        key_str = msg.key().decode("utf-8") if msg.key() else None
        print(
            f"[DELIVERED] OrderId={key_str} | Topic={msg.topic()} | "
            f"Partition={msg.partition()} | Offset={msg.offset()}"
        )


def load_schema(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Avro schema not found at: {path}")
    return path.read_text(encoding="utf-8")


def main() -> None:
    schema_str = load_schema(SCHEMA_PATH)

    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=schema_str,
        to_dict=order_to_dict,
    )
    key_serializer = StringSerializer("utf_8")

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    order_counter = 1001
    print(f"Starting Avro Order Producer -> Topic: '{TOPIC}' (Press Ctrl+C to stop)\n")

    try:
        while True:
            order_id = str(order_counter)
            product = random.choice(PRODUCTS)

            if random.random() < INVALID_ORDER_PROBABILITY:
                price = float(random.choice([-50.0, -25.0, 0.0]))
                label = "[INVALID - DLQ TEST]"
            else:
                price = round(random.uniform(10.0, 500.0), 2)
                label = "[VALID]"

            order = {
                "orderId": order_id,
                "product": product,
                "price": price,
            }

            print(f"[SENDING] {label} {order}")

            producer.produce(
                topic=TOPIC,
                key=key_serializer(
                    order_id, SerializationContext(TOPIC, MessageField.KEY)
                ),
                value=avro_serializer(
                    order, SerializationContext(TOPIC, MessageField.VALUE)
                ),
                on_delivery=delivery_report,
            )

            producer.poll(0)
            order_counter += 1
            time.sleep(SEND_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nShutting down producer gracefully...")
    finally:
        remaining = producer.flush(timeout=10)
        if remaining > 0:
            print(f"[WARNING] {remaining} message(s) were not delivered before shutdown.")
        else:
            print("All pending messages flushed. Producer stopped cleanly.")


if __name__ == "__main__":
    main()
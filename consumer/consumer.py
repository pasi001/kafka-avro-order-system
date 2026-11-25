import base64
import json
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringDeserializer,
)

# Enable ANSI escape colors in Windows PowerShell
if os.name == "nt":
    os.system("")

# ANSI Color Codes
RESET = "\033[0m"
BOLD = "\033[1m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
CYAN = "\033[96m"
MAGENTA = "\033[95m"

# Configuration
BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
SOURCE_TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"
GROUP_ID = "order-processor-group"

TRANSIENT_FAILURE_RATE = 0.20
RETRY_BACKOFFS = [0.5, 1.0, 2.0]  # 3 retries with exponential backoff

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "order.avsc"


class TransientProcessingError(Exception):
    """Simulated temporary processing or network failure."""


def load_avro_schema(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Avro schema not found at: {path}")
    return path.read_text(encoding="utf-8")


def send_to_dlq(
    dlq_producer: Producer,
    msg,
    original_key: str | None,
    original_payload,
    error_type: str,
    reason: str,
    retry_count: int,
) -> None:
    dlq_record = {
        "original_key": original_key,
        "original_payload": original_payload,
        "error_type": error_type,
        "reason": reason,
        "retry_count": retry_count,
        "source_topic": msg.topic(),
        "source_partition": msg.partition(),
        "source_offset": msg.offset(),
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }

    delivery_error = None

    def on_delivery(err, _msg):
        nonlocal delivery_error
        if err is not None:
            delivery_error = err

    dlq_producer.produce(
        topic=DLQ_TOPIC,
        key=original_key.encode("utf-8") if original_key else None,
        value=json.dumps(dlq_record).encode("utf-8"),
        on_delivery=on_delivery,
    )
    remaining = dlq_producer.flush(timeout=5.0)
    if remaining > 0 or delivery_error is not None:
        raise RuntimeError(f"Failed to deliver message to DLQ: {delivery_error}")


def simulate_transient_processing(order: dict) -> None:
    if random.random() < TRANSIENT_FAILURE_RATE:
        raise TransientProcessingError("Simulated transient downstream/network timeout")


def process_with_retries(order: dict) -> tuple[bool, int, str]:
    """Attempt processing with up to 3 retries and exponential backoff."""
    order_id = order.get("orderId", "UNKNOWN")

    # Initial attempt
    try:
        simulate_transient_processing(order)
        return True, 0, ""
    except TransientProcessingError as exc:
        last_error = str(exc)
        print(
            f"{YELLOW}[TRANSIENT] Order {order_id} failed on initial attempt "
            f"({last_error}){RESET}"
        )

    # Retries with exponential backoff
    for retry_num, delay in enumerate(RETRY_BACKOFFS, start=1):
        print(
            f"{YELLOW}[RETRY {retry_num}/{len(RETRY_BACKOFFS)}] "
            f"Order {order_id} - waiting {delay:.1f}s before retry...{RESET}"
        )
        time.sleep(delay)
        try:
            simulate_transient_processing(order)
            return True, retry_num, ""
        except TransientProcessingError as exc:
            last_error = str(exc)
            print(
                f"{YELLOW}[RETRY {retry_num}/{len(RETRY_BACKOFFS)} FAILED] "
                f"Order {order_id}: {last_error}{RESET}"
            )

    return False, len(RETRY_BACKOFFS), last_error


def print_summary(
    valid_count: int,
    running_total: float,
    recovered_count: int,
    dlq_count: int,
) -> None:
    running_avg = (running_total / valid_count) if valid_count > 0 else 0.0
    print(f"\n{CYAN}{BOLD}================ CONSUMER SESSION SUMMARY ================{RESET}")
    print(f"  Valid Orders Processed   : {GREEN}{valid_count}{RESET}")
    print(f"  Running Total Price      : {GREEN}${running_total:,.2f}{RESET}")
    print(f"  Final Running Average    : {GREEN}${running_avg:,.2f}{RESET}")
    print(f"  Retried & Recovered      : {YELLOW}{recovered_count}{RESET}")
    print(f"  Routed to DLQ            : {RED}{dlq_count}{RESET}")
    print(f"{CYAN}{BOLD}=========================================================={RESET}\n")


def main() -> None:
    schema_str = load_avro_schema(SCHEMA_PATH)
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)
    key_deserializer = StringDeserializer("utf_8")

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    dlq_producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    consumer.subscribe([SOURCE_TOPIC])

    valid_count = 0
    running_total = 0.0
    recovered_count = 0
    dlq_count = 0

    print(f"{CYAN}{BOLD}[START] Order Consumer started... Listening on '{SOURCE_TOPIC}'{RESET}")
    print(f"{CYAN}Press Ctrl+C to stop gracefully.{RESET}\n")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"{RED}[KAFKA ERROR] {msg.error()}{RESET}")
                continue

            # Decode message key safely
            try:
                order_key = key_deserializer(msg.key())
            except Exception:
                order_key = None

            # 1. Avro Deserialization (Poison Pill Detection -> Permanent Failure)
            try:
                ctx = SerializationContext(msg.topic(), MessageField.VALUE)
                order = avro_deserializer(msg.value(), ctx)
            except Exception as exc:
                raw_payload = (
                    base64.b64encode(msg.value()).decode("ascii")
                    if msg.value() is not None
                    else None
                )
                reason = f"Avro deserialization failed (poison pill): {exc}"
                send_to_dlq(
                    dlq_producer=dlq_producer,
                    msg=msg,
                    original_key=order_key,
                    original_payload={"raw_base64": raw_payload},
                    error_type="DESERIALIZATION_ERROR",
                    reason=reason,
                    retry_count=0,
                )
                dlq_count += 1
                print(
                    f"{RED}[DLQ - POISON PILL] Partition={msg.partition()} "
                    f"Offset={msg.offset()} | {reason}{RESET}"
                )
                consumer.commit(message=msg, asynchronous=False)
                continue

            # 2. Validation Check (Invalid Order -> Permanent Failure, no retries)
            price = order.get("price") if isinstance(order, dict) else None
            if not isinstance(order, dict) or price is None or float(price) <= 0.0:
                order_id = order.get("orderId", "UNKNOWN") if isinstance(order, dict) else "UNKNOWN"
                reason = f"Invalid order price: {price} (must be greater than 0)"
                send_to_dlq(
                    dlq_producer=dlq_producer,
                    msg=msg,
                    original_key=order_key,
                    original_payload=order,
                    error_type="VALIDATION_ERROR",
                    reason=reason,
                    retry_count=0,
                )
                dlq_count += 1
                print(
                    f"{MAGENTA}[DLQ - INVALID] Order={order_id} | {reason} "
                    f"| Routed immediately (0 retries){RESET}"
                )
                consumer.commit(message=msg, asynchronous=False)
                continue

            # 3. Transient Processing & Retry Logic
            succeeded, retry_count, last_error = process_with_retries(order)
            if not succeeded:
                send_to_dlq(
                    dlq_producer=dlq_producer,
                    msg=msg,
                    original_key=order_key,
                    original_payload=order,
                    error_type="TRANSIENT_RETRY_EXHAUSTED",
                    reason=f"Failed after {retry_count} retries: {last_error}",
                    retry_count=retry_count,
                )
                dlq_count += 1
                print(
                    f"{RED}[DLQ - RETRIES EXHAUSTED] Order={order['orderId']} "
                    f"Product={order['product']} Price=${float(price):.2f} "
                    f"| Sent to '{DLQ_TOPIC}'{RESET}"
                )
                consumer.commit(message=msg, asynchronous=False)
                continue

            # 4. Real-time Running Average Aggregation
            valid_count += 1
            running_total += float(price)
            running_avg = running_total / valid_count

            if retry_count > 0:
                recovered_count += 1
                status_tag = f"{CYAN}[RECOVERED after {retry_count} retry]{RESET}"
            else:
                status_tag = f"{GREEN}[OK]{RESET}"

            print(
                f"{status_tag} Order={order['orderId']} | Product={order['product']:<12} | "
                f"Price=${float(price):>6.2f} | "
                f"{BOLD}Valid={valid_count} | Total=${running_total:,.2f} | "
                f"Running Avg=${running_avg:,.2f}{RESET}"
            )

            # Manual offset commit after successful processing
            consumer.commit(message=msg, asynchronous=False)

    except KeyboardInterrupt:
        print(f"\n{YELLOW}[SHUTDOWN] Ctrl+C detected. Flushing DLQ producer and closing consumer...{RESET}")
    finally:
        dlq_producer.flush(timeout=5.0)
        consumer.close()
        print_summary(valid_count, running_total, recovered_count, dlq_count)


if __name__ == "__main__":
    main()
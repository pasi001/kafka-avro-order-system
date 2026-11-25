import json
import os
from collections import Counter

from confluent_kafka import Consumer, KafkaError

# Enable ANSI escape colors in Windows PowerShell
if os.name == "nt":
    os.system("")

# ANSI Color Codes
RESET = "\033[0m"
BOLD = "\033[1m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
MAGENTA = "\033[95m"
DIM = "\033[2m"

# Configuration
BOOTSTRAP_SERVERS = "localhost:9092"
DLQ_TOPIC = "orders-dlq"
GROUP_ID = "order-dlq-monitor-group"


def print_dlq_record(count: int, msg, data: dict) -> None:
    error_type = data.get("error_type", "UNKNOWN")
    reason = data.get("reason", "No reason provided")
    retry_count = data.get("retry_count", 0)
    src_topic = data.get("source_topic", "N/A")
    src_partition = data.get("source_partition", "N/A")
    src_offset = data.get("source_offset", "N/A")
    failed_at = data.get("failed_at", "N/A")
    original_key = data.get("original_key")
    original_payload = data.get("original_payload")

    badge_color = MAGENTA if error_type == "VALIDATION_ERROR" else RED

    print(f"{badge_color}{BOLD}--- [DLQ #{count}] {error_type} ---{RESET}")
    print(f"  {YELLOW}Reason         :{RESET} {reason}")
    print(f"  {YELLOW}Retry Count    :{RESET} {retry_count}")
    print(
        f"  {YELLOW}Source         :{RESET} topic={src_topic}, "
        f"partition={src_partition}, offset={src_offset}"
    )
    print(
        f"  {YELLOW}DLQ Position   :{RESET} partition={msg.partition()}, "
        f"offset={msg.offset()}"
    )
    print(f"  {YELLOW}Failed At (UTC):{RESET} {failed_at}")
    print(f"  {YELLOW}Original Key   :{RESET} {original_key}")
    print(f"  {YELLOW}Payload        :{RESET} {json.dumps(original_payload)}")
    print(f"{DIM}{'-' * 56}{RESET}\n")


def print_summary(total_count: int, error_counts: Counter) -> None:
    print(f"\n{CYAN}{BOLD}================ DLQ MONITOR SUMMARY ================ {RESET}")
    print(f"  Total DLQ Messages Received : {RED}{total_count}{RESET}")
    if error_counts:
        print("  Breakdown by Error Type     :")
        for err_type, cnt in error_counts.items():
            print(f"    - {err_type:<26}: {YELLOW}{cnt}{RESET}")
    print(f"{CYAN}{BOLD}====================================================={RESET}\n")


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    consumer.subscribe([DLQ_TOPIC])

    total_dlq = 0
    error_counts: Counter = Counter()

    print(f"{RED}{BOLD}[START] DLQ Consumer started... Monitoring '{DLQ_TOPIC}'{RESET}")
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

            raw_value = msg.value().decode("utf-8", errors="replace") if msg.value() else ""
            try:
                data = json.loads(raw_value)
            except json.JSONDecodeError:
                data = {
                    "error_type": "NON_JSON_DLQ_PAYLOAD",
                    "reason": "Raw payload is not valid JSON",
                    "retry_count": 0,
                    "original_key": msg.key().decode("utf-8", errors="replace") if msg.key() else None,
                    "original_payload": raw_value,
                }

            total_dlq += 1
            error_counts[data.get("error_type", "UNKNOWN")] += 1
            print_dlq_record(total_dlq, msg, data)

            consumer.commit(message=msg, asynchronous=False)

    except KeyboardInterrupt:
        print(f"\n{YELLOW}[SHUTDOWN] Ctrl+C detected. Closing DLQ consumer...{RESET}")
    finally:
        consumer.close()
        print_summary(total_dlq, error_counts)


if __name__ == "__main__":
    main()
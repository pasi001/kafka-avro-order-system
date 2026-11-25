
# Kafka + Avro Real-Time Order Processing System

A fault-tolerant, real-time order processing pipeline built with **Apache Kafka (KRaft mode)**, **Confluent Schema Registry (Avro)**, and **Python**. The system produces Avro-serialized order events, computes a live running average of valid order prices, handles transient failures via exponential backoff retries, and routes permanent or exhausted failures to a Dead Letter Queue (DLQ) with full metadata.

---

## Architecture & Data Flow

```text
+--------------------------+
|   producer/producer.py   |
|  (Avro Serializer, 1.5s) |
|  ~85% Valid | ~15% Bad   |
+------------+-------------+
             |
             | Registers / Validates Schema (orders-value, v1, ID: 1)
             v
+--------------------------+       +-----------------------------------+
| Confluent Schema Registry|<----->|  Apache Kafka (KRaft, localhost)  |
|   http://localhost:8081  |       |  Topic: orders  |  orders-dlq     |
+--------------------------+       +-----------------+-----------------+
                                                     |
                                         Consumes    | (Manual Commit)
                                                     v
                                   +-----------------------------------+
                                   |      consumer/consumer.py         |
                                   |  Group: order-processor-group     |
                                   +--------+---------------+----------+
                                            |               |
                        +-------------------+               +-------------------------+
                        | Valid Order                       | Failure Detected        |
                        v                                   v                         |
             +---------------------+          +---------------------------+           |
             | Update Real-Time    |          | Transient (~20% rate)?    |           |
             | Running Average     |          | Retry 3x (0.5s, 1s, 2s)   |           |
             | & Commit Offset     |          +------+-------------+------+           |
             +---------------------+                 |             |                  |
                                           Recovered |   Exhausted |    Permanent     |
                                                     v             | (Price <= 0 /    |
                                           (Process Normally)      |  Poison Pill)    |
                                                                   v                  |
                                                     +---------------------------+    |
                                                     |  Route to `orders-dlq`    |<---+
                                                     |  (UTF-8 JSON + Metadata)  |
                                                     +-------------+-------------+
                                                                   |
                                                                   v
                                                     +---------------------------+
                                                     | consumer/dlq_consumer.py  |
                                                     | Group: order-dlq-monitor  |
                                                     | Real-Time DLQ Inspector   |
                                                     +---------------------------+
```

---

## Project Structure

```text
kafka-avro-order-system/
├── docker-compose.yml        # Kafka (KRaft mode) & Confluent Schema Registry
├── requirements.txt          # Python dependencies (confluent-kafka, fastavro, etc.)
├── schemas/
│   └── order.avsc            # Avro schema (orderId: string, product: string, price: float)
├── producer/
│   └── producer.py           # Generates Avro-serialized valid & invalid orders every 1.5s
├── consumer/
│   ├── consumer.py           # Main consumer: running average, retries, DLQ routing, manual commit
│   └── dlq_consumer.py       # Real-time DLQ monitor displaying error metadata & summaries
├── .gitignore
└── README.md
```

---

## Prerequisites

- **OS:** Windows 10 / 11 (PowerShell)
- **Docker Desktop:** Running with Docker Compose enabled
- **Python:** 3.10 or higher

---

## Setup & Execution Guide (Windows PowerShell)

### 1. Clone Repository & Setup Virtual Environment

```powershell
git clone [https://github.com/pasi001/kafka-avro-order-system.git](https://github.com/pasi001/kafka-avro-order-system.git)
cd kafka-avro-order-system

python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Start Kafka & Schema Registry

```powershell
docker compose up -d
docker compose ps
```

Verify Schema Registry is reachable:

```powershell
Invoke-RestMethod http://localhost:8081/subjects
```

### 3. Run the Pipeline (3-Terminal Setup)

Open **three separate PowerShell terminals** in the project root and activate `.\venv\Scripts\Activate.ps1` in each.

**Terminal 1 — DLQ Monitor (`orders-dlq`):**

```powershell
.\venv\Scripts\Activate.ps1
python consumer/dlq_consumer.py
```

**Terminal 2 — Main Order Consumer (`orders`):**

```powershell
.\venv\Scripts\Activate.ps1
python consumer/consumer.py
```

**Terminal 3 — Order Producer:**

```powershell
.\venv\Scripts\Activate.ps1
python producer/producer.py
```

Press **`Ctrl + C`** in each terminal to trigger a graceful shutdown and print the session summary.

---

## Fault-Tolerance, Retry & DLQ Design

| Failure Category                      | Trigger Condition                 | Retry Policy                                   | DLQ `error_type`            |
| :------------------------------------ | :-------------------------------- | :--------------------------------------------- | :---------------------------- |
| **Permanent (Validation)**      | `price <= 0.0` (~15% produced)  | 0 retries (immediate DLQ)                      | `VALIDATION_ERROR`          |
| **Permanent (Deserialization)** | Malformed / non-Avro poison pill  | 0 retries (immediate DLQ)                      | `DESERIALIZATION_ERROR`     |
| **Transient (Simulated)**       | ~20% simulated downstream failure | Up to 3 retries (`0.5s`, `1.0s`, `2.0s`) | `TRANSIENT_RETRY_EXHAUSTED` |

- **Manual Offset Commits (`enable.auto.commit=False`):** Offsets are committed only after an order is either processed successfully or safely persisted to `orders-dlq`, preventing message loss.
- **Avro Schema Enforcement:** `schemas/order.avsc` is registered under subject `orders-value` (Version `1`, Schema ID `1`).
- **DLQ JSON Envelope Format:** Every failed record published to `orders-dlq` is serialized as UTF-8 JSON containing:
  - `original_key`, `original_payload`
  - `error_type`, `reason`, `retry_count`
  - `source_topic`, `source_partition`, `source_offset`, `failed_at` (UTC)

---

## Sample Terminal Output

### Main Consumer (`consumer/consumer.py`)

```text
[OK] Order ORD-1001 | Product: Laptop Stand | Price: $45.50 | Valid Count: 1 | Total: $45.50 | Running Avg: $45.50
[WARN] Transient error on ORD-1002 (Attempt 1/3). Retrying in 0.5s...
[OK] Recovered ORD-1002 on retry 1 | Product: USB-C Hub | Price: $34.50 | Valid Count: 2 | Total: $80.00 | Running Avg: $40.00
[DLQ] Permanent VALIDATION_ERROR on ORD-1003 (price=-15.00). Routed to orders-dlq (0 retries).
[WARN] Transient error on ORD-1004 (Attempt 1/3). Retrying in 0.5s...
[WARN] Transient error on ORD-1004 (Attempt 2/3). Retrying in 1.0s...
[WARN] Transient error on ORD-1004 (Attempt 3/3). Retrying in 2.0s...
[DLQ] TRANSIENT_RETRY_EXHAUSTED on ORD-1004 after 3 retries. Routed to orders-dlq.
^C
=== Consumer Session Summary ===
Valid Orders Processed : 14
Running Total          : $3,412.80
Final Running Average  : $243.77
Sent to DLQ            : 4
```

### DLQ Monitor (`consumer/dlq_consumer.py`)

```text
================ DLQ MESSAGE RECEIVED ================
Error Type : VALIDATION_ERROR
Reason     : Order price must be greater than 0.0 (received -15.0)
Retries    : 0
Source     : orders [partition=0, offset=2]
Failed At  : 2026-11-25T11:55:12.408121+00:00
Key        : ORD-1003
Payload    : {"orderId": "ORD-1003", "product": "Wireless Mouse", "price": -15.0}
======================================================
^C
=== DLQ Monitor Session Summary ===
Total DLQ Messages Observed : 4
```

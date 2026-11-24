# Kafka Avro Order Processing System

A real-time Apache Kafka order processing pipeline built with Avro serialization, streaming price aggregation, retry handling, and Dead Letter Queue (DLQ) support.

## Key Features
- **Avro Serialization:** Produces and consumes order messages strictly validated against `schemas/order.avsc`.
- **Real-Time Aggregation:** Computes a continuous running average of incoming order prices.
- **Retry Logic:** Handles transient processing failures with configurable retry attempts.
- **Dead Letter Queue (DLQ):** Routes permanently failed messages to a dedicated DLQ topic for inspection and recovery.

## Order Message Schema (`schemas/order.avsc`)
| Field | Type | Description |
| :--- | :--- | :--- |
| `orderId` | `string` | Unique identifier for the order (e.g., `"1001"`) |
| `product` | `string` | Name of the purchased item (e.g., `"Item1"`) |
| `price` | `float` | Randomized price of the product |

## Project Structure
- `schemas/order.avsc` - Avro schema definition for order events
- `docker-compose.yml` - Kafka, Zookeeper, and Schema Registry setup
- `producer/` - Avro order event producer
- `consumer/` - Consumer with running average aggregation, retry logic, and DLQ routing

## Submission Info
- **Registration Number:** EG/2021/4544

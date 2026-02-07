# Virtual Device Simulator

A Python-based service to simulate data from virtual devices (wearables, restaurant orders, IoT GPS sensors) and stream to Kafka topics.

## Features

- **Wearable Devices**: Simulates BP, blood sugar, and heart rate data
- **Restaurant Orders**: Simulates table orders with dish codes and payment status
- **GPS Telemetry**: Simulates realistic GPS movement along Indian city routes
- **Bad Data Injection**: Configurable probability for anomalous data
- **Monitoring Dashboard**: FastAPI-based dashboard with real-time metrics

## Quick Start

### 1. Start Kafka

```bash
docker-compose up -d
```

### 2. Install Dependencies

```bash
uv venv && uv pip install -r requirements.txt
```

### 3. Run the Simulator

```bash
uv run python -m virtual_devices.main --config config/settings.yaml
```

### 4. View Dashboard

Open http://localhost:8080 in your browser.

## Kafka Topics

| Topic | Description |
|-------|-------------|
| `virtual-wearables` | Health device data (BP, sugar, heart rate) |
| `virtual-restaurants` | Restaurant order events |
| `virtual-gps` | GPS telemetry data |

## Configuration

See `config/settings.yaml` for configurable options:
- Device counts per category
- Data generation frequency
- Bad data probability
- Kafka connection settings
- GPS route waypoints

## Consuming Messages

```bash
# Wearables
docker exec -it kafka bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic virtual-wearables

# Restaurants
docker exec -it kafka bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic virtual-restaurants

# GPS
docker exec -it kafka bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic virtual-gps
```

# OpenPipe - Virtual Device Simulator & Data Pipeline

A Python-based platform for simulating IoT device data, streaming to Kafka, persisting to TimescaleDB, and exploring data via SQL queries.

## Features

### Virtual Device Simulator
- **Wearable Devices**: Simulates BP, blood sugar, and heart rate data
- **Restaurant Orders**: Simulates table orders with dish codes and payment status
- **GPS Telemetry**: Simulates realistic GPS movement along Indian city routes
- **Bad Data Injection**: Configurable probability for anomalous data
- **Monitoring Dashboard**: FastAPI dashboard with real-time metrics at http://localhost:8080

### Data Pipeline Service
- **Kafka Consumer**: Consumes messages from all Kafka topics with metadata enrichment
- **TimescaleDB Storage**: Persists messages to hypertables optimized for time-series
- **SQL Query UI**: Web interface for browsing tables and executing queries
- **Health Monitoring**: Comprehensive health checks for all components

## Architecture

```
┌─────────────────────┐     ┌─────────────────┐     ┌─────────────────────┐
│ Virtual Device      │────▶│ Kafka           │────▶│ Data Pipeline       │
│ Simulator (:8080)   │     │ (:9092)         │     │ Service (:8081)     │
└─────────────────────┘     └─────────────────┘     └──────────┬──────────┘
                                                               │
                                                    ┌──────────▼──────────┐
                                                    │ PostgreSQL (TimescaleDB optional) (:5432) │
                                                    └─────────────────────┘
```

## Quick Start

### 1. Start Infrastructure

```bash
docker-compose up -d
```

This starts Kafka, PostgreSQL (standard), and Kafka UI.

### 2. Install Dependencies

```bash
uv venv && uv pip install -r requirements.txt
```

### 3. Run the Simulator

```bash
uv run python -m virtual_devices.main --config config/settings.yaml
```

Open http://localhost:8080 for the simulator dashboard.

### 4. Run the Data Pipeline

```bash
uv run python -m data_pipeline.main --config config/settings.yaml
```

Open http://localhost:8081 for the data viewer and SQL query interface.

## Service Ports

| Service | Port | Description |
|---------|------|-------------|
| Kafka | 9092 | Message broker |
| PostgreSQL | 5432 | Database (standard Pg15) |
| Virtual Device Dashboard | 8080 | Device monitoring |
| Kafka UI | 8082 | Message inspection |
| Data Pipeline UI | 8081 | Data viewer + health |

## Health Endpoints

| Endpoint | Service | Description |
|----------|---------|-------------|
| `/health` | Both | Comprehensive health check |
| `/health/live` | Both | Liveness probe |
| `/health/ready` | Both | Readiness probe |
| `/metrics` | Simulator | Prometheus metrics |

## Kafka Topics

| Topic | Description |
|-------|-------------|
| `virtual-wearables` | Health device data (BP, sugar, heart rate) |
| `virtual-restaurants` | Restaurant order events |
| `virtual-gps` | GPS telemetry data |

## Configuration

See `config/settings.yaml` for all configurable options:
- Device counts and frequencies
- Bad data probability
- Kafka connection settings
- Database connection settings
- Pipeline batch size and flush interval
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


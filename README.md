# OpenPipe - Virtual Device Simulator & Data Pipeline

A Python-based platform for simulating IoT device data, streaming to Kafka, processing with Apache Flink (PyFlink), and persisting to TimescaleDB.

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

### Stream Processing (Apache Flink via PyFlink)
- **Real Flink Cluster**: Jobs submitted to Flink JobManager + TaskManager
- **Windowed Aggregations**: Tumbling windows for real-time metric computation
- **Heart Rate Monitoring**: 1-minute windows with alerts when > 100 bpm
- **Blood Pressure Monitoring**: 1-minute windows with alerts when ≥ 140/90 mmHg
- **Blood Sugar Monitoring**: 10-minute windows with alerts when ≥ 180 mg/dL
- **Alert Generation**: Automatic alerts for elevated health metrics
- **JDBC Sink**: Aggregation results written directly to TimescaleDB
- **Standalone Fallback**: `--standalone` flag for environments without Flink

## Architecture

```
┌─────────────────────┐     ┌─────────────────┐     ┌─────────────────────┐
│ Virtual Device      │────▶│ Kafka           │────▶│ Data Pipeline       │
│ Simulator (:8080)   │     │ (:9092)         │     │ Service (:8081)     │
└─────────────────────┘     └─────────────────┘     └──────────┬──────────┘
                                    │                          │
                                    │               ┌──────────▼──────────┐
                                    │               │ TimescaleDB (:5432) │
                                    │               └──────────▲──────────┘
                                    │                          │
                            ┌───────▼───────┐                  │
                            │ Flink Cluster  │                 │
                            │  (:8083 UI)    │                 │
                            │  JobManager    │                 │
                            │  TaskManager   │                 │
                            └───────┬───────┘                  │
                                    │                          │
                    ┌───────────────┼───────────────┐          │
                    ▼               ▼               ▼          │
            ┌─────────────┐ ┌─────────────┐ ┌─────────────┐   │
            │ HR Windows  │ │ BP Windows  │ │Sugar Windows│   │
            │ (1 min)     │ │ (1 min)     │ │ (10 min)    │   │
            └──────┬──────┘ └──────┬──────┘ └──────┬──────┘   │
                   │               │               │          │
                   └───────────────┼───────────────┘          │
                                   ▼                          │
                           ┌──────────────┐                   │
                           │ JDBC Sink    │───────────────────┘
                           │ (Alerts +    │
                           │ Aggregations)│
                           └──────────────┘
```

## Quick Start

### 1. Start Infrastructure

```bash
docker-compose up -d
```

This starts Kafka, TimescaleDB, Kafka UI, and Flink (JobManager + TaskManager).
The `flink-jar-downloader` init container automatically fetches required connector JARs.

### 2. Install Dependencies

```bash
uv venv && uv pip install -r requirements.txt
```

> **Note:** PyFlink requires Java 11+. Verify with `java -version`.

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

### 5. Run the Stream Processor (PyFlink)

```bash
# Default: submits a PyFlink job to the Flink cluster
uv run python -m flink_processor.main --config config/settings.yaml

# Fallback: standalone Python processor (no Flink cluster needed)
uv run python -m flink_processor.main --config config/settings.yaml --standalone
```

Verify the job is running at the Flink UI: http://localhost:8083

## Service Ports

| Service | Port | Description |
|---------|------|-------------|
| Kafka | 9092 | Message broker |
| TimescaleDB | 5432 | Time-series database |
| Virtual Device Dashboard | 8080 | Device monitoring |
| Data Pipeline UI | 8081 | Data viewer + SQL query |
| Kafka UI | 8082 | Message inspection |
| Flink UI | 8083 | Stream processing dashboard |

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

## Stream Processing Tables

| Table | Description |
|-------|-------------|
| `health_metrics_1min` | Heart rate & BP aggregations (1-min windows) |
| `health_metrics_10min` | Blood sugar aggregations (10-min windows) |
| `health_alerts` | Elevated reading alerts |

## Configuration

See `config/settings.yaml` for all configurable options:
- Device counts and frequencies
- Bad data probability
- Kafka connection settings
- Database connection settings
- Pipeline batch size and flush interval
- GPS route waypoints
- **Flink cluster connection and execution mode**
- **Flink window sizes and alert thresholds**

### Flink Configuration

```yaml
flink:
  execution_mode: "local"          # "local" (mini-cluster) or "remote" (Flink cluster)
  jobmanager_host: "localhost"
  jobmanager_port: 8083
  windows:
    heart_rate_minutes: 1
    blood_pressure_minutes: 1
    blood_sugar_minutes: 10
  thresholds:
    elevated_heart_rate: 100      # bpm
    elevated_bp_systolic: 140     # mmHg
    elevated_bp_diastolic: 90     # mmHg
    elevated_blood_sugar: 180     # mg/dL
```

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

## Querying Aggregated Metrics

```sql
-- 1-minute heart rate aggregations
SELECT * FROM health_metrics_1min
WHERE metric_type = 'heart_rate'
ORDER BY time DESC LIMIT 10;

-- 1-minute blood pressure aggregations
SELECT * FROM health_metrics_1min
WHERE metric_type = 'blood_pressure'
ORDER BY time DESC LIMIT 10;

-- 10-minute blood sugar aggregations
SELECT * FROM health_metrics_10min ORDER BY time DESC LIMIT 5;

-- Recent health alerts
SELECT * FROM health_alerts ORDER BY time DESC LIMIT 20;
```

## Running Tests

```bash
# Flink processor tests
uv run python -m pytest flink_processor/tests/test_processor.py -v
```

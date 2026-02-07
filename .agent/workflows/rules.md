---
description: Data Engineering & Software Engineering best practices for OpenPipe virtual device simulator
---

# Agent Rules for OpenPipe

## Python Environment

### Always Use `uv`
- **Never use `pip` or `venv` directly** - always use `uv`
- Create virtual env: `uv venv`
- Install deps: `uv pip install -r requirements.txt`
- Run scripts: `uv run python <script>`
- The venv is created in `.venv/` directory

---

## Code Style

### Python
- Use type hints for all function parameters and return values
- Follow PEP 8 naming conventions (snake_case for functions/variables, PascalCase for classes)
- Use dataclasses or Pydantic models for structured data
- Keep functions under 50 lines; extract helpers when needed

### Configuration
- All configurable values go in `config/settings.yaml`
- Never hardcode connection strings, ports, or thresholds
- Use environment variables for secrets (via `.env` file)

---

## Data Engineering Patterns

### Message Schema
- Every Kafka message MUST include: `device_id`, `timestamp`, `is_anomaly`
- Use ISO 8601 format for timestamps with timezone
- Include units in nested objects (e.g., `{"value": 120, "unit": "mmHg"}`)

### Bad Data Generation
- Bad data probability should be configurable (default: 2-5%)
- Bad data types to simulate:
  - Null values
  - Out-of-range values (negative, extreme)
  - Invalid types (string instead of number)
  - Missing required fields

### Kafka Topics
- One topic per device category: `virtual-wearables`, `virtual-restaurants`, `virtual-gps`
- Use device_id as message key for partition affinity
- Set `acks=all` for reliability

---

## Testing Standards

### Unit Tests
- Test normal data generation ranges
- Test bad data injection logic
- Test metrics tracking accuracy

### Integration Tests
- Verify Kafka connectivity
- Verify message serialization/deserialization
- Verify dashboard API responses

### Run Tests
```bash
# Manual verification (no Kafka needed)
uv run python virtual_devices/tests/manual_verify.py

# Full integration (requires Kafka)
uv run python -m virtual_devices.main --config config/settings.yaml
```

---

## Docker & Infrastructure

### Kafka Setup
- Use `quay.io/strimzi/kafka` image (avoids Docker Hub rate limits)
- KRaft mode (no Zookeeper required)
- Default port: 9092

### Commands
```bash
# // turbo
docker compose up -d

# // turbo
docker compose down
```

---

## File Structure Conventions

```
openpipe/
├── config/           # YAML configuration files
├── virtual_devices/
│   ├── simulators/   # Device simulator implementations
│   ├── tests/        # Test modules
│   ├── main.py       # Entry point
│   └── dashboard.py  # FastAPI dashboard
└── .agent/workflows/ # Agent workflow definitions
```

---

## Common Tasks

### Add New Device Type
1. Create `virtual_devices/simulators/{device_type}.py`
2. Extend `BaseDeviceSimulator`
3. Implement `generate_normal_data()` and `generate_bad_data()`
4. Add topic to `config/settings.yaml`
5. Register in `device_manager.py` SIMULATOR_CLASSES

### Modify Data Generation Frequency
Edit `config/settings.yaml`:
```yaml
devices:
  {device_type}:
    frequency_seconds: 2.0
```

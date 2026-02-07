"""Configuration management for data pipeline service."""

import os
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass, field

import yaml


@dataclass
class DatabaseConfig:
    """Database configuration."""
    host: str = "localhost"
    port: int = 5432
    name: str = "openpipe_data"
    user: str = "openpipe"
    password: str = "openpipe"
    
    @property
    def connection_string(self) -> str:
        """Get SQLAlchemy connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


@dataclass
class PipelineConfig:
    """Pipeline processing configuration."""
    batch_size: int = 100
    flush_interval_seconds: float = 5.0
    consumer_group: str = "data-pipeline"


@dataclass
class KafkaConfig:
    """Kafka configuration."""
    bootstrap_servers: str = "localhost:9092"
    topics: Dict[str, str] = field(default_factory=lambda: {
        "wearables": "virtual-wearables",
        "restaurants": "virtual-restaurants",
        "gps": "virtual-gps"
    })


@dataclass
class MonitoringConfig:
    """Monitoring configuration."""
    dashboard_port: int = 8081
    metrics_enabled: bool = True


@dataclass
class AppConfig:
    """Main application configuration."""
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)


def load_config(config_path: str = "config/settings.yaml") -> AppConfig:
    """Load configuration from YAML file."""
    path = Path(config_path)
    
    if not path.exists():
        print(f"[Config] Config file not found: {config_path}, using defaults")
        return AppConfig()
    
    with open(path, 'r') as f:
        data = yaml.safe_load(f)
    
    config = AppConfig()
    
    # Load Kafka config
    if 'kafka' in data:
        kafka_data = data['kafka']
        config.kafka = KafkaConfig(
            bootstrap_servers=kafka_data.get('bootstrap_servers', 'localhost:9092'),
            topics=kafka_data.get('topics', config.kafka.topics)
        )
    
    # Load Database config
    if 'database' in data:
        db_data = data['database']
        config.database = DatabaseConfig(
            host=db_data.get('host', 'localhost'),
            port=db_data.get('port', 5432),
            name=db_data.get('name', 'openpipe_data'),
            user=db_data.get('user', 'openpipe'),
            password=db_data.get('password', 'openpipe')
        )
    
    # Load Pipeline config
    if 'pipeline' in data:
        pipe_data = data['pipeline']
        config.pipeline = PipelineConfig(
            batch_size=pipe_data.get('batch_size', 100),
            flush_interval_seconds=pipe_data.get('flush_interval_seconds', 5.0),
            consumer_group=pipe_data.get('consumer_group', 'data-pipeline')
        )
    
    # Load Monitoring config
    if 'monitoring' in data:
        mon_data = data['monitoring']
        # Use pipeline port, not simulator port
        config.monitoring = MonitoringConfig(
            dashboard_port=mon_data.get('pipeline_port', 8081),
            metrics_enabled=mon_data.get('metrics_enabled', True)
        )
    
    return config

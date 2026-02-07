"""Configuration for Flink stream processor."""

from dataclasses import dataclass, field
from typing import Dict, Any
import yaml
from pathlib import Path


@dataclass
class WindowConfig:
    """Window size configuration."""
    heart_rate_minutes: int = 1
    blood_pressure_minutes: int = 1
    blood_sugar_minutes: int = 10


@dataclass
class ThresholdConfig:
    """Alert threshold configuration."""
    elevated_heart_rate: int = 100  # bpm
    elevated_bp_systolic: int = 140  # mmHg
    elevated_bp_diastolic: int = 90  # mmHg
    elevated_blood_sugar: int = 180  # mg/dL


@dataclass
class FlinkConfig:
    """Complete Flink processor configuration."""
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "virtual-wearables"
    consumer_group: str = "flink-health-processor"
    
    # Database
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "openpipe_data"
    db_user: str = "openpipe"
    db_password: str = "openpipe"
    
    # Windows and thresholds
    windows: WindowConfig = field(default_factory=WindowConfig)
    thresholds: ThresholdConfig = field(default_factory=ThresholdConfig)
    
    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.db_host}:{self.db_port}/{self.db_name}"
    
    @classmethod
    def from_yaml(cls, config_path: str) -> "FlinkConfig":
        """Load configuration from YAML file."""
        path = Path(config_path)
        if not path.exists():
            return cls()
        
        with open(path) as f:
            data = yaml.safe_load(f)
        
        # Extract Kafka settings
        kafka_cfg = data.get("kafka", {})
        db_cfg = data.get("database", {})
        flink_cfg = data.get("flink", {})
        
        windows_data = flink_cfg.get("windows", {})
        thresholds_data = flink_cfg.get("thresholds", {})
        
        return cls(
            kafka_bootstrap_servers=kafka_cfg.get("bootstrap_servers", "localhost:9092"),
            kafka_topic=kafka_cfg.get("topics", {}).get("wearables", "virtual-wearables"),
            db_host=db_cfg.get("host", "localhost"),
            db_port=db_cfg.get("port", 5432),
            db_name=db_cfg.get("name", "openpipe_data"),
            db_user=db_cfg.get("user", "openpipe"),
            db_password=db_cfg.get("password", "openpipe"),
            windows=WindowConfig(
                heart_rate_minutes=windows_data.get("heart_rate_minutes", 1),
                blood_pressure_minutes=windows_data.get("blood_pressure_minutes", 1),
                blood_sugar_minutes=windows_data.get("blood_sugar_minutes", 10),
            ),
            thresholds=ThresholdConfig(
                elevated_heart_rate=thresholds_data.get("elevated_heart_rate", 100),
                elevated_bp_systolic=thresholds_data.get("elevated_bp_systolic", 140),
                elevated_bp_diastolic=thresholds_data.get("elevated_bp_diastolic", 90),
                elevated_blood_sugar=thresholds_data.get("elevated_blood_sugar", 180),
            ),
        )

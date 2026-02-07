"""Configuration management for Virtual Device Simulator"""

import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional
from pydantic import BaseModel
from pydantic_settings import BaseSettings


class KafkaConfig(BaseModel):
    """Kafka configuration"""
    bootstrap_servers: str = "localhost:9092"
    topics: Dict[str, str] = {
        "wearables": "virtual-wearables",
        "restaurants": "virtual-restaurants", 
        "gps": "virtual-gps"
    }


class DeviceTypeConfig(BaseModel):
    """Configuration for a single device type"""
    count: int = 5
    frequency_seconds: float = 2.0
    bad_data_probability: float = 0.05


class DevicesConfig(BaseModel):
    """All device configurations"""
    wearables: DeviceTypeConfig = DeviceTypeConfig()
    restaurants: DeviceTypeConfig = DeviceTypeConfig(count=3, frequency_seconds=5.0, bad_data_probability=0.03)
    gps: DeviceTypeConfig = DeviceTypeConfig(count=10, frequency_seconds=1.0, bad_data_probability=0.02)


class MonitoringConfig(BaseModel):
    """Monitoring configuration"""
    dashboard_port: int = 8080


class GPSWaypoint(BaseModel):
    """GPS waypoint with lat/lng"""
    waypoints: List[List[float]]


class GPSRoutesConfig(BaseModel):
    """GPS routes configuration"""
    bangalore: Optional[GPSWaypoint] = None
    mumbai: Optional[GPSWaypoint] = None
    delhi: Optional[GPSWaypoint] = None


class AppConfig(BaseModel):
    """Main application configuration"""
    kafka: KafkaConfig = KafkaConfig()
    devices: DevicesConfig = DevicesConfig()
    monitoring: MonitoringConfig = MonitoringConfig()
    gps_routes: GPSRoutesConfig = GPSRoutesConfig()

    @classmethod
    def from_yaml(cls, path: str) -> "AppConfig":
        """Load configuration from YAML file"""
        config_path = Path(path)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")
        
        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)
        
        return cls(**data)


# Global config instance
_config: Optional[AppConfig] = None


def get_config() -> AppConfig:
    """Get the global configuration instance"""
    global _config
    if _config is None:
        raise RuntimeError("Configuration not loaded. Call load_config() first.")
    return _config


def load_config(path: str) -> AppConfig:
    """Load configuration from file and set as global"""
    global _config
    _config = AppConfig.from_yaml(path)
    return _config

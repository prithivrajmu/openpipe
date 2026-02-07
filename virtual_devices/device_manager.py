"""Device manager - orchestrates all virtual device simulators"""

import threading
import uuid
from typing import Dict, Any, List, Optional, Type
from datetime import datetime, timezone

from .simulators.base import BaseDeviceSimulator
from .simulators.wearable import WearableSimulator
from .simulators.restaurant import RestaurantSimulator
from .simulators.gps import GPSSimulator
from .kafka_producer import VirtualDeviceKafkaProducer
from .metrics import get_metrics_collector
from .config import AppConfig


SIMULATOR_CLASSES: Dict[str, Type[BaseDeviceSimulator]] = {
    "wearable": WearableSimulator,
    "restaurant": RestaurantSimulator,
    "gps": GPSSimulator
}


class DeviceManager:
    """
    Orchestrates all virtual device simulators.
    
    Features:
    - Spin up/down devices dynamically
    - Track active devices per category
    - Integrate with Kafka producer
    - Graceful shutdown
    """
    
    def __init__(self, config: AppConfig, kafka_producer: VirtualDeviceKafkaProducer):
        self.config = config
        self.kafka_producer = kafka_producer
        self.metrics_collector = get_metrics_collector()
        
        self._lock = threading.Lock()
        self._devices: Dict[str, BaseDeviceSimulator] = {}
        self._running = False
    
    def _create_data_callback(self, device_type: str):
        """Create a callback function that sends data to Kafka."""
        def callback(data: Dict[str, Any]):
            try:
                self.kafka_producer.send(device_type, data)
            except Exception as e:
                print(f"[DeviceManager] Error sending to Kafka: {e}")
        return callback
    
    def spawn_device(
        self,
        device_type: str,
        device_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Spawn a new virtual device.
        
        Args:
            device_type: Type of device (wearable, restaurant, gps)
            device_id: Optional custom device ID
            **kwargs: Additional arguments for the simulator
        
        Returns:
            The device ID of the spawned device
        """
        if device_type not in SIMULATOR_CLASSES:
            raise ValueError(f"Unknown device type: {device_type}")
        
        simulator_class = SIMULATOR_CLASSES[device_type]
        
        # Get configuration for this device type
        device_config = getattr(self.config.devices, f"{device_type}s", None)
        if device_config is None:
            device_config = getattr(self.config.devices, device_type, None)
        
        frequency = kwargs.pop("frequency_seconds", 
                              device_config.frequency_seconds if device_config else 2.0)
        bad_data_prob = kwargs.pop("bad_data_probability",
                                   device_config.bad_data_probability if device_config else 0.05)
        
        # Create the simulator
        simulator = simulator_class(
            device_id=device_id,
            frequency_seconds=frequency,
            bad_data_probability=bad_data_prob,
            on_data_callback=self._create_data_callback(device_type),
            **kwargs
        )
        
        actual_device_id = simulator.device_id
        
        with self._lock:
            self._devices[actual_device_id] = simulator
        
        # Register with metrics collector
        self.metrics_collector.register_device(actual_device_id, device_type, simulator)
        
        # Start the simulator
        simulator.start()
        
        print(f"[DeviceManager] Spawned {device_type} device: {actual_device_id}")
        return actual_device_id
    
    def stop_device(self, device_id: str) -> bool:
        """
        Stop and remove a device.
        
        Returns:
            True if device was found and stopped, False otherwise
        """
        with self._lock:
            device = self._devices.pop(device_id, None)
        
        if device:
            device.stop()
            self.metrics_collector.unregister_device(device_id)
            print(f"[DeviceManager] Stopped device: {device_id}")
            return True
        
        return False
    
    def spawn_initial_devices(self):
        """Spawn devices based on configuration."""
        device_configs = [
            ("wearable", self.config.devices.wearables.count),
            ("restaurant", self.config.devices.restaurants.count),
            ("gps", self.config.devices.gps.count),
        ]
        
        for device_type, count in device_configs:
            for i in range(count):
                # For GPS, vary city routes
                kwargs = {}
                if device_type == "gps":
                    cities = ["bangalore", "mumbai", "delhi"]
                    kwargs["city"] = cities[i % len(cities)]
                
                self.spawn_device(device_type, **kwargs)
        
        self._running = True
        print(f"[DeviceManager] Spawned {len(self._devices)} initial devices")
    
    def get_device(self, device_id: str) -> Optional[BaseDeviceSimulator]:
        """Get a device by ID."""
        with self._lock:
            return self._devices.get(device_id)
    
    def get_all_devices(self) -> List[Dict[str, Any]]:
        """Get info about all devices."""
        with self._lock:
            return [
                {
                    "device_id": d.device_id,
                    "device_type": d.DEVICE_TYPE,
                    "running": d.is_running(),
                    "metrics": d.get_metrics()
                }
                for d in self._devices.values()
            ]
    
    def get_devices_by_type(self, device_type: str) -> List[Dict[str, Any]]:
        """Get all devices of a specific type."""
        with self._lock:
            return [
                {
                    "device_id": d.device_id,
                    "device_type": d.DEVICE_TYPE,
                    "running": d.is_running(),
                    "metrics": d.get_metrics()
                }
                for d in self._devices.values()
                if d.DEVICE_TYPE == device_type
            ]
    
    def get_device_count(self) -> Dict[str, int]:
        """Get count of devices by type."""
        with self._lock:
            counts = {"wearable": 0, "restaurant": 0, "gps": 0, "total": 0}
            for device in self._devices.values():
                counts[device.DEVICE_TYPE] = counts.get(device.DEVICE_TYPE, 0) + 1
                counts["total"] += 1
            return counts
    
    def shutdown(self):
        """Stop all devices and clean up."""
        self._running = False
        
        with self._lock:
            device_ids = list(self._devices.keys())
        
        for device_id in device_ids:
            self.stop_device(device_id)
        
        print("[DeviceManager] Shutdown complete")
    
    def is_running(self) -> bool:
        """Check if manager is running."""
        return self._running

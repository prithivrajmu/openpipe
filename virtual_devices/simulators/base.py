"""Base device simulator class"""

import random
import uuid
import threading
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Callable
from dataclasses import dataclass, field


@dataclass
class DeviceMetrics:
    """Metrics for a single device"""
    device_id: str
    device_type: str
    data_points_sent: int = 0
    bad_data_points: int = 0
    last_sent_at: Optional[datetime] = None
    errors: int = 0
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "device_id": self.device_id,
            "device_type": self.device_type,
            "data_points_sent": self.data_points_sent,
            "bad_data_points": self.bad_data_points,
            "last_sent_at": self.last_sent_at.isoformat() if self.last_sent_at else None,
            "errors": self.errors,
            "started_at": self.started_at.isoformat(),
            "uptime_seconds": (datetime.now(timezone.utc) - self.started_at).total_seconds()
        }


class BaseDeviceSimulator(ABC):
    """
    Abstract base class for device simulators.
    
    Each simulator runs in its own thread and generates data at a configured frequency.
    """
    
    DEVICE_TYPE: str = "base"
    
    def __init__(
        self,
        device_id: Optional[str] = None,
        frequency_seconds: float = 1.0,
        bad_data_probability: float = 0.05,
        on_data_callback: Optional[Callable[[Dict[str, Any]], None]] = None
    ):
        self.device_id = device_id or f"{self.DEVICE_TYPE}_{uuid.uuid4().hex[:8]}"
        self.frequency_seconds = frequency_seconds
        self.bad_data_probability = bad_data_probability
        self.on_data_callback = on_data_callback
        
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        
        self.metrics = DeviceMetrics(
            device_id=self.device_id,
            device_type=self.DEVICE_TYPE
        )
    
    @abstractmethod
    def generate_normal_data(self) -> Dict[str, Any]:
        """Generate normal, valid data point. Must be implemented by subclasses."""
        pass
    
    @abstractmethod
    def generate_bad_data(self) -> Dict[str, Any]:
        """Generate anomalous/bad data point. Must be implemented by subclasses."""
        pass
    
    def generate_data(self) -> Dict[str, Any]:
        """Generate data point, with chance of bad data based on probability."""
        is_bad = random.random() < self.bad_data_probability
        
        if is_bad:
            data = self.generate_bad_data()
            data["is_anomaly"] = True
            with self._lock:
                self.metrics.bad_data_points += 1
        else:
            data = self.generate_normal_data()
            data["is_anomaly"] = False
        
        # Add common fields
        data["device_id"] = self.device_id
        data["timestamp"] = datetime.now(timezone.utc).isoformat()
        
        with self._lock:
            self.metrics.data_points_sent += 1
            self.metrics.last_sent_at = datetime.now(timezone.utc)
        
        return data
    
    def _run_loop(self):
        """Main loop that generates data at configured frequency."""
        while self._running:
            try:
                data = self.generate_data()
                if self.on_data_callback:
                    self.on_data_callback(data)
            except Exception as e:
                with self._lock:
                    self.metrics.errors += 1
                print(f"[{self.device_id}] Error generating data: {e}")
            
            time.sleep(self.frequency_seconds)
    
    def start(self):
        """Start the device simulator in a background thread."""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        print(f"[{self.device_id}] Started")
    
    def stop(self):
        """Stop the device simulator."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)
        print(f"[{self.device_id}] Stopped")
    
    def is_running(self) -> bool:
        """Check if the simulator is running."""
        return self._running
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics for this device."""
        with self._lock:
            return self.metrics.to_dict()

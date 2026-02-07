"""Centralized metrics collection for virtual devices"""

import threading
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from dataclasses import dataclass, field
from collections import defaultdict


@dataclass
class AggregatedMetrics:
    """Aggregated metrics for a device type"""
    device_type: str
    active_devices: int = 0
    total_data_points: int = 0
    total_bad_data_points: int = 0
    total_errors: int = 0
    devices: List[Dict[str, Any]] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "device_type": self.device_type,
            "active_devices": self.active_devices,
            "total_data_points": self.total_data_points,
            "total_bad_data_points": self.total_bad_data_points,
            "total_errors": self.total_errors,
            "bad_data_rate": round(
                self.total_bad_data_points / max(self.total_data_points, 1) * 100, 2
            ),
            "devices": self.devices
        }


class MetricsCollector:
    """
    Centralized metrics collection for all virtual devices.
    
    Provides:
    - Per-device metrics tracking
    - Aggregated metrics by device type
    - Real-time statistics
    - Cumulative session counters (persist when devices stop)
    """
    
    # All supported device types - always shown even if no active devices
    DEVICE_TYPES = ["wearable", "restaurant", "gps"]
    
    def __init__(self):
        self._lock = threading.RLock()
        self._devices: Dict[str, Dict[str, Any]] = {}  # device_id -> device info
        self._started_at = datetime.now(timezone.utc)
        
        # Cumulative session counters (persist when devices are stopped)
        self._session_data_points: Dict[str, int] = {t: 0 for t in self.DEVICE_TYPES}
        self._session_bad_data: Dict[str, int] = {t: 0 for t in self.DEVICE_TYPES}
        self._session_errors: Dict[str, int] = {t: 0 for t in self.DEVICE_TYPES}
    
    def register_device(self, device_id: str, device_type: str, device_ref: Any):
        """Register a device for metrics tracking."""
        with self._lock:
            self._devices[device_id] = {
                "device_id": device_id,
                "device_type": device_type,
                "device_ref": device_ref,
                "registered_at": datetime.now(timezone.utc)
            }
    
    def unregister_device(self, device_id: str):
        """Unregister a device, preserving its final metrics in session totals."""
        with self._lock:
            device_info = self._devices.get(device_id)
            if device_info:
                # Save final metrics to session counters before removing
                device_ref = device_info["device_ref"]
                device_type = device_info["device_type"]
                if hasattr(device_ref, "get_metrics"):
                    metrics = device_ref.get_metrics()
                    self._session_data_points[device_type] += metrics.get("data_points_sent", 0)
                    self._session_bad_data[device_type] += metrics.get("bad_data_points", 0)
                    self._session_errors[device_type] += metrics.get("errors", 0)
                self._devices.pop(device_id, None)
    
    def get_device_metrics(self, device_id: str) -> Optional[Dict[str, Any]]:
        """Get metrics for a specific device."""
        with self._lock:
            device_info = self._devices.get(device_id)
            if not device_info:
                return None
            
            device_ref = device_info["device_ref"]
            if hasattr(device_ref, "get_metrics"):
                return device_ref.get_metrics()
            return None
    
    def get_all_device_metrics(self) -> List[Dict[str, Any]]:
        """Get metrics for all devices."""
        with self._lock:
            metrics = []
            for device_id, device_info in self._devices.items():
                device_ref = device_info["device_ref"]
                if hasattr(device_ref, "get_metrics"):
                    metrics.append(device_ref.get_metrics())
            return metrics
    
    def get_metrics_by_type(self, device_type: str) -> AggregatedMetrics:
        """Get aggregated metrics for a device type (includes session history)."""
        with self._lock:
            result = AggregatedMetrics(device_type=device_type)
            
            # Start with session cumulative counters
            result.total_data_points = self._session_data_points.get(device_type, 0)
            result.total_bad_data_points = self._session_bad_data.get(device_type, 0)
            result.total_errors = self._session_errors.get(device_type, 0)
            
            # Add metrics from currently active devices
            for device_id, device_info in self._devices.items():
                if device_info["device_type"] != device_type:
                    continue
                
                device_ref = device_info["device_ref"]
                if hasattr(device_ref, "get_metrics"):
                    device_metrics = device_ref.get_metrics()
                    result.active_devices += 1
                    result.total_data_points += device_metrics.get("data_points_sent", 0)
                    result.total_bad_data_points += device_metrics.get("bad_data_points", 0)
                    result.total_errors += device_metrics.get("errors", 0)
                    result.devices.append(device_metrics)
            
            return result
    
    def get_summary(self) -> Dict[str, Any]:
        """Get overall summary of all metrics."""
        with self._lock:
            summary = {
                "uptime_seconds": (datetime.now(timezone.utc) - self._started_at).total_seconds(),
                "total_devices": len(self._devices),
                "by_type": {}
            }
            
            total_data_points = 0
            total_bad_data_points = 0
            total_errors = 0
            
            # Always include all device types, even if no active devices
            for device_type in self.DEVICE_TYPES:
                type_metrics = self.get_metrics_by_type(device_type)
                summary["by_type"][device_type] = type_metrics.to_dict()
                total_data_points += type_metrics.total_data_points
                total_bad_data_points += type_metrics.total_bad_data_points
                total_errors += type_metrics.total_errors
            
            summary["total_data_points"] = total_data_points
            summary["total_bad_data_points"] = total_bad_data_points
            summary["total_errors"] = total_errors
            summary["overall_bad_data_rate"] = round(
                total_bad_data_points / max(total_data_points, 1) * 100, 2
            )
            
            return summary
    
    def get_device_count_by_type(self) -> Dict[str, int]:
        """Get count of devices by type."""
        with self._lock:
            counts = defaultdict(int)
            for device_info in self._devices.values():
                counts[device_info["device_type"]] += 1
            return dict(counts)


# Global metrics collector
_collector: Optional[MetricsCollector] = None


def get_metrics_collector() -> MetricsCollector:
    """Get the global metrics collector."""
    global _collector
    if _collector is None:
        _collector = MetricsCollector()
    return _collector

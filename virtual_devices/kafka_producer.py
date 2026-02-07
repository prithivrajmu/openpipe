"""Kafka producer wrapper for virtual devices"""

import json
import threading
from typing import Dict, Any, Optional
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError


class VirtualDeviceKafkaProducer:
    """
    Thread-safe Kafka producer for virtual device data.
    
    Features:
    - Connection pooling
    - Automatic JSON serialization
    - Topic routing by device type
    - Error handling and reconnection
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topics: Optional[Dict[str, str]] = None
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics or {
            "wearable": "virtual-wearables",
            "restaurant": "virtual-restaurants",
            "gps": "virtual-gps"
        }
        
        self._producer: Optional[KafkaProducer] = None
        self._lock = threading.Lock()
        self._connected = False
        
        # Metrics
        self._messages_sent = 0
        self._errors = 0
    
    def _ensure_connected(self):
        """Ensure producer is connected."""
        if self._producer is not None:
            return
        
        with self._lock:
            if self._producer is not None:
                return
            
            try:
                self._producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=3,
                    max_block_ms=5000
                )
                self._connected = True
                print(f"[KafkaProducer] Connected to {self.bootstrap_servers}")
            except KafkaError as e:
                print(f"[KafkaProducer] Failed to connect: {e}")
                raise
    
    def get_topic(self, device_type: str) -> str:
        """Get the Kafka topic for a device type."""
        return self.topics.get(device_type, f"virtual-{device_type}")
    
    def send(self, device_type: str, data: Dict[str, Any], key: Optional[str] = None):
        """
        Send data to the appropriate Kafka topic.
        
        Args:
            device_type: Type of device (wearable, restaurant, gps)
            data: Data to send
            key: Optional message key (defaults to device_id)
        """
        self._ensure_connected()
        
        topic = self.get_topic(device_type)
        message_key = key or data.get("device_id")
        
        try:
            future = self._producer.send(topic, value=data, key=message_key)
            # Don't block, but track errors
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)
        except Exception as e:
            self._errors += 1
            print(f"[KafkaProducer] Error sending to {topic}: {e}")
    
    def _on_send_success(self, record_metadata):
        """Callback for successful send."""
        with self._lock:
            self._messages_sent += 1
    
    def _on_send_error(self, exc):
        """Callback for send error."""
        with self._lock:
            self._errors += 1
        print(f"[KafkaProducer] Send error: {exc}")
    
    def flush(self, timeout: float = 10.0):
        """Flush pending messages."""
        if self._producer:
            self._producer.flush(timeout=timeout)
    
    def close(self):
        """Close the producer."""
        if self._producer:
            self._producer.flush()
            self._producer.close()
            self._producer = None
            self._connected = False
            print("[KafkaProducer] Closed")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get producer statistics."""
        with self._lock:
            return {
                "connected": self._connected,
                "messages_sent": self._messages_sent,
                "errors": self._errors,
                "bootstrap_servers": self.bootstrap_servers,
                "topics": self.topics
            }
    
    def is_connected(self) -> bool:
        """Check if producer is connected."""
        return self._connected


# Global producer instance
_producer: Optional[VirtualDeviceKafkaProducer] = None


def get_producer() -> VirtualDeviceKafkaProducer:
    """Get the global producer instance."""
    global _producer
    if _producer is None:
        raise RuntimeError("Kafka producer not initialized. Call init_producer() first.")
    return _producer


def init_producer(bootstrap_servers: str, topics: Dict[str, str]) -> VirtualDeviceKafkaProducer:
    """Initialize the global producer."""
    global _producer
    _producer = VirtualDeviceKafkaProducer(bootstrap_servers, topics)
    return _producer

"""Kafka consumer with message enrichment."""

import json
import threading
import time
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import KafkaError


class DataPipelineConsumer:
    """
    Kafka consumer that enriches messages with metadata.
    
    Features:
    - Subscribes to multiple topics
    - Enriches messages with Kafka metadata
    - Calls callback for each message
    - Thread-safe operation
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topics: Optional[List[str]] = None,
        consumer_group: str = "data-pipeline",
        on_message_callback: Optional[Callable[[Dict[str, Any]], None]] = None
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics or [
            "virtual-wearables",
            "virtual-restaurants", 
            "virtual-gps"
        ]
        self.consumer_group = consumer_group
        self.on_message = on_message_callback
        
        self._consumer: Optional[KafkaConsumer] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        
        # Stats
        self._messages_consumed = 0
        self._errors = 0
        self._last_message_time: Optional[datetime] = None
        self._topic_offsets: Dict[str, int] = {}
    
    def start(self):
        """Start consuming messages in background thread."""
        if self._running:
            return
        
        try:
            self._consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.consumer_group,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            
            self._running = True
            self._thread = threading.Thread(target=self._consume_loop, daemon=True)
            self._thread.start()
            
            print(f"[Consumer] Started consuming topics: {self.topics}")
            
        except KafkaError as e:
            self._errors += 1
            print(f"[Consumer] Failed to start: {e}")
            raise
    
    def stop(self):
        """Stop the consumer."""
        self._running = False
        
        if self._thread:
            self._thread.join(timeout=10)
        
        if self._consumer:
            self._consumer.close()
            self._consumer = None
        
        print("[Consumer] Stopped")
    
    def _consume_loop(self):
        """Main consume loop running in background thread."""
        while self._running:
            try:
                # Poll with timeout
                records = self._consumer.poll(timeout_ms=1000)
                
                for topic_partition, messages in records.items():
                    for message in messages:
                        enriched = self._enrich_message(message)
                        
                        if self.on_message:
                            self.on_message(enriched)
                        
                        with self._lock:
                            self._messages_consumed += 1
                            self._last_message_time = datetime.now(timezone.utc)
                            self._topic_offsets[message.topic] = message.offset
                
            except Exception as e:
                with self._lock:
                    self._errors += 1
                print(f"[Consumer] Error in consume loop: {e}")
                time.sleep(1)  # Brief pause before retry
    
    def _enrich_message(self, message) -> Dict[str, Any]:
        """Enrich a Kafka message with metadata."""
        return {
            "kafka_topic": message.topic,
            "kafka_partition": message.partition,
            "kafka_offset": message.offset,
            "ingestion_protocol": "kafka",  # Future: detect HTTP/TCP/MQTT
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "raw_data": message.value
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get consumer statistics."""
        with self._lock:
            return {
                "running": self._running,
                "topics": self.topics,
                "consumer_group": self.consumer_group,
                "messages_consumed": self._messages_consumed,
                "errors": self._errors,
                "last_message": str(self._last_message_time) if self._last_message_time else None,
                "topic_offsets": dict(self._topic_offsets)
            }
    
    def check_health(self) -> Dict[str, Any]:
        """Check consumer health."""
        stats = self.get_stats()
        
        # Consider unhealthy if not running or too many errors
        is_healthy = stats["running"] and stats["errors"] < 100
        
        return {
            "status": "healthy" if is_healthy else "unhealthy",
            **stats
        }


# Global consumer instance
_consumer: Optional[DataPipelineConsumer] = None


def get_consumer() -> DataPipelineConsumer:
    """Get the global consumer instance."""
    global _consumer
    if _consumer is None:
        raise RuntimeError("Consumer not initialized. Call init_consumer() first.")
    return _consumer


def init_consumer(
    bootstrap_servers: str,
    topics: List[str],
    consumer_group: str,
    on_message: Callable[[Dict[str, Any]], None]
) -> DataPipelineConsumer:
    """Initialize the global consumer."""
    global _consumer
    _consumer = DataPipelineConsumer(
        bootstrap_servers=bootstrap_servers,
        topics=topics,
        consumer_group=consumer_group,
        on_message_callback=on_message
    )
    return _consumer

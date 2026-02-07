"""Batch writer for persisting Kafka messages to TimescaleDB."""

import json
import threading
import time
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
from queue import Queue, Empty

from sqlalchemy import text

from .connection import get_db
from .models import topic_to_table_name


class BatchWriter:
    """
    Batches Kafka messages and writes them to TimescaleDB.
    
    Features:
    - Configurable batch size
    - Configurable flush interval
    - Thread-safe queue
    - Automatic flushing
    """
    
    def __init__(
        self,
        batch_size: int = 100,
        flush_interval_seconds: float = 5.0
    ):
        self.batch_size = batch_size
        self.flush_interval = flush_interval_seconds
        
        self._queue: Queue = Queue()
        self._lock = threading.Lock()
        self._running = False
        self._flush_thread: Optional[threading.Thread] = None
        
        # Stats
        self._messages_queued = 0
        self._messages_written = 0
        self._batches_written = 0
        self._errors = 0
        self._last_flush_time: Optional[datetime] = None
    
    def start(self):
        """Start the background flush thread."""
        if self._running:
            return
        
        self._running = True
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()
        print(f"[BatchWriter] Started (batch_size={self.batch_size}, interval={self.flush_interval}s)")
    
    def stop(self):
        """Stop the background flush thread and flush remaining messages."""
        self._running = False
        if self._flush_thread:
            self._flush_thread.join(timeout=10)
        
        # Final flush
        self._flush()
        print("[BatchWriter] Stopped")
    
    def add(self, enriched_message: Dict[str, Any]):
        """Add an enriched message to the queue."""
        self._queue.put(enriched_message)
        with self._lock:
            self._messages_queued += 1
        
        # Check if we should flush based on size
        if self._queue.qsize() >= self.batch_size:
            self._flush()
    
    def _flush_loop(self):
        """Background loop that flushes periodically."""
        while self._running:
            time.sleep(self.flush_interval)
            if not self._queue.empty():
                self._flush()
    
    def _flush(self):
        """Flush all queued messages to the database."""
        messages = []
        
        # Drain the queue
        while True:
            try:
                msg = self._queue.get_nowait()
                messages.append(msg)
            except Empty:
                break
        
        if not messages:
            return
        
        # Group by topic
        by_topic: Dict[str, List[Dict]] = {}
        for msg in messages:
            topic = msg.get("kafka_topic", "unknown")
            if topic not in by_topic:
                by_topic[topic] = []
            by_topic[topic].append(msg)
        
        # Write each group
        db = get_db()
        for topic, topic_messages in by_topic.items():
            table_name = topic_to_table_name(topic)
            self._write_batch(db, table_name, topic_messages)
        
        with self._lock:
            self._messages_written += len(messages)
            self._batches_written += 1
            self._last_flush_time = datetime.now(timezone.utc)
    
    def _write_batch(self, db, table_name: str, messages: List[Dict[str, Any]]):
        """Write a batch of messages to a specific table."""
        if not messages:
            return
        
        try:
            # Build batch insert query
            insert_sql = f"""
                INSERT INTO {table_name} 
                (device_id, kafka_topic, kafka_partition, kafka_offset, 
                 ingestion_protocol, ingested_at, raw_data)
                VALUES (:device_id, :kafka_topic, :kafka_partition, :kafka_offset,
                        :ingestion_protocol, :ingested_at, :raw_data)
            """
            
            # Prepare values
            values = []
            for msg in messages:
                raw_data = msg.get("raw_data", {})
                values.append({
                    "device_id": raw_data.get("device_id", "unknown"),
                    "kafka_topic": msg.get("kafka_topic", ""),
                    "kafka_partition": msg.get("kafka_partition"),
                    "kafka_offset": msg.get("kafka_offset"),
                    "ingestion_protocol": msg.get("ingestion_protocol", "kafka"),
                    "ingested_at": msg.get("ingested_at", datetime.now(timezone.utc)),
                    "raw_data": json.dumps(raw_data)
                })
            
            with db.session() as session:
                for val in values:
                    session.execute(text(insert_sql), val)
            
        except Exception as e:
            with self._lock:
                self._errors += 1
            print(f"[BatchWriter] Error writing to {table_name}: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get writer statistics."""
        with self._lock:
            return {
                "queue_depth": self._queue.qsize(),
                "messages_queued": self._messages_queued,
                "messages_written": self._messages_written,
                "batches_written": self._batches_written,
                "errors": self._errors,
                "last_flush": str(self._last_flush_time) if self._last_flush_time else None,
                "running": self._running
            }
    
    def check_health(self) -> Dict[str, Any]:
        """Check writer health."""
        stats = self.get_stats()
        
        # Consider unhealthy if queue is too deep or too many errors
        is_healthy = (
            stats["queue_depth"] < self.batch_size * 10 and
            stats["errors"] < 100
        )
        
        return {
            "status": "healthy" if is_healthy else "unhealthy",
            **stats
        }

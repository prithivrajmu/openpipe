"""
Standalone Health Metrics Processor using Python threads.

This is a simpler alternative to PyFlink that runs natively in Python,
consuming from Kafka and computing windowed aggregations.
"""

import argparse
import json
import logging
import signal
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field

from kafka import KafkaConsumer

from .config import FlinkConfig
from .sinks import HealthMetricsSink, init_health_metrics_schema


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class WindowBuffer:
    """Buffer for collecting readings within a time window."""
    window_start: datetime
    window_end: datetime
    readings: List[Dict[str, Any]] = field(default_factory=list)


class HealthMetricsProcessor:
    """
    Process health metrics with windowed aggregations.
    
    Windows:
    - Heart Rate: 1-minute tumbling
    - Blood Pressure: 1-minute tumbling
    - Blood Sugar: 10-minute tumbling
    """
    
    def __init__(self, config: FlinkConfig):
        self.config = config
        self.sink = HealthMetricsSink(config)
        
        # Window buffers keyed by user_id
        self._hr_buffers: Dict[str, WindowBuffer] = {}
        self._bp_buffers: Dict[str, WindowBuffer] = {}
        self._sugar_buffers: Dict[str, WindowBuffer] = {}
        
        self._lock = threading.Lock()
        self._running = False
        self._consumer: Optional[KafkaConsumer] = None
        self._consumer_thread: Optional[threading.Thread] = None
        self._window_thread: Optional[threading.Thread] = None
        
        # Stats
        self._messages_processed = 0
        self._alerts_generated = 0
    
    def start(self):
        """Start the processor."""
        logger.info("=" * 60)
        logger.info("  Health Metrics Stream Processor")
        logger.info("=" * 60)
        logger.info(f"Kafka: {self.config.kafka_bootstrap_servers}")
        logger.info(f"Topic: {self.config.kafka_topic}")
        logger.info(f"Windows: HR={self.config.windows.heart_rate_minutes}min, BP={self.config.windows.blood_pressure_minutes}min, Sugar={self.config.windows.blood_sugar_minutes}min")
        logger.info(f"Thresholds: HR>{self.config.thresholds.elevated_heart_rate}, BP≥{self.config.thresholds.elevated_bp_systolic}/{self.config.thresholds.elevated_bp_diastolic}, Sugar≥{self.config.thresholds.elevated_blood_sugar}")
        logger.info("")
        
        # Initialize database schema
        logger.info("Initializing database schema...")
        init_health_metrics_schema(self.config)
        
        # Connect sink
        self.sink.connect()
        
        # Create Kafka consumer
        self._consumer = KafkaConsumer(
            self.config.kafka_topic,
            bootstrap_servers=self.config.kafka_bootstrap_servers,
            group_id=self.config.consumer_group,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        )
        
        self._running = True
        
        # Start consumer thread
        self._consumer_thread = threading.Thread(target=self._consume_loop, daemon=True)
        self._consumer_thread.start()
        
        # Start window flushing thread
        self._window_thread = threading.Thread(target=self._window_flush_loop, daemon=True)
        self._window_thread.start()
        
        logger.info("Processor started. Waiting for messages...")
    
    def stop(self):
        """Stop the processor."""
        logger.info("Shutting down...")
        self._running = False
        
        # Flush remaining windows
        self._flush_all_windows()
        
        if self._consumer_thread:
            self._consumer_thread.join(timeout=5)
        if self._window_thread:
            self._window_thread.join(timeout=5)
        if self._consumer:
            self._consumer.close()
        
        self.sink.close()
        
        logger.info(f"Processed {self._messages_processed} messages, generated {self._alerts_generated} alerts")
        logger.info("Shutdown complete")
    
    def _consume_loop(self):
        """Main consume loop."""
        while self._running:
            try:
                records = self._consumer.poll(timeout_ms=1000)
                
                for topic_partition, messages in records.items():
                    for message in messages:
                        self._process_message(message.value)
                        self._messages_processed += 1
                        
            except Exception as e:
                logger.error(f"Error in consume loop: {e}")
                time.sleep(1)
    
    def _process_message(self, data: Dict[str, Any]):
        """Process a single message and add to appropriate windows."""
        try:
            raw_data = data.get("raw_data", data)
            user_id = raw_data.get("user_id", "unknown")
            now = datetime.now(timezone.utc)
            
            # Extract health metrics
            hr_data = raw_data.get("heart_rate", {})
            heart_rate = hr_data.get("value") if isinstance(hr_data, dict) else None
            
            bp_data = raw_data.get("blood_pressure", {})
            if isinstance(bp_data, dict):
                bp_systolic = bp_data.get("systolic")
                bp_diastolic = bp_data.get("diastolic")
            else:
                bp_systolic = None
                bp_diastolic = None
            
            sugar_data = raw_data.get("blood_sugar", {})
            blood_sugar = sugar_data.get("value") if isinstance(sugar_data, dict) else None
            
            reading = {
                "timestamp": now,
                "user_id": user_id,
                "heart_rate": heart_rate if self._is_valid_reading(heart_rate) else None,
                "bp_systolic": bp_systolic if self._is_valid_reading(bp_systolic) else None,
                "bp_diastolic": bp_diastolic if self._is_valid_reading(bp_diastolic) else None,
                "blood_sugar": blood_sugar if self._is_valid_reading(blood_sugar) else None,
            }
            
            with self._lock:
                self._add_to_window(user_id, reading, now)
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def _is_valid_reading(self, value) -> bool:
        """Check if a reading value is valid."""
        return value is not None and isinstance(value, (int, float)) and value > 0
    
    def _add_to_window(self, user_id: str, reading: Dict[str, Any], now: datetime):
        """Add a reading to appropriate window buffers."""
        # Heart rate window
        if reading["heart_rate"]:
            self._ensure_window(
                self._hr_buffers, user_id, now,
                self.config.windows.heart_rate_minutes
            )
            self._hr_buffers[user_id].readings.append(reading)
        
        # Blood pressure window
        if reading["bp_systolic"] and reading["bp_diastolic"]:
            self._ensure_window(
                self._bp_buffers, user_id, now,
                self.config.windows.blood_pressure_minutes
            )
            self._bp_buffers[user_id].readings.append(reading)
        
        # Blood sugar window
        if reading["blood_sugar"]:
            self._ensure_window(
                self._sugar_buffers, user_id, now,
                self.config.windows.blood_sugar_minutes
            )
            self._sugar_buffers[user_id].readings.append(reading)
    
    def _ensure_window(
        self,
        buffers: Dict[str, WindowBuffer],
        user_id: str,
        now: datetime,
        window_minutes: int
    ):
        """Ensure a window buffer exists for the user."""
        if user_id not in buffers:
            # Create new window aligned to minute boundary
            window_start = now.replace(second=0, microsecond=0)
            window_end = window_start.replace(
                minute=(window_start.minute // window_minutes + 1) * window_minutes % 60
            )
            if window_end <= window_start:
                window_end = window_end.replace(hour=window_start.hour + 1)
            
            buffers[user_id] = WindowBuffer(
                window_start=window_start,
                window_end=window_end,
            )
    
    def _window_flush_loop(self):
        """Periodically check and flush completed windows."""
        while self._running:
            time.sleep(5)  # Check every 5 seconds
            
            now = datetime.now(timezone.utc)
            
            with self._lock:
                # Flush heart rate windows
                self._check_and_flush_windows(
                    self._hr_buffers, now,
                    self.config.windows.heart_rate_minutes,
                    self._aggregate_heart_rate
                )
                
                # Flush blood pressure windows
                self._check_and_flush_windows(
                    self._bp_buffers, now,
                    self.config.windows.blood_pressure_minutes,
                    self._aggregate_blood_pressure
                )
                
                # Flush blood sugar windows
                self._check_and_flush_windows(
                    self._sugar_buffers, now,
                    self.config.windows.blood_sugar_minutes,
                    self._aggregate_blood_sugar
                )
    
    def _check_and_flush_windows(
        self,
        buffers: Dict[str, WindowBuffer],
        now: datetime,
        window_minutes: int,
        aggregate_fn
    ):
        """Check and flush completed windows."""
        users_to_flush = []
        
        for user_id, buffer in buffers.items():
            if now >= buffer.window_end and buffer.readings:
                users_to_flush.append(user_id)
        
        for user_id in users_to_flush:
            buffer = buffers[user_id]
            result = aggregate_fn(user_id, buffer)
            
            if result:
                if "alert" in result:
                    self._alerts_generated += 1
                    logger.warning(
                        f"🚨 ALERT: {result['alert']['type']} for {user_id} - "
                        f"severity: {result['alert']['severity']}"
                    )
            
            # Reset buffer for next window
            del buffers[user_id]
    
    def _flush_all_windows(self):
        """Flush all remaining windows on shutdown."""
        with self._lock:
            for user_id, buffer in list(self._hr_buffers.items()):
                if buffer.readings:
                    self._aggregate_heart_rate(user_id, buffer)
            
            for user_id, buffer in list(self._bp_buffers.items()):
                if buffer.readings:
                    self._aggregate_blood_pressure(user_id, buffer)
            
            for user_id, buffer in list(self._sugar_buffers.items()):
                if buffer.readings:
                    self._aggregate_blood_sugar(user_id, buffer)
    
    def _aggregate_heart_rate(self, user_id: str, buffer: WindowBuffer) -> Optional[Dict]:
        """Aggregate heart rate readings and check for alerts."""
        readings = [r["heart_rate"] for r in buffer.readings if r["heart_rate"]]
        if not readings:
            return None
        
        avg_hr = sum(readings) / len(readings)
        result = {
            "type": "heart_rate_1min",
            "window_start": buffer.window_start.isoformat(),
            "window_end": buffer.window_end.isoformat(),
            "user_id": user_id,
            "avg_heart_rate": round(avg_hr, 2),
            "min_heart_rate": min(readings),
            "max_heart_rate": max(readings),
            "reading_count": len(readings),
        }
        
        # Check threshold
        if avg_hr > self.config.thresholds.elevated_heart_rate:
            result["alert"] = {
                "type": "elevated_heart_rate",
                "value": round(avg_hr, 2),
                "threshold": self.config.thresholds.elevated_heart_rate,
                "severity": "warning" if avg_hr < 120 else "critical",
            }
        
        # Write to database
        self.sink.write_heart_rate_metric(result)
        
        logger.info(
            f"HR[1min] {user_id}: avg={result['avg_heart_rate']}, "
            f"min={result['min_heart_rate']}, max={result['max_heart_rate']}, "
            f"count={result['reading_count']}"
        )
        
        return result
    
    def _aggregate_blood_pressure(self, user_id: str, buffer: WindowBuffer) -> Optional[Dict]:
        """Aggregate blood pressure readings and check for alerts."""
        systolic = [r["bp_systolic"] for r in buffer.readings if r["bp_systolic"]]
        diastolic = [r["bp_diastolic"] for r in buffer.readings if r["bp_diastolic"]]
        
        if not systolic or not diastolic:
            return None
        
        avg_sys = sum(systolic) / len(systolic)
        avg_dia = sum(diastolic) / len(diastolic)
        
        result = {
            "type": "blood_pressure_1min",
            "window_start": buffer.window_start.isoformat(),
            "window_end": buffer.window_end.isoformat(),
            "user_id": user_id,
            "avg_bp_systolic": round(avg_sys, 2),
            "avg_bp_diastolic": round(avg_dia, 2),
            "reading_count": len(systolic),
        }
        
        # Check thresholds
        if (avg_sys >= self.config.thresholds.elevated_bp_systolic or 
            avg_dia >= self.config.thresholds.elevated_bp_diastolic):
            result["alert"] = {
                "type": "elevated_blood_pressure",
                "systolic": round(avg_sys, 2),
                "diastolic": round(avg_dia, 2),
                "thresholds": f"{self.config.thresholds.elevated_bp_systolic}/{self.config.thresholds.elevated_bp_diastolic}",
                "severity": "warning" if avg_sys < 160 else "critical",
            }
        
        # Write to database
        self.sink.write_blood_pressure_metric(result)
        
        logger.info(
            f"BP[1min] {user_id}: avg={result['avg_bp_systolic']}/{result['avg_bp_diastolic']}, "
            f"count={result['reading_count']}"
        )
        
        return result
    
    def _aggregate_blood_sugar(self, user_id: str, buffer: WindowBuffer) -> Optional[Dict]:
        """Aggregate blood sugar readings and check for alerts."""
        readings = [r["blood_sugar"] for r in buffer.readings if r["blood_sugar"]]
        if not readings:
            return None
        
        avg_sugar = sum(readings) / len(readings)
        
        result = {
            "type": "blood_sugar_10min",
            "window_start": buffer.window_start.isoformat(),
            "window_end": buffer.window_end.isoformat(),
            "user_id": user_id,
            "avg_blood_sugar": round(avg_sugar, 2),
            "min_blood_sugar": min(readings),
            "max_blood_sugar": max(readings),
            "reading_count": len(readings),
        }
        
        # Check threshold
        if avg_sugar >= self.config.thresholds.elevated_blood_sugar:
            result["alert"] = {
                "type": "elevated_blood_sugar",
                "value": round(avg_sugar, 2),
                "threshold": self.config.thresholds.elevated_blood_sugar,
                "severity": "warning" if avg_sugar < 250 else "critical",
            }
        
        # Write to database
        self.sink.write_blood_sugar_metric(result)
        
        logger.info(
            f"Sugar[10min] {user_id}: avg={result['avg_blood_sugar']}, "
            f"min={result['min_blood_sugar']}, max={result['max_blood_sugar']}, "
            f"count={result['reading_count']}"
        )
        
        return result


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Health Metrics Stream Processor")
    parser.add_argument(
        "--config", "-c",
        default="config/settings.yaml",
        help="Path to configuration file"
    )
    args = parser.parse_args()
    
    config = FlinkConfig.from_yaml(args.config)
    processor = HealthMetricsProcessor(config)
    
    # Setup signal handlers
    def handle_signal(signum, frame):
        processor.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    
    try:
        processor.start()
        # Keep main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        processor.stop()


if __name__ == "__main__":
    main()

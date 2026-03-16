"""
Flink Health Metrics Processor – CLI entry point.

Supports two modes:
- **flink** (default): Submits a real PyFlink job that runs inside the Flink
  cluster.  Windowed aggregations, keying, and sinks are all managed by Flink.
- **standalone**: Legacy Python-threads processor for environments without a
  Flink cluster.  Kept as a fallback.

Usage:
    # Submit PyFlink job (default)
    uv run python -m flink_processor.main --config config/settings.yaml

    # Run standalone (no Flink cluster needed)
    uv run python -m flink_processor.main --config config/settings.yaml --standalone
"""

import argparse
import json
import logging
import signal
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
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


# ═══════════════════════════════════════════════════════════════════════════
# Standalone (legacy) processor — kept as --standalone fallback
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class WindowBuffer:
    """Buffer for collecting readings within a time window."""
    window_start: datetime
    window_end: datetime
    readings: List[Dict[str, Any]] = field(default_factory=list)


class StandaloneHealthProcessor:
    """
    Process health metrics with windowed aggregations using Python threads.

    This is the legacy processor that does NOT use Flink.  It is kept here
    so that you can still run the pipeline without a Flink cluster.

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
        logger.info("  Health Metrics Processor (standalone / no Flink)")
        logger.info("=" * 60)
        logger.info(f"Kafka: {self.config.kafka_bootstrap_servers}")
        logger.info(f"Topic: {self.config.kafka_topic}")
        logger.info(
            f"Windows: HR={self.config.windows.heart_rate_minutes}min, "
            f"BP={self.config.windows.blood_pressure_minutes}min, "
            f"Sugar={self.config.windows.blood_sugar_minutes}min"
        )

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
        self._consumer_thread = threading.Thread(
            target=self._consume_loop, daemon=True
        )
        self._consumer_thread.start()

        # Start window flushing thread
        self._window_thread = threading.Thread(
            target=self._window_flush_loop, daemon=True
        )
        self._window_thread.start()

        logger.info("Standalone processor started. Waiting for messages...")

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

        logger.info(
            f"Processed {self._messages_processed} messages, "
            f"generated {self._alerts_generated} alerts"
        )
        logger.info("Shutdown complete")

    # ── Internal loops ─────────────────────────────────────────────────

    def _consume_loop(self):
        while self._running:
            try:
                records = self._consumer.poll(timeout_ms=1000)
                for tp, messages in records.items():
                    for message in messages:
                        self._process_message(message.value)
                        self._messages_processed += 1
            except Exception as e:
                logger.error(f"Error in consume loop: {e}")
                time.sleep(1)

    def _process_message(self, data: Dict[str, Any]):
        try:
            raw_data = data.get("raw_data", data)
            user_id = raw_data.get("user_id", "unknown")
            now = datetime.now(timezone.utc)

            hr_data = raw_data.get("heart_rate", {})
            heart_rate = hr_data.get("value") if isinstance(hr_data, dict) else None

            bp_data = raw_data.get("blood_pressure", {})
            if isinstance(bp_data, dict):
                bp_systolic = bp_data.get("systolic")
                bp_diastolic = bp_data.get("diastolic")
            else:
                bp_systolic = bp_diastolic = None

            sugar_data = raw_data.get("blood_sugar", {})
            blood_sugar = sugar_data.get("value") if isinstance(sugar_data, dict) else None

            reading = {
                "timestamp": now,
                "user_id": user_id,
                "heart_rate": heart_rate if self._valid(heart_rate) else None,
                "bp_systolic": bp_systolic if self._valid(bp_systolic) else None,
                "bp_diastolic": bp_diastolic if self._valid(bp_diastolic) else None,
                "blood_sugar": blood_sugar if self._valid(blood_sugar) else None,
            }

            with self._lock:
                self._add_to_window(user_id, reading, now)

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    @staticmethod
    def _valid(value) -> bool:
        return value is not None and isinstance(value, (int, float)) and value > 0

    def _add_to_window(self, user_id, reading, now):
        if reading["heart_rate"]:
            self._ensure_window(self._hr_buffers, user_id, now,
                                self.config.windows.heart_rate_minutes)
            self._hr_buffers[user_id].readings.append(reading)

        if reading["bp_systolic"] and reading["bp_diastolic"]:
            self._ensure_window(self._bp_buffers, user_id, now,
                                self.config.windows.blood_pressure_minutes)
            self._bp_buffers[user_id].readings.append(reading)

        if reading["blood_sugar"]:
            self._ensure_window(self._sugar_buffers, user_id, now,
                                self.config.windows.blood_sugar_minutes)
            self._sugar_buffers[user_id].readings.append(reading)

    def _ensure_window(self, buffers, user_id, now, window_minutes):
        if user_id not in buffers:
            ws = now.replace(second=0, microsecond=0)
            we = ws + timedelta(minutes=window_minutes)
            buffers[user_id] = WindowBuffer(window_start=ws, window_end=we)

    def _window_flush_loop(self):
        while self._running:
            time.sleep(5)
            now = datetime.now(timezone.utc)
            with self._lock:
                self._check_flush(self._hr_buffers, now,
                                  self.config.windows.heart_rate_minutes,
                                  self._agg_hr)
                self._check_flush(self._bp_buffers, now,
                                  self.config.windows.blood_pressure_minutes,
                                  self._agg_bp)
                self._check_flush(self._sugar_buffers, now,
                                  self.config.windows.blood_sugar_minutes,
                                  self._agg_sugar)

    def _check_flush(self, buffers, now, window_minutes, agg_fn):
        to_flush = [uid for uid, buf in buffers.items()
                    if now >= buf.window_end and buf.readings]
        for uid in to_flush:
            result = agg_fn(uid, buffers[uid])
            if result and "alert" in result:
                self._alerts_generated += 1
                logger.warning(
                    f"🚨 ALERT: {result['alert']['type']} for {uid}")
            del buffers[uid]

    def _flush_all_windows(self):
        with self._lock:
            for uid, buf in list(self._hr_buffers.items()):
                if buf.readings:
                    self._agg_hr(uid, buf)
            for uid, buf in list(self._bp_buffers.items()):
                if buf.readings:
                    self._agg_bp(uid, buf)
            for uid, buf in list(self._sugar_buffers.items()):
                if buf.readings:
                    self._agg_sugar(uid, buf)

    # ── Aggregators ────────────────────────────────────────────────────

    def _agg_hr(self, user_id, buf):
        vals = [r["heart_rate"] for r in buf.readings if r["heart_rate"]]
        if not vals:
            return None
        avg = sum(vals) / len(vals)
        result = {
            "type": "heart_rate_1min",
            "window_start": buf.window_start.isoformat(),
            "window_end": buf.window_end.isoformat(),
            "user_id": user_id,
            "avg_heart_rate": round(avg, 2),
            "min_heart_rate": min(vals),
            "max_heart_rate": max(vals),
            "reading_count": len(vals),
        }
        if avg > self.config.thresholds.elevated_heart_rate:
            result["alert"] = {
                "type": "elevated_heart_rate", "value": round(avg, 2),
                "threshold": self.config.thresholds.elevated_heart_rate,
                "severity": "warning" if avg < 120 else "critical",
            }
        self.sink.write_heart_rate_metric(result)
        logger.info(f"HR[1min] {user_id}: avg={result['avg_heart_rate']}")
        return result

    def _agg_bp(self, user_id, buf):
        sys_vals = [r["bp_systolic"] for r in buf.readings if r["bp_systolic"]]
        dia_vals = [r["bp_diastolic"] for r in buf.readings if r["bp_diastolic"]]
        if not sys_vals or not dia_vals:
            return None
        avg_s, avg_d = sum(sys_vals)/len(sys_vals), sum(dia_vals)/len(dia_vals)
        result = {
            "type": "blood_pressure_1min",
            "window_start": buf.window_start.isoformat(),
            "window_end": buf.window_end.isoformat(),
            "user_id": user_id,
            "avg_bp_systolic": round(avg_s, 2),
            "avg_bp_diastolic": round(avg_d, 2),
            "reading_count": len(sys_vals),
        }
        t = self.config.thresholds
        if avg_s >= t.elevated_bp_systolic or avg_d >= t.elevated_bp_diastolic:
            result["alert"] = {
                "type": "elevated_blood_pressure",
                "systolic": round(avg_s, 2), "diastolic": round(avg_d, 2),
                "thresholds": f"{t.elevated_bp_systolic}/{t.elevated_bp_diastolic}",
                "severity": "warning" if avg_s < 160 else "critical",
            }
        self.sink.write_blood_pressure_metric(result)
        logger.info(f"BP[1min] {user_id}: avg={result['avg_bp_systolic']}/{result['avg_bp_diastolic']}")
        return result

    def _agg_sugar(self, user_id, buf):
        vals = [r["blood_sugar"] for r in buf.readings if r["blood_sugar"]]
        if not vals:
            return None
        avg = sum(vals) / len(vals)
        result = {
            "type": "blood_sugar_10min",
            "window_start": buf.window_start.isoformat(),
            "window_end": buf.window_end.isoformat(),
            "user_id": user_id,
            "avg_blood_sugar": round(avg, 2),
            "min_blood_sugar": min(vals),
            "max_blood_sugar": max(vals),
            "reading_count": len(vals),
        }
        if avg >= self.config.thresholds.elevated_blood_sugar:
            result["alert"] = {
                "type": "elevated_blood_sugar", "value": round(avg, 2),
                "threshold": self.config.thresholds.elevated_blood_sugar,
                "severity": "warning" if avg < 250 else "critical",
            }
        self.sink.write_blood_sugar_metric(result)
        logger.info(f"Sugar[10min] {user_id}: avg={result['avg_blood_sugar']}")
        return result


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════

def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Health Metrics Stream Processor"
    )
    parser.add_argument(
        "--config", "-c",
        default="config/settings.yaml",
        help="Path to configuration file",
    )
    parser.add_argument(
        "--standalone",
        action="store_true",
        help="Run the legacy standalone Python processor (no Flink cluster needed)",
    )
    args = parser.parse_args()

    config = FlinkConfig.from_yaml(args.config)

    if args.standalone:
        # ── Legacy standalone mode ──
        processor = StandaloneHealthProcessor(config)

        def handle_signal(signum, frame):
            processor.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

        try:
            processor.start()
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            processor.stop()
    else:
        # ── PyFlink mode (default) ──
        logger.info("Launching PyFlink stream processing job...")
        from .health_stream_job import run_health_stream_job

        # Initialize DB schema before submitting job
        init_health_metrics_schema(config)

        run_health_stream_job(config)


if __name__ == "__main__":
    main()

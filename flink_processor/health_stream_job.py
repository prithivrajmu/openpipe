"""
Health Metrics Stream Processor using PyFlink.

Implements windowed aggregations with JDBC sinks to TimescaleDB:
- Heart rate: 1-minute tumbling window with elevated HR alerts
- Blood pressure: 1-minute tumbling window with elevated BP alerts
- Blood sugar: 10-minute tumbling window with elevated sugar alerts
"""

import argparse
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, Tuple, Iterable
from dataclasses import dataclass

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Time, Configuration
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.functions import (
    ProcessWindowFunction,
    MapFunction,
    SinkFunction,
    RuntimeContext,
)
from pyflink.common.typeinfo import Types

from .config import FlinkConfig


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Message parser
# ---------------------------------------------------------------------------

class HealthMessageParser(MapFunction):
    """Parse incoming JSON messages to a typed tuple."""

    def map(self, value: str) -> Tuple:
        try:
            data = json.loads(value)
            raw_data = data.get("raw_data", data)

            # Extract heart rate
            hr_data = raw_data.get("heart_rate", {})
            heart_rate = hr_data.get("value") if isinstance(hr_data, dict) else None

            # Extract blood pressure
            bp_data = raw_data.get("blood_pressure", {})
            if isinstance(bp_data, dict):
                bp_systolic = bp_data.get("systolic")
                bp_diastolic = bp_data.get("diastolic")
            else:
                bp_systolic = None
                bp_diastolic = None

            # Extract blood sugar
            sugar_data = raw_data.get("blood_sugar", {})
            blood_sugar = sugar_data.get("value") if isinstance(sugar_data, dict) else None

            # Validate values (positive integers only)
            is_valid = all([
                heart_rate is None or (isinstance(heart_rate, (int, float)) and heart_rate > 0),
                bp_systolic is None or (isinstance(bp_systolic, (int, float)) and bp_systolic > 0),
                bp_diastolic is None or (isinstance(bp_diastolic, (int, float)) and bp_diastolic > 0),
                blood_sugar is None or (isinstance(blood_sugar, (int, float)) and blood_sugar > 0),
            ])

            timestamp = data.get("ingested_at", datetime.now().isoformat())
            user_id = raw_data.get("user_id", "unknown")

            return (
                timestamp,
                user_id,
                int(heart_rate) if is_valid and heart_rate else -1,
                int(bp_systolic) if is_valid and bp_systolic else -1,
                int(bp_diastolic) if is_valid and bp_diastolic else -1,
                int(blood_sugar) if is_valid and blood_sugar else -1,
                1 if is_valid else 0,  # valid flag
            )

        except Exception as e:
            logger.error(f"Failed to parse message: {e}")
            return (
                datetime.now().isoformat(),
                "unknown",
                -1, -1, -1, -1, 0
            )


# ---------------------------------------------------------------------------
# Window processors
# ---------------------------------------------------------------------------

class HeartRateWindowProcessor(ProcessWindowFunction):
    """Process 1-minute heart rate windows and generate alerts."""

    def __init__(self, threshold: int):
        super().__init__()
        self.threshold = threshold

    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context,
        elements: Iterable,
    ) -> Iterable[str]:
        """Aggregate heart rate readings and check for elevated values."""
        readings = [e for e in elements if e[2] > 0]  # heart_rate > 0

        if not readings:
            return

        hr_values = [r[2] for r in readings]
        avg_hr = sum(hr_values) / len(hr_values)
        min_hr = min(hr_values)
        max_hr = max(hr_values)

        window_start = datetime.fromtimestamp(context.window().start / 1000)
        window_end = datetime.fromtimestamp(context.window().end / 1000)

        result = {
            "type": "heart_rate_1min",
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "user_id": key,
            "avg_heart_rate": round(avg_hr, 2),
            "min_heart_rate": min_hr,
            "max_heart_rate": max_hr,
            "reading_count": len(readings),
        }

        # Check for elevated heart rate
        if avg_hr > self.threshold:
            result["alert"] = {
                "type": "elevated_heart_rate",
                "value": round(avg_hr, 2),
                "threshold": self.threshold,
                "severity": "warning" if avg_hr < 120 else "critical",
            }

        yield json.dumps(result)


class BloodPressureWindowProcessor(ProcessWindowFunction):
    """Process 1-minute blood pressure windows and generate alerts."""

    def __init__(self, systolic_threshold: int, diastolic_threshold: int):
        super().__init__()
        self.systolic_threshold = systolic_threshold
        self.diastolic_threshold = diastolic_threshold

    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context,
        elements: Iterable,
    ) -> Iterable[str]:
        """Aggregate BP readings and check for elevated values."""
        readings = [e for e in elements if e[3] > 0 and e[4] > 0]

        if not readings:
            return

        systolic_values = [r[3] for r in readings]
        diastolic_values = [r[4] for r in readings]

        avg_systolic = sum(systolic_values) / len(systolic_values)
        avg_diastolic = sum(diastolic_values) / len(diastolic_values)

        window_start = datetime.fromtimestamp(context.window().start / 1000)
        window_end = datetime.fromtimestamp(context.window().end / 1000)

        result = {
            "type": "blood_pressure_1min",
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "user_id": key,
            "avg_bp_systolic": round(avg_systolic, 2),
            "avg_bp_diastolic": round(avg_diastolic, 2),
            "reading_count": len(readings),
        }

        # Check for elevated blood pressure
        if avg_systolic >= self.systolic_threshold or avg_diastolic >= self.diastolic_threshold:
            result["alert"] = {
                "type": "elevated_blood_pressure",
                "systolic": round(avg_systolic, 2),
                "diastolic": round(avg_diastolic, 2),
                "thresholds": f"{self.systolic_threshold}/{self.diastolic_threshold}",
                "severity": "warning" if avg_systolic < 160 else "critical",
            }

        yield json.dumps(result)


class BloodSugarWindowProcessor(ProcessWindowFunction):
    """Process 10-minute blood sugar windows and generate alerts."""

    def __init__(self, threshold: int):
        super().__init__()
        self.threshold = threshold

    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context,
        elements: Iterable,
    ) -> Iterable[str]:
        """Aggregate blood sugar readings and check for elevated values."""
        readings = [e for e in elements if e[5] > 0]  # blood_sugar > 0

        if not readings:
            return

        sugar_values = [r[5] for r in readings]
        avg_sugar = sum(sugar_values) / len(sugar_values)
        min_sugar = min(sugar_values)
        max_sugar = max(sugar_values)

        window_start = datetime.fromtimestamp(context.window().start / 1000)
        window_end = datetime.fromtimestamp(context.window().end / 1000)

        result = {
            "type": "blood_sugar_10min",
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "user_id": key,
            "avg_blood_sugar": round(avg_sugar, 2),
            "min_blood_sugar": min_sugar,
            "max_blood_sugar": max_sugar,
            "reading_count": len(readings),
        }

        # Check for elevated blood sugar
        if avg_sugar >= self.threshold:
            result["alert"] = {
                "type": "elevated_blood_sugar",
                "value": round(avg_sugar, 2),
                "threshold": self.threshold,
                "severity": "warning" if avg_sugar < 250 else "critical",
            }

        yield json.dumps(result)


# ---------------------------------------------------------------------------
# Database sink (runs inside Flink Python UDF process)
# ---------------------------------------------------------------------------

class HealthMetricsFlinkSink(SinkFunction):
    """
    Flink SinkFunction that writes aggregated health metrics to TimescaleDB.

    This sink runs inside the Flink TaskManager Python worker process.
    It manages its own database connection lifecycle.
    """

    def __init__(self, db_host: str, db_port: int, db_name: str,
                 db_user: str, db_password: str):
        super().__init__()
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self._conn = None

    def _get_connection(self):
        """Lazy-initialize database connection."""
        if self._conn is None or self._conn.closed:
            import psycopg2
            self._conn = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password,
            )
            self._conn.autocommit = True
        return self._conn

    def invoke(self, value: str, context):
        """Process each result and write to the appropriate table."""
        try:
            result = json.loads(value)
            metric_type = result.get("type", "")

            conn = self._get_connection()
            cur = conn.cursor()

            if metric_type == "heart_rate_1min":
                self._write_heart_rate(cur, result)
            elif metric_type == "blood_pressure_1min":
                self._write_blood_pressure(cur, result)
            elif metric_type == "blood_sugar_10min":
                self._write_blood_sugar(cur, result)

            # Write alert if present
            if "alert" in result:
                self._write_alert(cur, result)

            cur.close()
            logger.info(f"Wrote {metric_type} for {result.get('user_id')}")

        except Exception as e:
            logger.error(f"Failed to write metric: {e}")

    def _write_heart_rate(self, cur, result: Dict[str, Any]) -> None:
        cur.execute(
            """INSERT INTO health_metrics_1min
               (time, user_id, metric_type, window_start, window_end,
                avg_value, min_value, max_value, reading_count)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT DO NOTHING""",
            (
                datetime.fromisoformat(result["window_end"]),
                result["user_id"],
                "heart_rate",
                datetime.fromisoformat(result["window_start"]),
                datetime.fromisoformat(result["window_end"]),
                result["avg_heart_rate"],
                result["min_heart_rate"],
                result["max_heart_rate"],
                result["reading_count"],
            ),
        )

    def _write_blood_pressure(self, cur, result: Dict[str, Any]) -> None:
        cur.execute(
            """INSERT INTO health_metrics_1min
               (time, user_id, metric_type, window_start, window_end,
                avg_value, min_value, max_value, reading_count)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT DO NOTHING""",
            (
                datetime.fromisoformat(result["window_end"]),
                result["user_id"],
                "blood_pressure",
                datetime.fromisoformat(result["window_start"]),
                datetime.fromisoformat(result["window_end"]),
                result["avg_bp_systolic"],
                result.get("avg_bp_diastolic", 0),
                result.get("avg_bp_systolic", 0),
                result["reading_count"],
            ),
        )

    def _write_blood_sugar(self, cur, result: Dict[str, Any]) -> None:
        cur.execute(
            """INSERT INTO health_metrics_10min
               (time, user_id, metric_type, window_start, window_end,
                avg_value, min_value, max_value, reading_count)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT DO NOTHING""",
            (
                datetime.fromisoformat(result["window_end"]),
                result["user_id"],
                "blood_sugar",
                datetime.fromisoformat(result["window_start"]),
                datetime.fromisoformat(result["window_end"]),
                result["avg_blood_sugar"],
                result["min_blood_sugar"],
                result["max_blood_sugar"],
                result["reading_count"],
            ),
        )

    def _write_alert(self, cur, result: Dict[str, Any]) -> None:
        alert = result["alert"]
        cur.execute(
            """INSERT INTO health_alerts
               (time, user_id, alert_type, severity, metric_value,
                threshold_value, window_start, window_end)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT DO NOTHING""",
            (
                datetime.fromisoformat(result["window_end"]),
                result["user_id"],
                alert["type"],
                alert["severity"],
                alert.get("value", alert.get("systolic", 0)),
                alert.get("threshold", 0),
                datetime.fromisoformat(result["window_start"]),
                datetime.fromisoformat(result["window_end"]),
            ),
        )


# ---------------------------------------------------------------------------
# Kafka source builder
# ---------------------------------------------------------------------------

def create_kafka_source(config: FlinkConfig) -> KafkaSource:
    """Create Kafka source connector."""
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(config.kafka_bootstrap_servers)
        .set_topics(config.kafka_topic)
        .set_group_id(config.consumer_group)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


# ---------------------------------------------------------------------------
# Job definition
# ---------------------------------------------------------------------------

def run_health_stream_job(config: FlinkConfig):
    """
    Main entry point for the Flink stream processing job.

    Builds the PyFlink DataStream DAG:
      Kafka Source → Parse → Key By user_id →
        ├─ Heart Rate 1-min Window  →  DB Sink
        ├─ Blood Pressure 1-min Window → DB Sink
        └─ Blood Sugar 10-min Window → DB Sink
    """
    logger.info("=" * 60)
    logger.info("  Health Metrics Stream Processor (PyFlink)")
    logger.info("=" * 60)
    logger.info(f"Kafka: {config.kafka_bootstrap_servers} / {config.kafka_topic}")
    logger.info(f"Database: {config.db_host}:{config.db_port}/{config.db_name}")
    logger.info(
        f"Windows: HR={config.windows.heart_rate_minutes}min, "
        f"BP={config.windows.blood_pressure_minutes}min, "
        f"Sugar={config.windows.blood_sugar_minutes}min"
    )
    logger.info(
        f"Thresholds: HR>{config.thresholds.elevated_heart_rate}, "
        f"BP≥{config.thresholds.elevated_bp_systolic}/{config.thresholds.elevated_bp_diastolic}, "
        f"Sugar≥{config.thresholds.elevated_blood_sugar}"
    )
    logger.info(f"Execution mode: {config.execution_mode}")

    # Create execution environment
    flink_config = Configuration()

    if config.execution_mode == "remote":
        flink_config.set_string("rest.address", config.jobmanager_host)
        flink_config.set_string("rest.port", str(config.jobmanager_port))

    env = StreamExecutionEnvironment.get_execution_environment(flink_config)
    env.set_parallelism(1)

    # Add Kafka + JDBC connector JARs
    jar_dir = "/opt/flink/lib/extra"
    try:
        env.add_jars(
            f"file://{jar_dir}/flink-connector-kafka-3.1.0-1.18.jar",
            f"file://{jar_dir}/kafka-clients-3.7.0.jar",
            f"file://{jar_dir}/flink-connector-jdbc-3.1.2-1.18.jar",
            f"file://{jar_dir}/postgresql-42.7.1.jar",
        )
        logger.info(f"Added JARs from {jar_dir}")
    except Exception as e:
        logger.warning(f"Could not add JARs from {jar_dir}: {e}")
        logger.info("Trying local JAR paths...")
        # Fallback for local development
        try:
            import glob
            import os
            flink_home = os.environ.get("FLINK_HOME", "")
            if flink_home:
                jars = glob.glob(f"{flink_home}/lib/*.jar")
                if jars:
                    env.add_jars(*[f"file://{j}" for j in jars])
                    logger.info(f"Added {len(jars)} JARs from FLINK_HOME")
        except Exception as e2:
            logger.warning(f"Could not add local JARs: {e2}")

    # Create Kafka source
    kafka_source = create_kafka_source(config)

    # Create data stream from Kafka
    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.for_monotonous_timestamps(),
        "Wearables Kafka Source"
    )

    # Parse messages into typed tuples
    parsed_stream = stream.map(
        HealthMessageParser(),
        output_type=Types.TUPLE([
            Types.STRING(),  # timestamp
            Types.STRING(),  # user_id
            Types.INT(),     # heart_rate
            Types.INT(),     # bp_systolic
            Types.INT(),     # bp_diastolic
            Types.INT(),     # blood_sugar
            Types.INT(),     # valid flag
        ])
    )

    # Key by user_id (index 1)
    keyed_stream = parsed_stream.key_by(lambda x: x[1])

    # Create the shared DB sink
    db_sink = HealthMetricsFlinkSink(
        db_host=config.db_host,
        db_port=config.db_port,
        db_name=config.db_name,
        db_user=config.db_user,
        db_password=config.db_password,
    )

    # ── Heart rate aggregation (1-minute window) ──
    hr_window_ms = config.windows.heart_rate_minutes * 60 * 1000
    heart_rate_stream = (
        keyed_stream
        .window(TumblingProcessingTimeWindows.of(Time.milliseconds(hr_window_ms)))
        .process(
            HeartRateWindowProcessor(config.thresholds.elevated_heart_rate),
            output_type=Types.STRING(),
        )
    )

    # ── Blood pressure aggregation (1-minute window) ──
    bp_window_ms = config.windows.blood_pressure_minutes * 60 * 1000
    blood_pressure_stream = (
        keyed_stream
        .window(TumblingProcessingTimeWindows.of(Time.milliseconds(bp_window_ms)))
        .process(
            BloodPressureWindowProcessor(
                config.thresholds.elevated_bp_systolic,
                config.thresholds.elevated_bp_diastolic,
            ),
            output_type=Types.STRING(),
        )
    )

    # ── Blood sugar aggregation (10-minute window) ──
    sugar_window_ms = config.windows.blood_sugar_minutes * 60 * 1000
    blood_sugar_stream = (
        keyed_stream
        .window(TumblingProcessingTimeWindows.of(Time.milliseconds(sugar_window_ms)))
        .process(
            BloodSugarWindowProcessor(config.thresholds.elevated_blood_sugar),
            output_type=Types.STRING(),
        )
    )

    # ── Sinks: write aggregated results to TimescaleDB ──
    heart_rate_stream.add_sink(db_sink).name("HR → TimescaleDB")
    blood_pressure_stream.add_sink(db_sink).name("BP → TimescaleDB")
    blood_sugar_stream.add_sink(db_sink).name("Sugar → TimescaleDB")

    # Also print for debugging / monitoring
    heart_rate_stream.print().name("HR Debug Output")
    blood_pressure_stream.print().name("BP Debug Output")
    blood_sugar_stream.print().name("Sugar Debug Output")

    # Execute the job
    logger.info("Submitting job to Flink...")
    env.execute("Health Metrics Stream Processor")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Health Metrics Flink Stream Processor")
    parser.add_argument(
        "--config", "-c",
        default="config/settings.yaml",
        help="Path to configuration file"
    )
    args = parser.parse_args()

    config = FlinkConfig.from_yaml(args.config)
    run_health_stream_job(config)


if __name__ == "__main__":
    main()

"""Database sinks for health metrics stream processor results.

Two sink implementations:
1. HealthMetricsSink  – used by the standalone (--standalone) processor.
2. HealthMetricsFlinkSink  – Flink SinkFunction, lives in health_stream_job.py.

Both write to the same set of TimescaleDB tables created by
init_health_metrics_schema().
"""

import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values

from .config import FlinkConfig


logger = logging.getLogger(__name__)


class HealthMetricsSink:
    """
    Sink for writing windowed health metrics to TimescaleDB.

    Used by the standalone Python-threads processor.
    Writes to tables:
    - health_metrics_1min: Heart rate and BP aggregations
    - health_metrics_10min: Blood sugar aggregations
    - health_alerts: Elevated reading alerts
    """

    def __init__(self, config: FlinkConfig):
        self.config = config
        self._conn: Optional[psycopg2.extensions.connection] = None

    def connect(self):
        """Establish database connection."""
        self._conn = psycopg2.connect(
            host=self.config.db_host,
            port=self.config.db_port,
            dbname=self.config.db_name,
            user=self.config.db_user,
            password=self.config.db_password,
        )
        logger.info(
            f"Connected to database: "
            f"{self.config.db_host}:{self.config.db_port}/{self.config.db_name}"
        )

    def close(self):
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def write_heart_rate_metric(self, result: Dict[str, Any]):
        """Write heart rate 1-minute aggregation."""
        if not self._conn:
            self.connect()

        with self._conn.cursor() as cur:
            cur.execute("""
                INSERT INTO health_metrics_1min
                    (time, user_id, metric_type, window_start, window_end,
                     avg_value, min_value, max_value, reading_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                result["window_end"],
                result["user_id"],
                "heart_rate",
                result["window_start"],
                result["window_end"],
                result["avg_heart_rate"],
                result["min_heart_rate"],
                result["max_heart_rate"],
                result["reading_count"],
            ))
            self._conn.commit()

        if "alert" in result:
            self._write_alert(result)

    def write_blood_pressure_metric(self, result: Dict[str, Any]):
        """Write blood pressure 1-minute aggregation."""
        if not self._conn:
            self.connect()

        with self._conn.cursor() as cur:
            cur.execute("""
                INSERT INTO health_metrics_1min
                    (time, user_id, metric_type, window_start, window_end,
                     avg_value, min_value, max_value, reading_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                result["window_end"],
                result["user_id"],
                "blood_pressure",
                result["window_start"],
                result["window_end"],
                result["avg_bp_systolic"],
                result.get("avg_bp_diastolic", 0),
                result.get("avg_bp_systolic", 0),
                result["reading_count"],
            ))
            self._conn.commit()

        if "alert" in result:
            self._write_alert(result)

    def write_blood_sugar_metric(self, result: Dict[str, Any]):
        """Write blood sugar 10-minute aggregation."""
        if not self._conn:
            self.connect()

        with self._conn.cursor() as cur:
            cur.execute("""
                INSERT INTO health_metrics_10min
                    (time, user_id, metric_type, window_start, window_end,
                     avg_value, min_value, max_value, reading_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                result["window_end"],
                result["user_id"],
                "blood_sugar",
                result["window_start"],
                result["window_end"],
                result["avg_blood_sugar"],
                result["min_blood_sugar"],
                result["max_blood_sugar"],
                result["reading_count"],
            ))
            self._conn.commit()

        if "alert" in result:
            self._write_alert(result)

    def _write_alert(self, result: Dict[str, Any]):
        """Write health alert to database."""
        alert = result["alert"]

        if alert["type"] == "elevated_heart_rate":
            metric_value = alert["value"]
            threshold = alert["threshold"]
        elif alert["type"] == "elevated_blood_pressure":
            metric_value = alert["systolic"]
            threshold = self.config.thresholds.elevated_bp_systolic
        elif alert["type"] == "elevated_blood_sugar":
            metric_value = alert["value"]
            threshold = alert["threshold"]
        else:
            return

        with self._conn.cursor() as cur:
            cur.execute("""
                INSERT INTO health_alerts
                    (time, user_id, alert_type, severity,
                     metric_value, threshold_value, window_start, window_end)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                result["window_end"],
                result["user_id"],
                alert["type"],
                alert["severity"],
                metric_value,
                threshold,
                result["window_start"],
                result["window_end"],
            ))
            self._conn.commit()

        logger.warning(
            f"ALERT: {alert['type']} for user {result['user_id']} - "
            f"value: {metric_value}, threshold: {threshold}, "
            f"severity: {alert['severity']}"
        )

    def process_result(self, json_result: str):
        """Process a JSON result string and route to appropriate handler."""
        try:
            result = json.loads(json_result)
            result_type = result.get("type", "")

            if result_type == "heart_rate_1min":
                self.write_heart_rate_metric(result)
            elif result_type == "blood_pressure_1min":
                self.write_blood_pressure_metric(result)
            elif result_type == "blood_sugar_10min":
                self.write_blood_sugar_metric(result)
            else:
                logger.warning(f"Unknown result type: {result_type}")
        except Exception as e:
            logger.error(f"Failed to process result: {e}")


def init_health_metrics_schema(config: FlinkConfig):
    """Initialize database schema for health metrics tables.

    Creates a unified schema used by both the Flink SinkFunction and the
    standalone HealthMetricsSink.  The schema is generic:
    - time, metric_type, avg_value, min_value, max_value
    which lets us store heart-rate, blood-pressure, and blood-sugar rows
    in the same hypertable with a type discriminator.
    """
    conn = psycopg2.connect(
        host=config.db_host,
        port=config.db_port,
        dbname=config.db_name,
        user=config.db_user,
        password=config.db_password,
    )

    with conn.cursor() as cur:
        # 1-minute aggregations (heart rate + blood pressure)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS health_metrics_1min (
                time         TIMESTAMPTZ NOT NULL,
                user_id      TEXT        NOT NULL,
                metric_type  TEXT        NOT NULL,
                window_start TIMESTAMPTZ NOT NULL,
                window_end   TIMESTAMPTZ NOT NULL,
                avg_value    NUMERIC,
                min_value    NUMERIC,
                max_value    NUMERIC,
                reading_count INTEGER,
                UNIQUE (window_start, user_id, metric_type)
            );
        """)

        # Convert to hypertable if TimescaleDB is available
        cur.execute("""
            DO $$
            BEGIN
                PERFORM create_hypertable(
                    'health_metrics_1min', 'time',
                    if_not_exists => TRUE,
                    migrate_data  => TRUE
                );
            EXCEPTION WHEN others THEN
                RAISE NOTICE 'Could not create hypertable (TimescaleDB may not be available): %', SQLERRM;
            END $$;
        """)

        # 10-minute aggregations (blood sugar)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS health_metrics_10min (
                time         TIMESTAMPTZ NOT NULL,
                user_id      TEXT        NOT NULL,
                metric_type  TEXT        NOT NULL,
                window_start TIMESTAMPTZ NOT NULL,
                window_end   TIMESTAMPTZ NOT NULL,
                avg_value    NUMERIC,
                min_value    NUMERIC,
                max_value    NUMERIC,
                reading_count INTEGER,
                UNIQUE (window_start, user_id, metric_type)
            );
        """)

        cur.execute("""
            DO $$
            BEGIN
                PERFORM create_hypertable(
                    'health_metrics_10min', 'time',
                    if_not_exists => TRUE,
                    migrate_data  => TRUE
                );
            EXCEPTION WHEN others THEN
                RAISE NOTICE 'Could not create hypertable: %', SQLERRM;
            END $$;
        """)

        # Health alerts
        cur.execute("""
            CREATE TABLE IF NOT EXISTS health_alerts (
                time            TIMESTAMPTZ NOT NULL,
                user_id         TEXT        NOT NULL,
                alert_type      TEXT        NOT NULL,
                severity        TEXT        DEFAULT 'warning',
                metric_value    NUMERIC,
                threshold_value NUMERIC,
                window_start    TIMESTAMPTZ,
                window_end      TIMESTAMPTZ
            );
        """)

        cur.execute("""
            DO $$
            BEGIN
                PERFORM create_hypertable(
                    'health_alerts', 'time',
                    if_not_exists => TRUE,
                    migrate_data  => TRUE
                );
            EXCEPTION WHEN others THEN
                RAISE NOTICE 'Could not create hypertable: %', SQLERRM;
            END $$;
        """)

        # Indexes for fast queries
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_health_metrics_1min_user
            ON health_metrics_1min (user_id, time DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_health_metrics_1min_type
            ON health_metrics_1min (metric_type, time DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_health_metrics_10min_user
            ON health_metrics_10min (user_id, time DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_health_alerts_user
            ON health_alerts (user_id, time DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_health_alerts_type
            ON health_alerts (alert_type, time DESC);
        """)

        conn.commit()

    conn.close()
    logger.info("Health metrics schema initialized successfully")

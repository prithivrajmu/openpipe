"""Database sinks for Flink stream processor results."""

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
        logger.info(f"Connected to database: {self.config.db_host}:{self.config.db_port}/{self.config.db_name}")
    
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
                    (window_start, window_end, user_id, avg_heart_rate, min_heart_rate, max_heart_rate, reading_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (window_start, user_id) DO UPDATE SET
                    avg_heart_rate = EXCLUDED.avg_heart_rate,
                    min_heart_rate = EXCLUDED.min_heart_rate,
                    max_heart_rate = EXCLUDED.max_heart_rate,
                    reading_count = EXCLUDED.reading_count
            """, (
                result["window_start"],
                result["window_end"],
                result["user_id"],
                result["avg_heart_rate"],
                result["min_heart_rate"],
                result["max_heart_rate"],
                result["reading_count"],
            ))
            self._conn.commit()
        
        # Write alert if present
        if "alert" in result:
            self._write_alert(result)
    
    def write_blood_pressure_metric(self, result: Dict[str, Any]):
        """Write blood pressure 1-minute aggregation."""
        if not self._conn:
            self.connect()
        
        with self._conn.cursor() as cur:
            cur.execute("""
                INSERT INTO health_metrics_1min 
                    (window_start, window_end, user_id, avg_bp_systolic, avg_bp_diastolic, reading_count)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (window_start, user_id) DO UPDATE SET
                    avg_bp_systolic = EXCLUDED.avg_bp_systolic,
                    avg_bp_diastolic = EXCLUDED.avg_bp_diastolic,
                    reading_count = COALESCE(health_metrics_1min.reading_count, 0) + EXCLUDED.reading_count
            """, (
                result["window_start"],
                result["window_end"],
                result["user_id"],
                result["avg_bp_systolic"],
                result["avg_bp_diastolic"],
                result["reading_count"],
            ))
            self._conn.commit()
        
        # Write alert if present
        if "alert" in result:
            self._write_alert(result)
    
    def write_blood_sugar_metric(self, result: Dict[str, Any]):
        """Write blood sugar 10-minute aggregation."""
        if not self._conn:
            self.connect()
        
        with self._conn.cursor() as cur:
            cur.execute("""
                INSERT INTO health_metrics_10min 
                    (window_start, window_end, user_id, avg_blood_sugar, min_blood_sugar, max_blood_sugar, reading_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (window_start, user_id) DO UPDATE SET
                    avg_blood_sugar = EXCLUDED.avg_blood_sugar,
                    min_blood_sugar = EXCLUDED.min_blood_sugar,
                    max_blood_sugar = EXCLUDED.max_blood_sugar,
                    reading_count = EXCLUDED.reading_count
            """, (
                result["window_start"],
                result["window_end"],
                result["user_id"],
                result["avg_blood_sugar"],
                result["min_blood_sugar"],
                result["max_blood_sugar"],
                result["reading_count"],
            ))
            self._conn.commit()
        
        # Write alert if present
        if "alert" in result:
            self._write_alert(result)
    
    def _write_alert(self, result: Dict[str, Any]):
        """Write health alert to database."""
        alert = result["alert"]
        
        # Determine metric value based on alert type
        if alert["type"] == "elevated_heart_rate":
            metric_value = alert["value"]
            threshold = alert["threshold"]
        elif alert["type"] == "elevated_blood_pressure":
            metric_value = alert["systolic"]  # Use systolic as primary
            threshold = self.config.thresholds.elevated_bp_systolic
        elif alert["type"] == "elevated_blood_sugar":
            metric_value = alert["value"]
            threshold = alert["threshold"]
        else:
            return
        
        with self._conn.cursor() as cur:
            cur.execute("""
                INSERT INTO health_alerts 
                    (alert_time, user_id, alert_type, metric_value, threshold, severity)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                result["window_end"],
                result["user_id"],
                alert["type"],
                metric_value,
                threshold,
                alert["severity"],
            ))
            self._conn.commit()
        
        logger.warning(
            f"ALERT: {alert['type']} for user {result['user_id']} - "
            f"value: {metric_value}, threshold: {threshold}, severity: {alert['severity']}"
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
    """Initialize database schema for health metrics tables."""
    conn = psycopg2.connect(
        host=config.db_host,
        port=config.db_port,
        dbname=config.db_name,
        user=config.db_user,
        password=config.db_password,
    )
    
    with conn.cursor() as cur:
        # 1-minute aggregations (heart rate + BP)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS health_metrics_1min (
                window_start TIMESTAMPTZ NOT NULL,
                window_end TIMESTAMPTZ NOT NULL,
                user_id TEXT NOT NULL,
                avg_heart_rate NUMERIC,
                min_heart_rate INTEGER,
                max_heart_rate INTEGER,
                avg_bp_systolic NUMERIC,
                avg_bp_diastolic NUMERIC,
                reading_count INTEGER,
                PRIMARY KEY (window_start, user_id)
            );
        """)
        
        # 10-minute aggregations (blood sugar)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS health_metrics_10min (
                window_start TIMESTAMPTZ NOT NULL,
                window_end TIMESTAMPTZ NOT NULL,
                user_id TEXT NOT NULL,
                avg_blood_sugar NUMERIC,
                min_blood_sugar INTEGER,
                max_blood_sugar INTEGER,
                reading_count INTEGER,
                PRIMARY KEY (window_start, user_id)
            );
        """)
        
        # Health alerts
        cur.execute("""
            CREATE TABLE IF NOT EXISTS health_alerts (
                id SERIAL,
                alert_time TIMESTAMPTZ NOT NULL,
                user_id TEXT NOT NULL,
                alert_type TEXT NOT NULL,
                metric_value NUMERIC,
                threshold NUMERIC,
                severity TEXT DEFAULT 'warning',
                PRIMARY KEY (alert_time, id)
            );
        """)
        
        # Create indexes for faster queries
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_health_metrics_1min_user 
            ON health_metrics_1min (user_id, window_start DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_health_metrics_10min_user 
            ON health_metrics_10min (user_id, window_start DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_health_alerts_user 
            ON health_alerts (user_id, alert_time DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_health_alerts_type 
            ON health_alerts (alert_type, alert_time DESC);
        """)
        
        conn.commit()
    
    conn.close()
    logger.info("Health metrics schema initialized successfully")

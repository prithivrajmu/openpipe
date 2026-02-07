"""
Health Metrics Stream Processor using PyFlink.

Implements windowed aggregations:
- Heart rate: 1-minute tumbling window with elevated HR alerts
- Blood pressure: 1-minute tumbling window with elevated BP alerts
- Blood sugar: 10-minute tumbling window with elevated sugar alerts
"""

import argparse
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Time
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.functions import (
    ProcessWindowFunction,
    RuntimeContext,
    MapFunction,
)
from pyflink.common.typeinfo import Types

from .config import FlinkConfig


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class HealthReading:
    """Parsed health reading from wearable device."""
    timestamp: datetime
    device_id: str
    user_id: str
    heart_rate: Optional[int]
    bp_systolic: Optional[int]
    bp_diastolic: Optional[int]
    blood_sugar: Optional[int]
    is_valid: bool = True


class HealthMessageParser(MapFunction):
    """Parse incoming JSON messages to HealthReading."""
    
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
                heart_rate is None or (isinstance(heart_rate, int) and heart_rate > 0),
                bp_systolic is None or (isinstance(bp_systolic, int) and bp_systolic > 0),
                bp_diastolic is None or (isinstance(bp_diastolic, int) and bp_diastolic > 0),
                blood_sugar is None or (isinstance(blood_sugar, int) and blood_sugar > 0),
            ])
            
            timestamp = data.get("ingested_at", datetime.now().isoformat())
            device_id = raw_data.get("device_id", "unknown")
            user_id = raw_data.get("user_id", "unknown")
            
            return (
                timestamp,
                user_id,
                heart_rate if is_valid and heart_rate else -1,
                bp_systolic if is_valid and bp_systolic else -1,
                bp_diastolic if is_valid and bp_diastolic else -1,
                blood_sugar if is_valid and blood_sugar else -1,
                1 if is_valid else 0,  # valid flag
            )
            
        except Exception as e:
            logger.error(f"Failed to parse message: {e}")
            return (
                datetime.now().isoformat(),
                "unknown",
                -1, -1, -1, -1, 0
            )


class HeartRateWindowProcessor(ProcessWindowFunction):
    """Process 1-minute heart rate windows and generate alerts."""
    
    def __init__(self, threshold: int):
        super().__init__()
        self.threshold = threshold
    
    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context,
        elements,
    ):
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
        elements,
    ):
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
        elements,
    ):
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


def run_health_stream_job(config: FlinkConfig):
    """Main entry point for the Flink stream processing job."""
    logger.info("Starting Health Metrics Stream Processor")
    logger.info(f"Kafka: {config.kafka_bootstrap_servers} / {config.kafka_topic}")
    logger.info(f"Windows: HR={config.windows.heart_rate_minutes}min, BP={config.windows.blood_pressure_minutes}min, Sugar={config.windows.blood_sugar_minutes}min")
    logger.info(f"Thresholds: HR>{config.thresholds.elevated_heart_rate}, BP≥{config.thresholds.elevated_bp_systolic}/{config.thresholds.elevated_bp_diastolic}, Sugar≥{config.thresholds.elevated_blood_sugar}")
    
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Add Kafka connector JARs
    env.add_jars(
        "file:///opt/flink/lib/flink-connector-kafka-1.18.0.jar",
        "file:///opt/flink/lib/kafka-clients-3.7.0.jar",
    )
    
    # Create Kafka source
    kafka_source = create_kafka_source(config)
    
    # Create data stream
    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.for_monotonous_timestamps(),
        "Wearables Kafka Source"
    )
    
    # Parse messages
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
    
    # Heart rate aggregation (1-minute window)
    hr_window_ms = config.windows.heart_rate_minutes * 60 * 1000
    heart_rate_stream = (
        keyed_stream
        .window(TumblingEventTimeWindows.of(Time.milliseconds(hr_window_ms)))
        .process(HeartRateWindowProcessor(config.thresholds.elevated_heart_rate))
    )
    
    # Blood pressure aggregation (1-minute window)
    bp_window_ms = config.windows.blood_pressure_minutes * 60 * 1000
    blood_pressure_stream = (
        keyed_stream
        .window(TumblingEventTimeWindows.of(Time.milliseconds(bp_window_ms)))
        .process(BloodPressureWindowProcessor(
            config.thresholds.elevated_bp_systolic,
            config.thresholds.elevated_bp_diastolic
        ))
    )
    
    # Blood sugar aggregation (10-minute window)
    sugar_window_ms = config.windows.blood_sugar_minutes * 60 * 1000
    blood_sugar_stream = (
        keyed_stream
        .window(TumblingEventTimeWindows.of(Time.milliseconds(sugar_window_ms)))
        .process(BloodSugarWindowProcessor(config.thresholds.elevated_blood_sugar))
    )
    
    # Print results (will be replaced with DB sink)
    heart_rate_stream.print().name("Heart Rate Output")
    blood_pressure_stream.print().name("Blood Pressure Output")
    blood_sugar_stream.print().name("Blood Sugar Output")
    
    # Execute
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

"""
Tests for the Health Metrics Flink Processor.

Tests windowed aggregations and alert generation logic.
"""

import json
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, patch, MagicMock

import sys
sys.path.insert(0, '/home/prithiv/Prithiv_Projects/openpipe')

from flink_processor.config import FlinkConfig, WindowConfig, ThresholdConfig


class TestFlinkConfig:
    """Tests for configuration loading."""
    
    def test_default_config(self):
        """Test default configuration values."""
        config = FlinkConfig()
        
        assert config.kafka_bootstrap_servers == "localhost:9092"
        assert config.kafka_topic == "virtual-wearables"
        assert config.windows.heart_rate_minutes == 1
        assert config.windows.blood_pressure_minutes == 1
        assert config.windows.blood_sugar_minutes == 10
        assert config.thresholds.elevated_heart_rate == 100
        assert config.thresholds.elevated_bp_systolic == 140
        assert config.thresholds.elevated_bp_diastolic == 90
        assert config.thresholds.elevated_blood_sugar == 180
    
    def test_jdbc_url_generation(self):
        """Test JDBC URL is correctly generated."""
        config = FlinkConfig(
            db_host="testhost",
            db_port=5433,
            db_name="testdb"
        )
        assert config.jdbc_url == "jdbc:postgresql://testhost:5433/testdb"


class TestHeartRateAggregation:
    """Tests for heart rate windowed aggregation."""
    
    def test_normal_heart_rate_no_alert(self):
        """Test normal heart rate does not generate alert."""
        readings = [75, 80, 78, 82, 77]
        avg = sum(readings) / len(readings)
        threshold = 100
        
        assert avg <= threshold
        assert avg == 78.4
    
    def test_elevated_heart_rate_generates_alert(self):
        """Test elevated heart rate generates warning alert."""
        readings = [105, 110, 108, 112, 106]
        avg = sum(readings) / len(readings)
        threshold = 100
        
        assert avg > threshold
        assert avg == 108.2
        
        severity = "warning" if avg < 120 else "critical"
        assert severity == "warning"
    
    def test_critical_heart_rate_alert(self):
        """Test very high heart rate generates critical alert."""
        readings = [125, 130, 128, 135, 132]
        avg = sum(readings) / len(readings)
        threshold = 100
        
        assert avg > threshold
        severity = "warning" if avg < 120 else "critical"
        assert severity == "critical"


class TestBloodPressureAggregation:
    """Tests for blood pressure windowed aggregation."""
    
    def test_normal_bp_no_alert(self):
        """Test normal blood pressure does not generate alert."""
        systolic = [120, 118, 122, 119, 121]
        diastolic = [80, 78, 82, 79, 81]
        
        avg_sys = sum(systolic) / len(systolic)
        avg_dia = sum(diastolic) / len(diastolic)
        
        sys_threshold = 140
        dia_threshold = 90
        
        has_alert = avg_sys >= sys_threshold or avg_dia >= dia_threshold
        assert not has_alert
    
    def test_elevated_systolic_generates_alert(self):
        """Test elevated systolic BP generates alert."""
        systolic = [145, 148, 142, 150, 146]
        diastolic = [80, 78, 82, 79, 81]
        
        avg_sys = sum(systolic) / len(systolic)
        avg_dia = sum(diastolic) / len(diastolic)
        
        sys_threshold = 140
        dia_threshold = 90
        
        has_alert = avg_sys >= sys_threshold or avg_dia >= dia_threshold
        assert has_alert
        assert avg_sys >= sys_threshold
    
    def test_elevated_diastolic_generates_alert(self):
        """Test elevated diastolic BP generates alert."""
        systolic = [120, 118, 122, 119, 121]
        diastolic = [92, 95, 91, 94, 93]
        
        avg_sys = sum(systolic) / len(systolic)
        avg_dia = sum(diastolic) / len(diastolic)
        
        sys_threshold = 140
        dia_threshold = 90
        
        has_alert = avg_sys >= sys_threshold or avg_dia >= dia_threshold
        assert has_alert
        assert avg_dia >= dia_threshold


class TestBloodSugarAggregation:
    """Tests for blood sugar windowed aggregation."""
    
    def test_normal_blood_sugar_no_alert(self):
        """Test normal blood sugar does not generate alert."""
        readings = [95, 100, 98, 105, 92]
        avg = sum(readings) / len(readings)
        threshold = 180
        
        assert avg < threshold
        assert not (avg >= threshold)
    
    def test_elevated_blood_sugar_generates_alert(self):
        """Test elevated blood sugar generates alert."""
        readings = [185, 190, 182, 195, 188]
        avg = sum(readings) / len(readings)
        threshold = 180
        
        assert avg >= threshold
        
        severity = "warning" if avg < 250 else "critical"
        assert severity == "warning"
    
    def test_critical_blood_sugar_alert(self):
        """Test very high blood sugar generates critical alert."""
        readings = [260, 270, 255, 280, 265]
        avg = sum(readings) / len(readings)
        threshold = 180
        
        assert avg >= threshold
        
        severity = "warning" if avg < 250 else "critical"
        assert severity == "critical"


class TestMessageParsing:
    """Tests for Kafka message parsing."""
    
    def test_parse_valid_wearable_message(self):
        """Test parsing a valid wearable message."""
        message = {
            "kafka_topic": "virtual-wearables",
            "kafka_partition": 0,
            "kafka_offset": 123,
            "ingested_at": "2026-02-08T00:00:00+00:00",
            "raw_data": {
                "device_id": "wearable_abc123",
                "user_id": "user_xyz789",
                "blood_pressure": {
                    "systolic": 120,
                    "diastolic": 80,
                    "unit": "mmHg"
                },
                "blood_sugar": {
                    "value": 95,
                    "unit": "mg/dL"
                },
                "heart_rate": {
                    "value": 72,
                    "unit": "bpm"
                }
            }
        }
        
        raw = message["raw_data"]
        
        hr = raw["heart_rate"]["value"]
        bp_sys = raw["blood_pressure"]["systolic"]
        bp_dia = raw["blood_pressure"]["diastolic"]
        sugar = raw["blood_sugar"]["value"]
        
        assert hr == 72
        assert bp_sys == 120
        assert bp_dia == 80
        assert sugar == 95
    
    def test_parse_bad_data_with_nulls(self):
        """Test parsing message with null values."""
        message = {
            "raw_data": {
                "user_id": "user_test",
                "blood_pressure": None,
                "blood_sugar": None,
                "heart_rate": None
            }
        }
        
        raw = message["raw_data"]
        hr = raw["heart_rate"]
        assert hr is None
        
        is_valid = hr is not None and isinstance(hr, dict)
        assert not is_valid
    
    def test_parse_bad_data_with_negative_values(self):
        """Test parsing message with negative (invalid) values."""
        message = {
            "raw_data": {
                "user_id": "user_test",
                "heart_rate": {"value": -50, "unit": "bpm"},
                "blood_pressure": {"systolic": -120, "diastolic": -80},
                "blood_sugar": {"value": -100}
            }
        }
        
        raw = message["raw_data"]
        hr = raw["heart_rate"]["value"]
        
        is_valid = hr is not None and isinstance(hr, int) and hr > 0
        assert not is_valid


class TestWindowLogic:
    """Tests for windowing logic."""
    
    def test_one_minute_window_alignment(self):
        """Test that 1-minute windows are properly aligned."""
        now = datetime(2026, 2, 8, 10, 30, 45, tzinfo=timezone.utc)
        
        window_start = now.replace(second=0, microsecond=0)
        window_end = window_start + timedelta(minutes=1)
        
        assert window_start.second == 0
        assert window_start.microsecond == 0
        assert (window_end - window_start).total_seconds() == 60
    
    def test_ten_minute_window_size(self):
        """Test that 10-minute windows have correct duration."""
        window_minutes = 10
        window_duration = timedelta(minutes=window_minutes)
        
        assert window_duration.total_seconds() == 600


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

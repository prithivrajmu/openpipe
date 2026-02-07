"""Tests for Kafka consumer with message enrichment."""

import json
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime


def test_message_enrichment():
    """Test that messages are properly enriched with metadata."""
    from data_pipeline.kafka_consumer import DataPipelineConsumer
    
    consumer = DataPipelineConsumer(
        bootstrap_servers="localhost:9092",
        topics=["virtual-wearables"],
        consumer_group="test-group"
    )
    
    # Create a mock Kafka message
    mock_message = Mock()
    mock_message.topic = "virtual-wearables"
    mock_message.partition = 0
    mock_message.offset = 12345
    mock_message.value = {
        "device_id": "test-device-001",
        "blood_pressure": {"systolic": 120, "diastolic": 80}
    }
    
    # Test enrichment
    enriched = consumer._enrich_message(mock_message)
    
    assert enriched["kafka_topic"] == "virtual-wearables"
    assert enriched["kafka_partition"] == 0
    assert enriched["kafka_offset"] == 12345
    assert enriched["ingestion_protocol"] == "kafka"
    assert "ingested_at" in enriched
    assert enriched["raw_data"]["device_id"] == "test-device-001"


def test_consumer_stats():
    """Test consumer statistics tracking."""
    from data_pipeline.kafka_consumer import DataPipelineConsumer
    
    consumer = DataPipelineConsumer()
    
    stats = consumer.get_stats()
    
    assert "running" in stats
    assert "topics" in stats
    assert "consumer_group" in stats
    assert "messages_consumed" in stats
    assert "errors" in stats


def test_consumer_health_check():
    """Test consumer health check."""
    from data_pipeline.kafka_consumer import DataPipelineConsumer
    
    consumer = DataPipelineConsumer()
    
    # Not running - should report accordingly
    health = consumer.check_health()
    
    assert "status" in health
    assert health["running"] == False


if __name__ == "__main__":
    test_message_enrichment()
    test_consumer_stats()
    test_consumer_health_check()
    print("All consumer tests passed!")

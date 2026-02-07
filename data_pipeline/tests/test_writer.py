"""Tests for batch writer functionality."""

import json
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone


def test_batch_writer_initialization():
    """Test batch writer initializes with correct config."""
    from data_pipeline.db.writer import BatchWriter
    
    writer = BatchWriter(batch_size=50, flush_interval_seconds=2.0)
    
    assert writer.batch_size == 50
    assert writer.flush_interval == 2.0
    
    stats = writer.get_stats()
    assert stats["queue_depth"] == 0
    assert stats["messages_queued"] == 0
    assert stats["messages_written"] == 0


def test_batch_writer_add_message():
    """Test adding messages to the writer queue."""
    from data_pipeline.db.writer import BatchWriter
    
    writer = BatchWriter(batch_size=100, flush_interval_seconds=10.0)
    
    enriched_message = {
        "kafka_topic": "virtual-wearables",
        "kafka_partition": 0,
        "kafka_offset": 123,
        "ingestion_protocol": "kafka",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "raw_data": {"device_id": "test-001", "value": 42}
    }
    
    writer.add(enriched_message)
    
    stats = writer.get_stats()
    assert stats["messages_queued"] == 1
    assert stats["queue_depth"] == 1


def test_batch_writer_health_check():
    """Test batch writer health check."""
    from data_pipeline.db.writer import BatchWriter
    
    writer = BatchWriter(batch_size=100)
    
    health = writer.check_health()
    
    assert "status" in health
    assert health["status"] == "healthy"  # Empty queue, no errors


def test_topic_to_table_name():
    """Test topic name to table name conversion."""
    from data_pipeline.db.models import topic_to_table_name
    
    assert topic_to_table_name("virtual-wearables") == "virtual_wearables"
    assert topic_to_table_name("virtual-gps") == "virtual_gps"
    assert topic_to_table_name("virtual-restaurants") == "virtual_restaurants"


if __name__ == "__main__":
    test_batch_writer_initialization()
    test_batch_writer_add_message()
    test_batch_writer_health_check()
    test_topic_to_table_name()
    print("All writer tests passed!")

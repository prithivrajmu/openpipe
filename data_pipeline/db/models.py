"""TimescaleDB table models and hypertable management."""

from typing import Dict, List, Any
from sqlalchemy import text

from .connection import get_db


# Table definitions for each Kafka topic
TABLE_DEFINITIONS = {
    "virtual_wearables": """
        CREATE TABLE IF NOT EXISTS virtual_wearables (
            id SERIAL,
            device_id VARCHAR(64) NOT NULL,
            kafka_topic VARCHAR(128) NOT NULL,
            kafka_partition INTEGER,
            kafka_offset BIGINT,
            ingestion_protocol VARCHAR(32) DEFAULT 'kafka',
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw_data JSONB NOT NULL,
            PRIMARY KEY (id, ingested_at)
        )
    """,
    "virtual_restaurants": """
        CREATE TABLE IF NOT EXISTS virtual_restaurants (
            id SERIAL,
            device_id VARCHAR(64) NOT NULL,
            kafka_topic VARCHAR(128) NOT NULL,
            kafka_partition INTEGER,
            kafka_offset BIGINT,
            ingestion_protocol VARCHAR(32) DEFAULT 'kafka',
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw_data JSONB NOT NULL,
            PRIMARY KEY (id, ingested_at)
        )
    """,
    "virtual_gps": """
        CREATE TABLE IF NOT EXISTS virtual_gps (
            id SERIAL,
            device_id VARCHAR(64) NOT NULL,
            kafka_topic VARCHAR(128) NOT NULL,
            kafka_partition INTEGER,
            kafka_offset BIGINT,
            ingestion_protocol VARCHAR(32) DEFAULT 'kafka',
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw_data JSONB NOT NULL,
            PRIMARY KEY (id, ingested_at)
        )
    """
}

# Indexes for each table
INDEX_DEFINITIONS = {
    "virtual_wearables": [
        "CREATE INDEX IF NOT EXISTS idx_wearables_device_id ON virtual_wearables(device_id, ingested_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_wearables_topic ON virtual_wearables(kafka_topic)"
    ],
    "virtual_restaurants": [
        "CREATE INDEX IF NOT EXISTS idx_restaurants_device_id ON virtual_restaurants(device_id, ingested_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_restaurants_topic ON virtual_restaurants(kafka_topic)"
    ],
    "virtual_gps": [
        "CREATE INDEX IF NOT EXISTS idx_gps_device_id ON virtual_gps(device_id, ingested_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_gps_topic ON virtual_gps(kafka_topic)"
    ]
}


def topic_to_table_name(topic: str) -> str:
    """Convert Kafka topic name to table name."""
    # Replace hyphens with underscores: virtual-wearables -> virtual_wearables
    return topic.replace("-", "_")


def create_tables() -> None:
    """Create all tables if they don't exist."""
    db = get_db()
    
    for table_name, create_sql in TABLE_DEFINITIONS.items():
        try:
            db.execute(create_sql)
            print(f"[Models] Created/verified table: {table_name}")
        except Exception as e:
            print(f"[Models] Error creating table {table_name}: {e}")


def create_hypertables() -> None:
    """Convert tables to TimescaleDB hypertables."""
    db = get_db()
    
    for table_name in TABLE_DEFINITIONS.keys():
        try:
            # Check if already a hypertable
            check_sql = """
                SELECT EXISTS (
                    SELECT 1 FROM timescaledb_information.hypertables
                    WHERE hypertable_name = :table_name
                )
            """
            with db.session() as session:
                result = session.execute(text(check_sql), {"table_name": table_name})
                is_hypertable = result.scalar()
            
            if not is_hypertable:
                # Convert to hypertable
                hypertable_sql = f"SELECT create_hypertable('{table_name}', 'ingested_at', if_not_exists => TRUE)"
                db.execute(hypertable_sql)
                print(f"[Models] Created hypertable: {table_name}")
            else:
                print(f"[Models] Hypertable already exists: {table_name}")
                
        except Exception as e:
            # TimescaleDB might not be installed, log warning
            print(f"[Models] Warning - could not create hypertable {table_name}: {e}")


def create_indexes() -> None:
    """Create indexes for all tables."""
    db = get_db()
    
    for table_name, indexes in INDEX_DEFINITIONS.items():
        for index_sql in indexes:
            try:
                db.execute(index_sql)
            except Exception as e:
                print(f"[Models] Error creating index for {table_name}: {e}")
    
    print("[Models] Created/verified all indexes")


def init_schema() -> None:
    """Initialize the complete database schema."""
    print("[Models] Initializing schema...")
    create_tables()
    create_hypertables()
    create_indexes()
    print("[Models] Schema initialization complete")


def get_table_stats() -> Dict[str, Any]:
    """Get statistics for all tables."""
    db = get_db()
    stats = {}
    
    for table_name in TABLE_DEFINITIONS.keys():
        try:
            with db.session() as session:
                # Row count
                count_result = session.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                row_count = count_result.scalar()
                
                # Latest record
                latest_result = session.execute(
                    text(f"SELECT ingested_at FROM {table_name} ORDER BY ingested_at DESC LIMIT 1")
                )
                latest = latest_result.scalar()
                
                stats[table_name] = {
                    "row_count": row_count,
                    "latest_record": str(latest) if latest else None
                }
        except Exception as e:
            stats[table_name] = {"error": str(e)}
    
    return stats

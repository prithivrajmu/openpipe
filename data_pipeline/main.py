"""Main entry point for Data Pipeline Service."""

import argparse
import signal
import sys
import os
from pathlib import Path
from typing import Optional

import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from .config import load_config, AppConfig
from .kafka_consumer import init_consumer, get_consumer
from .db.connection import init_db, get_db
from .db.models import init_schema
from .db.writer import BatchWriter
from .api.health import router as health_router, set_components
from .api.routes import router as data_router


# FastAPI app
app = FastAPI(
    title="OpenPipe Data Pipeline",
    description="Kafka to TimescaleDB data pipeline with SQL query interface",
    version="0.1.0"
)

# Include routers
app.include_router(health_router)
app.include_router(data_router, prefix="/api")

# Global components
_batch_writer: Optional[BatchWriter] = None
_config: Optional[AppConfig] = None


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve the data viewer dashboard."""
    template_path = Path(__file__).parent / "ui" / "templates" / "index.html"
    
    if template_path.exists():
        return HTMLResponse(content=template_path.read_text())
    else:
        return HTMLResponse(content="""
            <html><body style="background:#0f172a;color:#f1f5f9;font-family:sans-serif;padding:2rem;">
                <h1>OpenPipe Data Pipeline</h1>
                <p>API is running. Template not found.</p>
                <ul>
                    <li><a href="/health" style="color:#6366f1;">/health</a> - Health status</li>
                    <li><a href="/api/tables" style="color:#6366f1;">/api/tables</a> - List tables</li>
                    <li><a href="/docs" style="color:#6366f1;">/docs</a> - API documentation</li>
                </ul>
            </body></html>
        """)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Data Pipeline - Consume Kafka messages and persist to TimescaleDB"
    )
    parser.add_argument(
        "--config", "-c",
        default="config/settings.yaml",
        help="Path to configuration file (default: config/settings.yaml)"
    )
    parser.add_argument(
        "--port", "-p",
        type=int,
        default=None,
        help="Dashboard port (overrides config, default: 8081)"
    )
    parser.add_argument(
        "--simulator-url",
        default="http://localhost:8080",
        help="URL of the virtual device simulator for health checks"
    )
    return parser.parse_args()


class DataPipelineService:
    """Main service that orchestrates data pipeline components."""
    
    def __init__(self, config: AppConfig, simulator_url: str):
        self.config = config
        self.simulator_url = simulator_url
        self.batch_writer: Optional[BatchWriter] = None
    
    def start(self):
        """Start all service components."""
        print("=" * 60)
        print("  OpenPipe Data Pipeline")
        print("=" * 60)
        print()
        
        # Initialize database
        print("[Pipeline] Connecting to TimescaleDB...")
        db = init_db(self.config.database.connection_string)
        
        # Initialize schema
        print("[Pipeline] Initializing schema...")
        init_schema()
        
        # Initialize batch writer
        print("[Pipeline] Starting batch writer...")
        self.batch_writer = BatchWriter(
            batch_size=self.config.pipeline.batch_size,
            flush_interval_seconds=self.config.pipeline.flush_interval_seconds
        )
        self.batch_writer.start()
        
        # Initialize Kafka consumer
        print("[Pipeline] Starting Kafka consumer...")
        topics = list(self.config.kafka.topics.values())
        consumer = init_consumer(
            bootstrap_servers=self.config.kafka.bootstrap_servers,
            topics=topics,
            consumer_group=self.config.pipeline.consumer_group,
            on_message=self.batch_writer.add
        )
        consumer.start()
        
        # Set components for health checks
        set_components(
            kafka_consumer=consumer,
            batch_writer=self.batch_writer,
            db_connection=db,
            simulator_url=self.simulator_url
        )
        
        print()
        print(f"[Pipeline] Subscribed to topics: {topics}")
        print(f"[Pipeline] Consumer group: {self.config.pipeline.consumer_group}")
        print(f"[Pipeline] Database: {self.config.database.host}:{self.config.database.port}/{self.config.database.name}")
        print()
    
    def run_dashboard(self, port: Optional[int] = None):
        """Run the FastAPI dashboard."""
        dashboard_port = port or self.config.monitoring.dashboard_port
        print(f"[Pipeline] Starting dashboard on http://localhost:{dashboard_port}")
        print()
        
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=dashboard_port,
            log_level="warning"
        )
    
    def shutdown(self):
        """Shutdown all components."""
        print()
        print("[Pipeline] Shutting down...")
        
        try:
            consumer = get_consumer()
            consumer.stop()
        except RuntimeError:
            pass
        
        if self.batch_writer:
            self.batch_writer.stop()
        
        try:
            db = get_db()
            db.close()
        except RuntimeError:
            pass
        
        print("[Pipeline] Shutdown complete")


def main():
    """Main entry point."""
    args = parse_args()
    
    # Load configuration
    print(f"[Pipeline] Loading configuration from {args.config}")
    config = load_config(args.config)
    
    # Create service
    service = DataPipelineService(config, args.simulator_url)
    
    # Setup signal handlers
    def handle_signal(signum, frame):
        service.shutdown()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    
    # Start service
    try:
        service.start()
        service.run_dashboard(port=args.port)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"[Pipeline] Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        service.shutdown()


if __name__ == "__main__":
    main()

"""Main entry point for Virtual Device Simulator"""

import argparse
import signal
import sys
import threading
import time
from typing import Optional

import uvicorn

from .config import load_config, AppConfig
from .kafka_producer import init_producer, VirtualDeviceKafkaProducer
from .device_manager import DeviceManager
from .dashboard import app, set_device_manager


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Virtual Device Simulator - Generate fake device data for Kafka"
    )
    parser.add_argument(
        "--config", "-c",
        default="config/settings.yaml",
        help="Path to configuration file (default: config/settings.yaml)"
    )
    parser.add_argument(
        "--no-dashboard",
        action="store_true",
        help="Run without the web dashboard"
    )
    parser.add_argument(
        "--port", "-p",
        type=int,
        default=None,
        help="Dashboard port (overrides config)"
    )
    return parser.parse_args()


class VirtualDeviceService:
    """Main service that orchestrates all components."""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.kafka_producer: Optional[VirtualDeviceKafkaProducer] = None
        self.device_manager: Optional[DeviceManager] = None
        self._shutdown_event = threading.Event()
    
    def start(self):
        """Start all service components."""
        print("=" * 60)
        print("  Virtual Device Simulator")
        print("=" * 60)
        print()
        
        # Initialize Kafka producer
        print("[Main] Initializing Kafka producer...")
        topics = {
            "wearable": self.config.kafka.topics.get("wearables", "virtual-wearables"),
            "restaurant": self.config.kafka.topics.get("restaurants", "virtual-restaurants"),
            "gps": self.config.kafka.topics.get("gps", "virtual-gps")
        }
        self.kafka_producer = init_producer(
            self.config.kafka.bootstrap_servers,
            topics
        )
        
        # Initialize device manager
        print("[Main] Initializing device manager...")
        self.device_manager = DeviceManager(self.config, self.kafka_producer)
        
        # Set device manager for dashboard
        set_device_manager(self.device_manager)
        
        # Spawn initial devices
        print("[Main] Spawning initial devices...")
        self.device_manager.spawn_initial_devices()
        
        print()
        print(f"[Main] Kafka topics:")
        for device_type, topic in topics.items():
            print(f"  - {device_type}: {topic}")
        print()
        
        device_counts = self.device_manager.get_device_count()
        print(f"[Main] Devices spawned:")
        print(f"  - Wearables: {device_counts.get('wearable', 0)}")
        print(f"  - Restaurants: {device_counts.get('restaurant', 0)}")
        print(f"  - GPS: {device_counts.get('gps', 0)}")
        print(f"  - Total: {device_counts.get('total', 0)}")
        print()
    
    def run_dashboard(self, port: Optional[int] = None):
        """Run the FastAPI dashboard."""
        dashboard_port = port or self.config.monitoring.dashboard_port
        print(f"[Main] Starting dashboard on http://localhost:{dashboard_port}")
        print()
        
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=dashboard_port,
            log_level="warning"
        )
    
    def wait_for_shutdown(self):
        """Wait for shutdown signal."""
        print("[Main] Press Ctrl+C to stop...")
        try:
            while not self._shutdown_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            pass
    
    def shutdown(self):
        """Shutdown all components."""
        print()
        print("[Main] Shutting down...")
        
        self._shutdown_event.set()
        
        if self.device_manager:
            self.device_manager.shutdown()
        
        if self.kafka_producer:
            self.kafka_producer.close()
        
        print("[Main] Shutdown complete")


def main():
    """Main entry point."""
    args = parse_args()
    
    # Load configuration
    print(f"[Main] Loading configuration from {args.config}")
    try:
        config = load_config(args.config)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    # Create service
    service = VirtualDeviceService(config)
    
    # Setup signal handlers
    def handle_signal(signum, frame):
        service.shutdown()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    
    # Start service
    try:
        service.start()
        
        if args.no_dashboard:
            service.wait_for_shutdown()
        else:
            service.run_dashboard(port=args.port)
    except KeyboardInterrupt:
        pass
    finally:
        service.shutdown()


if __name__ == "__main__":
    main()

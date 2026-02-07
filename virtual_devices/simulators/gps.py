"""GPS telemetry simulator with realistic routes"""

import random
import math
import uuid
from typing import Dict, Any, Optional, Callable, List, Tuple
from datetime import datetime, timezone

from .base import BaseDeviceSimulator


# Predefined routes for Indian cities
DEFAULT_ROUTES = {
    "bangalore": [
        (12.9716, 77.5946),   # MG Road
        (12.9352, 77.6245),   # Koramangala
        (12.9279, 77.6271),   # BTM Layout
        (12.9698, 77.7500),   # Whitefield
        (13.0358, 77.5970),   # Hebbal
        (12.9716, 77.5946),   # Back to MG Road (loop)
    ],
    "mumbai": [
        (19.0760, 72.8777),   # Gateway of India
        (19.0176, 72.8561),   # Nariman Point
        (19.1136, 72.8697),   # Bandra
        (19.1334, 72.9133),   # Andheri
        (19.2183, 72.9781),   # Thane
    ],
    "delhi": [
        (28.6139, 77.2090),   # Connaught Place
        (28.6280, 77.2189),   # Red Fort
        (28.5535, 77.2588),   # Lotus Temple
        (28.5355, 77.2410),   # Nehru Place
        (28.6692, 77.4538),   # Noida
    ]
}


class GPSSimulator(BaseDeviceSimulator):
    """
    Simulates IoT GPS telemetry with realistic movement.
    
    Features:
    - Follows predefined city routes
    - Smooth interpolation between waypoints
    - Realistic speed and heading calculations
    - Support for different vehicle types
    """
    
    DEVICE_TYPE = "gps"
    
    # Vehicle types with speed ranges (km/h)
    VEHICLE_SPEEDS = {
        "truck": (20, 60),
        "car": (30, 80),
        "bike": (20, 50),
        "scooter": (15, 40)
    }
    
    def __init__(
        self,
        device_id: Optional[str] = None,
        frequency_seconds: float = 1.0,
        bad_data_probability: float = 0.02,
        on_data_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        vehicle_id: Optional[str] = None,
        vehicle_type: str = "car",
        route: Optional[List[Tuple[float, float]]] = None,
        city: str = "bangalore"
    ):
        super().__init__(device_id, frequency_seconds, bad_data_probability, on_data_callback)
        
        self.vehicle_id = vehicle_id or f"VH-{uuid.uuid4().hex[:6].upper()}"
        self.vehicle_type = vehicle_type if vehicle_type in self.VEHICLE_SPEEDS else "car"
        
        # Set up route
        if route:
            self.route = route
        else:
            self.route = DEFAULT_ROUTES.get(city, DEFAULT_ROUTES["bangalore"])
        
        # Current position tracking
        self._current_waypoint_idx = 0
        self._progress = 0.0  # 0 to 1 between waypoints
        self._current_speed = random.uniform(*self.VEHICLE_SPEEDS[self.vehicle_type])
        self._altitude = random.uniform(800, 1000)  # meters
    
    def _interpolate_position(self) -> Tuple[float, float]:
        """Interpolate position between current and next waypoint."""
        current = self.route[self._current_waypoint_idx]
        next_idx = (self._current_waypoint_idx + 1) % len(self.route)
        next_wp = self.route[next_idx]
        
        lat = current[0] + (next_wp[0] - current[0]) * self._progress
        lng = current[1] + (next_wp[1] - current[1]) * self._progress
        
        return (lat, lng)
    
    def _calculate_heading(self) -> float:
        """Calculate heading in degrees from current to next waypoint."""
        current = self.route[self._current_waypoint_idx]
        next_idx = (self._current_waypoint_idx + 1) % len(self.route)
        next_wp = self.route[next_idx]
        
        delta_lng = next_wp[1] - current[1]
        delta_lat = next_wp[0] - current[0]
        
        heading = math.atan2(delta_lng, delta_lat) * 180 / math.pi
        if heading < 0:
            heading += 360
        
        return round(heading, 1)
    
    def _advance_position(self):
        """Advance position based on speed and frequency."""
        # Approximate distance covered per tick (simplified)
        distance_per_second = self._current_speed / 3600  # km per second
        
        # Advance progress (simplified - assumes ~1km between waypoints)
        self._progress += distance_per_second * self.frequency_seconds
        
        # Move to next waypoint if reached
        if self._progress >= 1.0:
            self._progress = 0.0
            self._current_waypoint_idx = (self._current_waypoint_idx + 1) % len(self.route)
            # Vary speed at waypoint (traffic simulation)
            min_speed, max_speed = self.VEHICLE_SPEEDS[self.vehicle_type]
            self._current_speed = random.uniform(min_speed, max_speed)
        
        # Slight altitude variation
        self._altitude += random.uniform(-2, 2)
    
    def generate_normal_data(self) -> Dict[str, Any]:
        """Generate normal GPS telemetry data."""
        lat, lng = self._interpolate_position()
        heading = self._calculate_heading()
        
        # Advance for next reading
        self._advance_position()
        
        return {
            "vehicle_id": self.vehicle_id,
            "vehicle_type": self.vehicle_type,
            "latitude": round(lat, 6),
            "longitude": round(lng, 6),
            "altitude": round(self._altitude, 1),
            "speed_kmh": round(self._current_speed + random.uniform(-2, 2), 1),
            "heading": heading,
            "accuracy_meters": round(random.uniform(3, 15), 1),
            "satellites": random.randint(6, 12)
        }
    
    def generate_bad_data(self) -> Dict[str, Any]:
        """Generate anomalous/bad GPS data."""
        bad_type = random.choice(["invalid_coords", "extreme_speed", "null_coords", "out_of_range"])
        
        if bad_type == "invalid_coords":
            return {
                "vehicle_id": self.vehicle_id,
                "vehicle_type": self.vehicle_type,
                "latitude": random.uniform(100, 200),  # Invalid latitude
                "longitude": random.uniform(200, 300),  # Invalid longitude
                "altitude": self._altitude,
                "speed_kmh": self._current_speed,
                "heading": random.uniform(0, 360),
                "accuracy_meters": random.uniform(3, 15),
                "satellites": random.randint(6, 12)
            }
        
        elif bad_type == "extreme_speed":
            return {
                "vehicle_id": self.vehicle_id,
                "vehicle_type": self.vehicle_type,
                "latitude": self.route[0][0],
                "longitude": self.route[0][1],
                "altitude": self._altitude,
                "speed_kmh": random.uniform(500, 1000),  # Impossible speed
                "heading": random.uniform(0, 360),
                "accuracy_meters": random.uniform(3, 15),
                "satellites": random.randint(6, 12)
            }
        
        elif bad_type == "null_coords":
            return {
                "vehicle_id": self.vehicle_id,
                "vehicle_type": self.vehicle_type,
                "latitude": None,
                "longitude": None,
                "altitude": None,
                "speed_kmh": None,
                "heading": None,
                "accuracy_meters": None,
                "satellites": 0
            }
        
        else:  # out_of_range
            return {
                "vehicle_id": self.vehicle_id,
                "vehicle_type": self.vehicle_type,
                "latitude": -100.0,  # Invalid
                "longitude": 190.0,  # Invalid
                "altitude": -500,  # Below sea level significantly
                "speed_kmh": -50,  # Negative speed
                "heading": 500,  # Invalid heading
                "accuracy_meters": -10,
                "satellites": -1
            }

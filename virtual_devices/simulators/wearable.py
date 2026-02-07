"""Wearable device simulator - BP, blood sugar, heart rate"""

import random
from typing import Dict, Any, Optional, Callable
from faker import Faker

from .base import BaseDeviceSimulator


fake = Faker()


class WearableSimulator(BaseDeviceSimulator):
    """
    Simulates wearable health device data.
    
    Generates:
    - Blood Pressure (systolic/diastolic)
    - Blood Sugar levels
    - Heart Rate
    """
    
    DEVICE_TYPE = "wearable"
    
    # Normal ranges
    BP_SYSTOLIC_RANGE = (90, 140)
    BP_DIASTOLIC_RANGE = (60, 90)
    BLOOD_SUGAR_RANGE = (70, 140)
    HEART_RATE_RANGE = (60, 100)
    
    def __init__(
        self,
        device_id: Optional[str] = None,
        frequency_seconds: float = 2.0,
        bad_data_probability: float = 0.05,
        on_data_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        user_id: Optional[str] = None
    ):
        super().__init__(device_id, frequency_seconds, bad_data_probability, on_data_callback)
        self.user_id = user_id or f"user_{fake.uuid4()[:8]}"
    
    def generate_normal_data(self) -> Dict[str, Any]:
        """Generate normal health metrics within healthy ranges."""
        systolic = random.randint(*self.BP_SYSTOLIC_RANGE)
        diastolic = random.randint(*self.BP_DIASTOLIC_RANGE)
        
        # Ensure systolic > diastolic (realistic)
        if systolic <= diastolic:
            systolic = diastolic + random.randint(20, 50)
        
        return {
            "user_id": self.user_id,
            "blood_pressure": {
                "systolic": systolic,
                "diastolic": diastolic,
                "unit": "mmHg"
            },
            "blood_sugar": {
                "value": random.randint(*self.BLOOD_SUGAR_RANGE),
                "unit": "mg/dL"
            },
            "heart_rate": {
                "value": random.randint(*self.HEART_RATE_RANGE),
                "unit": "bpm"
            }
        }
    
    def generate_bad_data(self) -> Dict[str, Any]:
        """Generate anomalous/bad health data."""
        bad_type = random.choice(["negative", "extreme", "null", "invalid_type"])
        
        if bad_type == "negative":
            return {
                "user_id": self.user_id,
                "blood_pressure": {
                    "systolic": random.randint(-50, -1),
                    "diastolic": random.randint(-30, -1),
                    "unit": "mmHg"
                },
                "blood_sugar": {
                    "value": random.randint(-100, -1),
                    "unit": "mg/dL"
                },
                "heart_rate": {
                    "value": random.randint(-50, -1),
                    "unit": "bpm"
                }
            }
        
        elif bad_type == "extreme":
            return {
                "user_id": self.user_id,
                "blood_pressure": {
                    "systolic": random.randint(300, 500),
                    "diastolic": random.randint(200, 300),
                    "unit": "mmHg"
                },
                "blood_sugar": {
                    "value": random.randint(500, 1000),
                    "unit": "mg/dL"
                },
                "heart_rate": {
                    "value": random.randint(250, 400),
                    "unit": "bpm"
                }
            }
        
        elif bad_type == "null":
            return {
                "user_id": self.user_id,
                "blood_pressure": None,
                "blood_sugar": None,
                "heart_rate": None
            }
        
        else:  # invalid_type
            return {
                "user_id": self.user_id,
                "blood_pressure": "not_a_number",
                "blood_sugar": {"invalid": True},
                "heart_rate": [1, 2, 3]
            }

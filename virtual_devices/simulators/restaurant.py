"""Restaurant order simulator"""

import random
import uuid
from typing import Dict, Any, Optional, Callable, List
from datetime import datetime, timezone
from enum import Enum

from .base import BaseDeviceSimulator


class OrderStatus(str, Enum):
    ORDER_PLACED = "ORDER_PLACED"
    ORDER_UPDATED = "ORDER_UPDATED"
    ORDER_COMPLETED = "ORDER_COMPLETED"
    PAYMENT_PENDING = "PAYMENT_PENDING"
    PAYMENT_COMPLETED = "PAYMENT_COMPLETED"
    CANCELLED = "CANCELLED"


class RestaurantSimulator(BaseDeviceSimulator):
    """
    Simulates restaurant order flow.
    
    Generates order lifecycle events from order placement to payment completion.
    Tracks active tables/orders and simulates realistic order flows.
    """
    
    DEVICE_TYPE = "restaurant"
    
    # Dish codes (will be mapped to actual names later via DB)
    DISH_CODES = [f"D{i:03d}" for i in range(101, 151)]  # D101 to D150
    
    # Table configuration
    MAX_TABLES = 20
    
    def __init__(
        self,
        device_id: Optional[str] = None,
        frequency_seconds: float = 5.0,
        bad_data_probability: float = 0.03,
        on_data_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        restaurant_id: Optional[str] = None
    ):
        super().__init__(device_id, frequency_seconds, bad_data_probability, on_data_callback)
        self.restaurant_id = restaurant_id or f"rest_{uuid.uuid4().hex[:6]}"
        
        # Track active orders per table
        self._active_orders: Dict[str, Dict[str, Any]] = {}
    
    def _generate_order_items(self, count: int = None) -> List[Dict[str, Any]]:
        """Generate random order items."""
        if count is None:
            count = random.randint(1, 5)
        
        items = []
        for _ in range(count):
            items.append({
                "dish_code": random.choice(self.DISH_CODES),
                "quantity": random.randint(1, 3)
            })
        return items
    
    def _get_or_create_order(self, table_id: str) -> Dict[str, Any]:
        """Get existing order or create new one for a table."""
        if table_id not in self._active_orders:
            self._active_orders[table_id] = {
                "order_id": f"ORD-{datetime.now().strftime('%Y%m%d')}-{uuid.uuid4().hex[:6].upper()}",
                "items": self._generate_order_items(),
                "status": OrderStatus.ORDER_PLACED,
                "created_at": datetime.now(timezone.utc).isoformat()
            }
        return self._active_orders[table_id]
    
    def generate_normal_data(self) -> Dict[str, Any]:
        """Generate normal restaurant order event."""
        table_id = f"T-{random.randint(1, self.MAX_TABLES):02d}"
        
        # Decide what kind of event to generate
        if table_id in self._active_orders:
            order = self._active_orders[table_id]
            current_status = order["status"]
            
            # Progress the order lifecycle
            if current_status == OrderStatus.ORDER_PLACED:
                if random.random() < 0.3:
                    # Add more items
                    order["items"].extend(self._generate_order_items(random.randint(1, 2)))
                    order["status"] = OrderStatus.ORDER_UPDATED
                else:
                    order["status"] = OrderStatus.ORDER_COMPLETED
            
            elif current_status == OrderStatus.ORDER_UPDATED:
                order["status"] = OrderStatus.ORDER_COMPLETED
            
            elif current_status == OrderStatus.ORDER_COMPLETED:
                order["status"] = OrderStatus.PAYMENT_PENDING
                # Calculate total (mock - just item count * random price)
                order["total_amount"] = round(
                    sum(item["quantity"] for item in order["items"]) * random.uniform(150, 500),
                    2
                )
            
            elif current_status == OrderStatus.PAYMENT_PENDING:
                order["status"] = OrderStatus.PAYMENT_COMPLETED
                # Clear the table after payment
                del self._active_orders[table_id]
        else:
            # New order
            order = self._get_or_create_order(table_id)
        
        return {
            "restaurant_id": self.restaurant_id,
            "table_id": table_id,
            "order_id": order["order_id"],
            "status": order["status"].value if isinstance(order["status"], OrderStatus) else order["status"],
            "items": order["items"],
            "payment_status": "COMPLETED" if order["status"] == OrderStatus.PAYMENT_COMPLETED else "PENDING",
            "total_amount": order.get("total_amount"),
            "created_at": order["created_at"]
        }
    
    def generate_bad_data(self) -> Dict[str, Any]:
        """Generate anomalous/bad restaurant data."""
        bad_type = random.choice(["invalid_dish", "negative_quantity", "impossible_total", "null_fields", "invalid_status"])
        
        table_id = f"T-{random.randint(1, self.MAX_TABLES):02d}"
        order_id = f"ORD-BAD-{uuid.uuid4().hex[:6].upper()}"
        
        if bad_type == "invalid_dish":
            return {
                "restaurant_id": self.restaurant_id,
                "table_id": table_id,
                "order_id": order_id,
                "status": "ORDER_PLACED",
                "items": [
                    {"dish_code": "INVALID_CODE", "quantity": 1},
                    {"dish_code": "D-999", "quantity": 2},
                    {"dish_code": "", "quantity": 1}
                ],
                "payment_status": "PENDING",
                "total_amount": None
            }
        
        elif bad_type == "negative_quantity":
            return {
                "restaurant_id": self.restaurant_id,
                "table_id": table_id,
                "order_id": order_id,
                "status": "ORDER_PLACED",
                "items": [
                    {"dish_code": "D101", "quantity": -5},
                    {"dish_code": "D102", "quantity": 0}
                ],
                "payment_status": "PENDING",
                "total_amount": None
            }
        
        elif bad_type == "impossible_total":
            return {
                "restaurant_id": self.restaurant_id,
                "table_id": table_id,
                "order_id": order_id,
                "status": "PAYMENT_COMPLETED",
                "items": [{"dish_code": "D101", "quantity": 1}],
                "payment_status": "COMPLETED",
                "total_amount": -500.00  # Negative total
            }
        
        elif bad_type == "null_fields":
            return {
                "restaurant_id": self.restaurant_id,
                "table_id": None,
                "order_id": None,
                "status": None,
                "items": None,
                "payment_status": None,
                "total_amount": None
            }
        
        else:  # invalid_status
            return {
                "restaurant_id": self.restaurant_id,
                "table_id": table_id,
                "order_id": order_id,
                "status": "INVALID_STATUS_XYZ",
                "items": self._generate_order_items(),
                "payment_status": "UNKNOWN",
                "total_amount": "not_a_number"
            }

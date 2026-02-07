"""Comprehensive health check endpoints."""

from typing import Dict, Any, Optional
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter

router = APIRouter(prefix="/health", tags=["Health"])

# Component references (set by main.py)
_kafka_consumer = None
_batch_writer = None
_db_connection = None
_simulator_url: Optional[str] = None


def set_components(kafka_consumer, batch_writer, db_connection, simulator_url: str = None):
    """Set references to components for health checking."""
    global _kafka_consumer, _batch_writer, _db_connection, _simulator_url
    _kafka_consumer = kafka_consumer
    _batch_writer = batch_writer
    _db_connection = db_connection
    _simulator_url = simulator_url or "http://localhost:8080"


@router.get("")
async def health_check() -> Dict[str, Any]:
    """Aggregated health check for all components."""
    checks = {
        "kafka": await kafka_health(),
        "database": await db_health(),
        "writer": await writer_health()
    }
    
    # Check external simulator if configured
    if _simulator_url:
        checks["simulator"] = await simulator_health()
    
    # Overall status
    all_healthy = all(c.get("status") == "healthy" for c in checks.values())
    
    return {
        "status": "healthy" if all_healthy else "unhealthy",
        "service": "data-pipeline",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "components": checks
    }


@router.get("/live")
async def liveness() -> Dict[str, str]:
    """Simple liveness probe."""
    return {"status": "alive"}


@router.get("/ready")
async def readiness() -> Dict[str, Any]:
    """Readiness probe - checks if service can handle requests."""
    ready = True
    details = {}
    
    if _db_connection:
        db_health = _db_connection.check_health()
        ready = ready and db_health.get("status") == "healthy"
        details["database"] = db_health.get("status", "unknown")
    
    if _kafka_consumer:
        kafka_stats = _kafka_consumer.check_health()
        ready = ready and kafka_stats.get("status") == "healthy"
        details["kafka"] = kafka_stats.get("status", "unknown")
    
    return {
        "status": "ready" if ready else "not_ready",
        "details": details
    }


@router.get("/kafka")
async def kafka_health() -> Dict[str, Any]:
    """Kafka consumer health details."""
    if not _kafka_consumer:
        return {"status": "not_initialized"}
    
    return _kafka_consumer.check_health()


@router.get("/db")
async def db_health() -> Dict[str, Any]:
    """Database health details."""
    if not _db_connection:
        return {"status": "not_initialized"}
    
    return _db_connection.check_health()


@router.get("/writer")
async def writer_health() -> Dict[str, Any]:
    """Batch writer health details."""
    if not _batch_writer:
        return {"status": "not_initialized"}
    
    return _batch_writer.check_health()


@router.get("/simulator")
async def simulator_health() -> Dict[str, Any]:
    """Check Virtual Device Simulator health."""
    if not _simulator_url:
        return {"status": "not_configured"}
    
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(f"{_simulator_url}/health")
            
            if response.status_code == 200:
                return {
                    "status": "healthy",
                    "url": _simulator_url,
                    "response": response.json()
                }
            else:
                return {
                    "status": "unhealthy",
                    "url": _simulator_url,
                    "status_code": response.status_code
                }
    except httpx.TimeoutException:
        return {"status": "unhealthy", "error": "timeout"}
    except httpx.ConnectError:
        return {"status": "unhealthy", "error": "connection_refused"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

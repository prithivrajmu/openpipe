"""TimescaleDB connection pool management."""

import threading
from typing import Optional, Dict, Any
from contextlib import contextmanager

from sqlalchemy import create_engine, text, event
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool


class DatabaseConnection:
    """
    Thread-safe TimescaleDB connection manager.
    
    Features:
    - Connection pooling
    - Automatic reconnection
    - Health checking
    """
    
    def __init__(self, connection_string: str, pool_size: int = 5):
        self.connection_string = connection_string
        self.pool_size = pool_size
        
        self._engine = None
        self._session_factory = None
        self._lock = threading.Lock()
        self._connected = False
        
        # Stats
        self._queries_executed = 0
        self._errors = 0
    
    def connect(self) -> bool:
        """Establish database connection."""
        if self._engine is not None:
            return True
        
        with self._lock:
            if self._engine is not None:
                return True
            
            try:
                self._engine = create_engine(
                    self.connection_string,
                    poolclass=QueuePool,
                    pool_size=self.pool_size,
                    max_overflow=10,
                    pool_pre_ping=True,
                    pool_recycle=3600
                )
                
                self._session_factory = sessionmaker(bind=self._engine)
                
                # Test connection
                with self._engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                
                self._connected = True
                print(f"[Database] Connected to TimescaleDB")
                return True
                
            except Exception as e:
                self._errors += 1
                print(f"[Database] Connection failed: {e}")
                return False
    
    @contextmanager
    def session(self):
        """Get a database session."""
        if not self._connected:
            self.connect()
        
        session = self._session_factory()
        try:
            yield session
            session.commit()
            self._queries_executed += 1
        except Exception as e:
            session.rollback()
            self._errors += 1
            raise
        finally:
            session.close()
    
    def execute(self, query: str, params: Optional[Dict[str, Any]] = None):
        """Execute a raw SQL query."""
        with self.session() as session:
            result = session.execute(text(query), params or {})
            return result
    
    def check_health(self) -> Dict[str, Any]:
        """Check database health."""
        try:
            with self._engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
            
            pool_status = self._engine.pool.status()
            
            return {
                "status": "healthy",
                "connected": self._connected,
                "pool_size": self.pool_size,
                "pool_status": pool_status,
                "queries_executed": self._queries_executed,
                "errors": self._errors
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "connected": False,
                "error": str(e),
                "errors": self._errors
            }
    
    def close(self):
        """Close all connections."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
            self._connected = False
            print("[Database] Closed all connections")
    
    @property
    def is_connected(self) -> bool:
        return self._connected


# Global connection instance
_db: Optional[DatabaseConnection] = None


def get_db() -> DatabaseConnection:
    """Get the global database connection."""
    global _db
    if _db is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _db


def init_db(connection_string: str) -> DatabaseConnection:
    """Initialize the global database connection."""
    global _db
    _db = DatabaseConnection(connection_string)
    _db.connect()
    return _db

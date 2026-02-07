"""FastAPI routes for data viewing and SQL query interface."""

import json
from typing import Dict, Any, List, Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from ..db.connection import get_db
from ..db.models import TABLE_DEFINITIONS, get_table_stats

router = APIRouter(tags=["Data"])


class SQLQueryRequest(BaseModel):
    """SQL query request body."""
    query: str
    limit: int = 100


class SQLQueryResponse(BaseModel):
    """SQL query response."""
    columns: List[str]
    rows: List[Dict[str, Any]]
    row_count: int
    execution_time_ms: float


@router.get("/tables")
async def list_tables() -> Dict[str, Any]:
    """List all available tables."""
    tables = list(TABLE_DEFINITIONS.keys())
    stats = get_table_stats()
    
    return {
        "tables": tables,
        "stats": stats
    }


@router.get("/tables/{table_name}")
async def get_table_data(
    table_name: str,
    limit: int = Query(default=50, le=1000),
    offset: int = Query(default=0, ge=0),
    device_id: Optional[str] = None
) -> Dict[str, Any]:
    """Get data from a specific table with pagination."""
    if table_name not in TABLE_DEFINITIONS:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    
    db = get_db()
    
    try:
        # Build query
        query = f"SELECT * FROM {table_name}"
        params = {}
        
        if device_id:
            query += " WHERE device_id = :device_id"
            params["device_id"] = device_id
        
        query += " ORDER BY ingested_at DESC LIMIT :limit OFFSET :offset"
        params["limit"] = limit
        params["offset"] = offset
        
        from sqlalchemy import text
        with db.session() as session:
            result = session.execute(text(query), params)
            columns = list(result.keys())
            rows = []
            for row in result.fetchall():
                row_dict = {}
                for i, col in enumerate(columns):
                    val = row[i]
                    # Convert datetime to string, handle JSONB
                    if isinstance(val, datetime):
                        val = val.isoformat()
                    elif col == "raw_data" and val:
                        val = json.loads(val) if isinstance(val, str) else val
                    row_dict[col] = val
                rows.append(row_dict)
            
            # Get total count
            count_result = session.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            total = count_result.scalar()
        
        return {
            "table": table_name,
            "columns": columns,
            "rows": rows,
            "total": total,
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/query")
async def execute_query(request: SQLQueryRequest) -> SQLQueryResponse:
    """Execute a custom SQL query (read-only)."""
    query = request.query.strip()
    
    # Basic safety check - only allow SELECT queries
    if not query.upper().startswith("SELECT"):
        raise HTTPException(
            status_code=400, 
            detail="Only SELECT queries are allowed"
        )
    
    # Check for dangerous patterns
    dangerous = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE", "CREATE"]
    query_upper = query.upper()
    for pattern in dangerous:
        if pattern in query_upper:
            raise HTTPException(
                status_code=400,
                detail=f"Query contains forbidden keyword: {pattern}"
            )
    
    db = get_db()
    
    try:
        import time
        start = time.time()
        
        # Add limit if not present
        if "LIMIT" not in query_upper:
            query = f"{query} LIMIT {request.limit}"
        
        from sqlalchemy import text
        with db.session() as session:
            result = session.execute(text(query))
            columns = list(result.keys())
            rows = []
            for row in result.fetchall():
                row_dict = {}
                for i, col in enumerate(columns):
                    val = row[i]
                    if isinstance(val, datetime):
                        val = val.isoformat()
                    elif isinstance(val, dict):
                        pass  # Keep as-is
                    elif col == "raw_data" and val:
                        try:
                            val = json.loads(val) if isinstance(val, str) else val
                        except:
                            pass
                    row_dict[col] = val
                rows.append(row_dict)
        
        elapsed = (time.time() - start) * 1000
        
        return SQLQueryResponse(
            columns=columns,
            rows=rows,
            row_count=len(rows),
            execution_time_ms=round(elapsed, 2)
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

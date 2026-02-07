"""FastAPI dashboard for virtual device monitoring"""

import os
from pathlib import Path
from typing import Dict, Any, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from .metrics import get_metrics_collector
from .device_manager import DeviceManager


# Store device manager reference
_device_manager: Optional[DeviceManager] = None

app = FastAPI(
    title="Virtual Device Simulator Dashboard",
    description="Monitor and manage virtual device simulators",
    version="0.1.0"
)

# HTML template for dashboard
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Virtual Device Simulator Dashboard</title>
    <style>
        :root {
            --primary: #6366f1;
            --primary-dark: #4f46e5;
            --success: #10b981;
            --warning: #f59e0b;
            --danger: #ef4444;
            --bg: #0f172a;
            --card-bg: #1e293b;
            --text: #f8fafc;
            --text-muted: #94a3b8;
            --border: #334155;
        }
        
        * { box-sizing: border-box; margin: 0; padding: 0; }
        
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background: var(--bg);
            color: var(--text);
            min-height: 100vh;
            line-height: 1.6;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 2rem;
        }
        
        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 2rem;
            padding-bottom: 1.5rem;
            border-bottom: 1px solid var(--border);
        }
        
        h1 {
            font-size: 1.75rem;
            font-weight: 700;
            background: linear-gradient(135deg, var(--primary), #a855f7);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        
        .status-badge {
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
            padding: 0.5rem 1rem;
            border-radius: 9999px;
            font-size: 0.875rem;
            font-weight: 500;
        }
        
        .status-running {
            background: rgba(16, 185, 129, 0.15);
            color: var(--success);
            border: 1px solid rgba(16, 185, 129, 0.3);
        }
        
        .pulse {
            width: 8px;
            height: 8px;
            background: var(--success);
            border-radius: 50%;
            animation: pulse 2s infinite;
        }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        
        .summary-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            margin-bottom: 2rem;
        }
        
        .summary-card {
            background: var(--card-bg);
            border-radius: 12px;
            padding: 1.5rem;
            border: 1px solid var(--border);
            transition: transform 0.2s, box-shadow 0.2s;
        }
        
        .summary-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 24px rgba(0, 0, 0, 0.3);
        }
        
        .summary-card h3 {
            color: var(--text-muted);
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 0.5rem;
        }
        
        .summary-card .value {
            font-size: 2rem;
            font-weight: 700;
        }
        
        .summary-card .subtitle {
            color: var(--text-muted);
            font-size: 0.875rem;
        }
        
        .device-type-section {
            margin-bottom: 2rem;
        }
        
        .section-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1rem;
        }
        
        .section-header h2 {
            font-size: 1.25rem;
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }
        
        .section-header .count {
            background: var(--primary);
            color: white;
            font-size: 0.75rem;
            padding: 0.25rem 0.75rem;
            border-radius: 9999px;
        }
        
        .devices-table {
            width: 100%;
            background: var(--card-bg);
            border-radius: 12px;
            border: 1px solid var(--border);
            overflow: hidden;
        }
        
        .devices-table th,
        .devices-table td {
            padding: 1rem;
            text-align: left;
        }
        
        .devices-table th {
            background: rgba(99, 102, 241, 0.1);
            color: var(--text-muted);
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            font-weight: 600;
        }
        
        .devices-table tr:not(:last-child) td {
            border-bottom: 1px solid var(--border);
        }
        
        .devices-table tbody tr:hover {
            background: rgba(99, 102, 241, 0.05);
        }
        
        .btn {
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
            padding: 0.5rem 1rem;
            border-radius: 8px;
            font-size: 0.875rem;
            font-weight: 500;
            cursor: pointer;
            border: none;
            transition: all 0.2s;
        }
        
        .btn-primary {
            background: var(--primary);
            color: white;
        }
        
        .btn-primary:hover {
            background: var(--primary-dark);
        }
        
        .btn-danger {
            background: rgba(239, 68, 68, 0.1);
            color: var(--danger);
            border: 1px solid rgba(239, 68, 68, 0.3);
        }
        
        .btn-danger:hover {
            background: var(--danger);
            color: white;
        }
        
        .refresh-time {
            color: var(--text-muted);
            font-size: 0.875rem;
        }
        
        .emoji { font-size: 1.25rem; }
        
        .bad-data-indicator {
            color: var(--warning);
            font-size: 0.875rem;
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🔌 Virtual Device Simulator</h1>
            <div class="status-badge status-running">
                <span class="pulse"></span>
                Running
            </div>
        </header>
        
        <div class="summary-grid" id="summary">
            <!-- Populated by JS -->
        </div>
        
        <div id="device-sections">
            <!-- Populated by JS -->
        </div>
        
        <p class="refresh-time">Auto-refreshing every 2 seconds...</p>
    </div>
    
    <script>
        const DEVICE_ICONS = {
            'wearable': '⌚',
            'restaurant': '🍽️',
            'gps': '📍'
        };
        
        async function fetchMetrics() {
            const response = await fetch('/api/metrics');
            return response.json();
        }
        
        function renderSummary(metrics) {
            const summary = document.getElementById('summary');
            summary.innerHTML = `
                <div class="summary-card">
                    <h3>Total Devices</h3>
                    <div class="value">${metrics.total_devices}</div>
                    <div class="subtitle">Active simulators</div>
                </div>
                <div class="summary-card">
                    <h3>Data Points</h3>
                    <div class="value">${metrics.total_data_points.toLocaleString()}</div>
                    <div class="subtitle">Messages sent</div>
                </div>
                <div class="summary-card">
                    <h3>Bad Data</h3>
                    <div class="value bad-data-indicator">${metrics.total_bad_data_points.toLocaleString()}</div>
                    <div class="subtitle">${metrics.overall_bad_data_rate}% rate</div>
                </div>
                <div class="summary-card">
                    <h3>Uptime</h3>
                    <div class="value">${Math.floor(metrics.uptime_seconds / 60)}m</div>
                    <div class="subtitle">${Math.floor(metrics.uptime_seconds % 60)}s</div>
                </div>
            `;
        }
        
        function renderDeviceSections(metrics) {
            const sections = document.getElementById('device-sections');
            let html = '';
            
            for (const [type, data] of Object.entries(metrics.by_type)) {
                const icon = DEVICE_ICONS[type] || '📟';
                html += `
                    <div class="device-type-section">
                        <div class="section-header">
                            <h2>
                                <span class="emoji">${icon}</span>
                                ${type.charAt(0).toUpperCase() + type.slice(1)} Devices
                                <span class="count">${data.active_devices}</span>
                            </h2>
                            <button class="btn btn-primary" onclick="spawnDevice('${type}')">
                                + Spawn Device
                            </button>
                        </div>
                        <table class="devices-table">
                            <thead>
                                <tr>
                                    <th>Device ID</th>
                                    <th>Data Points</th>
                                    <th>Bad Data</th>
                                    <th>Errors</th>
                                    <th>Last Sent</th>
                                    <th>Actions</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${data.devices.map(d => `
                                    <tr>
                                        <td><code>${d.device_id}</code></td>
                                        <td>${d.data_points_sent.toLocaleString()}</td>
                                        <td class="bad-data-indicator">${d.bad_data_points}</td>
                                        <td>${d.errors}</td>
                                        <td>${d.last_sent_at ? new Date(d.last_sent_at).toLocaleTimeString() : '-'}</td>
                                        <td>
                                            <button class="btn btn-danger" onclick="stopDevice('${d.device_id}')">
                                                Stop
                                            </button>
                                        </td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    </div>
                `;
            }
            
            sections.innerHTML = html;
        }
        
        async function spawnDevice(type) {
            await fetch(`/api/devices/${type}/spawn`, { method: 'POST' });
            refresh();
        }
        
        async function stopDevice(deviceId) {
            await fetch(`/api/devices/${deviceId}`, { method: 'DELETE' });
            refresh();
        }
        
        async function refresh() {
            const metrics = await fetchMetrics();
            renderSummary(metrics);
            renderDeviceSections(metrics);
        }
        
        // Initial load and auto-refresh
        refresh();
        setInterval(refresh, 2000);
    </script>
</body>
</html>
"""


def set_device_manager(manager: DeviceManager):
    """Set the device manager instance for the dashboard."""
    global _device_manager
    _device_manager = manager


def get_device_manager() -> DeviceManager:
    """Get the device manager instance."""
    if _device_manager is None:
        raise HTTPException(status_code=500, detail="Device manager not initialized")
    return _device_manager


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve the dashboard HTML page."""
    return HTMLResponse(content=DASHBOARD_HTML)


@app.get("/api/metrics")
async def get_metrics():
    """Get aggregated metrics for all devices."""
    collector = get_metrics_collector()
    return collector.get_summary()


@app.get("/api/metrics/{device_type}")
async def get_metrics_by_type(device_type: str):
    """Get metrics for a specific device type."""
    collector = get_metrics_collector()
    metrics = collector.get_metrics_by_type(device_type)
    return metrics.to_dict()


@app.get("/api/devices")
async def list_devices():
    """List all active devices."""
    manager = get_device_manager()
    return {
        "devices": manager.get_all_devices(),
        "counts": manager.get_device_count()
    }


@app.get("/api/devices/{device_type}")
async def list_devices_by_type(device_type: str):
    """List devices of a specific type."""
    manager = get_device_manager()
    return manager.get_devices_by_type(device_type)


class SpawnDeviceRequest(BaseModel):
    device_id: Optional[str] = None


@app.post("/api/devices/{device_type}/spawn")
async def spawn_device(device_type: str, request: SpawnDeviceRequest = None):
    """Spawn a new device of the specified type."""
    manager = get_device_manager()
    
    if device_type not in ["wearable", "restaurant", "gps"]:
        raise HTTPException(status_code=400, detail=f"Invalid device type: {device_type}")
    
    device_id = request.device_id if request else None
    new_device_id = manager.spawn_device(device_type, device_id=device_id)
    
    return {"device_id": new_device_id, "device_type": device_type, "status": "running"}


@app.delete("/api/devices/{device_id}")
async def stop_device(device_id: str):
    """Stop and remove a device."""
    manager = get_device_manager()
    
    success = manager.stop_device(device_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Device not found: {device_id}")
    
    return {"device_id": device_id, "status": "stopped"}


@app.get("/api/kafka/stats")
async def get_kafka_stats():
    """Get Kafka producer statistics."""
    manager = get_device_manager()
    return manager.kafka_producer.get_stats()

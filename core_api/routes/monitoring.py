"""
Monitoring endpoints for dashboard integration.
Returns aggregated metrics and logs from all services.
"""

from fastapi import APIRouter, Query
from typing import List, Dict, Any
import httpx
import asyncio
from datetime import datetime, timedelta

router = APIRouter(prefix="/monitoring", tags=["Monitoring"])

PROMETHEUS_URL = "http://localhost:6006"
LOKI_URL = "http://localhost:3100"


@router.get("/services/health")
async def get_services_health():
    """
    Get health status of all services.

    Returns:
        Dict with service health status and uptime
    """
    services = {
        "core-api": {"url": "http://localhost:6001/health", "port": 6001},
        "signal-service": {"url": "http://localhost:6002/health", "port": 6002},
        "intraday-engine": {"url": "http://localhost:6008/health", "port": 6008},
        "dashboard": {"url": "http://localhost:3000", "port": 3000},  # Next.js server
        "dashboard-proxy": {"url": "http://localhost:6003", "port": 6003},  # Reverse proxy
        "mojo-compute": {"tcp_port": 6101},  # TCP socket server
        "news-nlp": {"tcp_port": 6102},  # TCP socket server
        "postgres": {"url": "postgresql://hariprasath@localhost:6432/trading_chitti", "port": 6432},
    }

    results = {}

    for service_name, config in services.items():
        try:
            if "url" in config and config["url"].startswith("http"):
                # HTTP/HTTPS endpoint check
                async with httpx.AsyncClient(timeout=2.0) as client:
                    response = await client.get(config["url"])
                    # Accept 2xx as healthy, 503 as degraded (partially working)
                    if 200 <= response.status_code < 300:
                        results[service_name] = {
                            "status": "healthy",
                            "response_time_ms": response.elapsed.total_seconds() * 1000,
                            "port": config.get("port"),
                            "last_check": datetime.now().isoformat()
                        }
                    elif response.status_code == 503:
                        # Service is partially available (degraded state)
                        results[service_name] = {
                            "status": "degraded",
                            "response_time_ms": response.elapsed.total_seconds() * 1000,
                            "error": "Service degraded (some dependencies unavailable)",
                            "port": config.get("port"),
                            "last_check": datetime.now().isoformat()
                        }
                    else:
                        results[service_name] = {
                            "status": "unhealthy",
                            "error": f"HTTP {response.status_code}",
                            "port": config.get("port"),
                            "last_check": datetime.now().isoformat()
                        }
            elif "tcp_port" in config:
                # TCP port check
                import socket
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2.0)
                result = sock.connect_ex(('localhost', config["tcp_port"]))
                sock.close()

                if result == 0:
                    results[service_name] = {
                        "status": "healthy",
                        "port": config["tcp_port"],
                        "last_check": datetime.now().isoformat()
                    }
                else:
                    results[service_name] = {
                        "status": "unhealthy",
                        "error": f"Port {config['tcp_port']} not reachable",
                        "port": config["tcp_port"],
                        "last_check": datetime.now().isoformat()
                    }
            elif "url" in config and config["url"].startswith("postgresql"):
                # PostgreSQL connection check
                import psycopg2
                try:
                    conn = psycopg2.connect(config["url"], connect_timeout=2)
                    conn.close()
                    results[service_name] = {
                        "status": "healthy",
                        "port": config.get("port"),
                        "last_check": datetime.now().isoformat()
                    }
                except Exception as pg_error:
                    results[service_name] = {
                        "status": "unhealthy",
                        "error": str(pg_error),
                        "port": config.get("port"),
                        "last_check": datetime.now().isoformat()
                    }
        except Exception as e:
            results[service_name] = {
                "status": "unhealthy",
                "error": str(e),
                "port": config.get("port"),
                "last_check": datetime.now().isoformat()
            }

    return {
        "services": results,
        "timestamp": datetime.now().isoformat()
    }


@router.get("/metrics/request-rate")
async def get_request_rate():
    """
    Get request rate for all services from Prometheus.

    Returns:
        Request rate metrics per service
    """
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            # Query Prometheus for request rates
            query = 'rate(http_requests_total[5m])'
            response = await client.get(
                f"{PROMETHEUS_URL}/api/v1/query",
                params={"query": query}
            )

            if response.status_code == 200:
                data = response.json()
                metrics = []

                if data.get("status") == "success":
                    for result in data.get("data", {}).get("result", []):
                        metrics.append({
                            "service": result["metric"].get("job", "unknown"),
                            "method": result["metric"].get("method", ""),
                            "endpoint": result["metric"].get("endpoint", ""),
                            "rate": float(result["value"][1])
                        })

                return {
                    "metrics": metrics,
                    "timestamp": datetime.utcnow().isoformat()
                }
    except Exception as e:
        return {
            "error": str(e),
            "metrics": [],
            "timestamp": datetime.utcnow().isoformat()
        }


@router.get("/metrics/response-time")
async def get_response_time():
    """
    Get response time percentiles from Prometheus.

    Returns:
        P50, P95, P99 response times
    """
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            percentiles = {}

            for quantile in [0.5, 0.95, 0.99]:
                query = f'histogram_quantile({quantile}, rate(http_request_duration_seconds_bucket[5m]))'
                response = await client.get(
                    f"{PROMETHEUS_URL}/api/v1/query",
                    params={"query": query}
                )

                if response.status_code == 200:
                    data = response.json()
                    if data.get("status") == "success":
                        results = data.get("data", {}).get("result", [])
                        if results:
                            value = float(results[0]["value"][1])
                            percentiles[f"p{int(quantile * 100)}"] = round(value * 1000, 2)  # Convert to ms

            return {
                "response_times_ms": percentiles,
                "timestamp": datetime.utcnow().isoformat()
            }
    except Exception as e:
        return {
            "error": str(e),
            "response_times_ms": {},
            "timestamp": datetime.utcnow().isoformat()
        }


@router.get("/metrics/error-rate")
async def get_error_rate():
    """
    Get error rate from Prometheus.

    Returns:
        Error rate by service and endpoint
    """
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            # Query for 5xx errors
            query = 'rate(http_requests_total{status=~"5.."}[5m])'
            response = await client.get(
                f"{PROMETHEUS_URL}/api/v1/query",
                params={"query": query}
            )

            if response.status_code == 200:
                data = response.json()
                errors = []

                if data.get("status") == "success":
                    for result in data.get("data", {}).get("result", []):
                        errors.append({
                            "service": result["metric"].get("job", "unknown"),
                            "endpoint": result["metric"].get("endpoint", ""),
                            "status": result["metric"].get("status", ""),
                            "error_rate": float(result["value"][1])
                        })

                return {
                    "errors": errors,
                    "timestamp": datetime.utcnow().isoformat()
                }
    except Exception as e:
        return {
            "error": str(e),
            "errors": [],
            "timestamp": datetime.utcnow().isoformat()
        }


@router.get("/logs/recent")
async def get_recent_logs(
    service: str = Query(None, description="Filter by service name"),
    level: str = Query(None, description="Filter by log level (info, warning, error)"),
    limit: int = Query(100, ge=1, le=1000, description="Number of log entries")
):
    """
    Get recent logs from Loki.

    Args:
        service: Service name filter (core-api, mojo-compute, news-nlp)
        level: Log level filter
        limit: Number of entries to return

    Returns:
        Recent log entries
    """
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            # Build LogQL query
            query_parts = []
            if service:
                query_parts.append(f'job="{service}"')
            if level:
                query_parts.append(f'level="{level}"')

            logql = "{" + ",".join(query_parts) + "}" if query_parts else "{job=~\".+\"}"

            # Query Loki
            response = await client.get(
                f"{LOKI_URL}/loki/api/v1/query_range",
                params={
                    "query": logql,
                    "limit": limit,
                    "start": int((datetime.utcnow() - timedelta(hours=1)).timestamp() * 1e9),
                    "end": int(datetime.utcnow().timestamp() * 1e9)
                }
            )

            if response.status_code == 200:
                data = response.json()
                logs = []

                if data.get("status") == "success":
                    for stream in data.get("data", {}).get("result", []):
                        labels = stream.get("stream", {})
                        for entry in stream.get("values", []):
                            timestamp_ns, log_line = entry
                            timestamp = datetime.fromtimestamp(int(timestamp_ns) / 1e9)

                            logs.append({
                                "timestamp": timestamp.isoformat(),
                                "service": labels.get("job", "unknown"),
                                "level": labels.get("level", "info"),
                                "message": log_line
                            })

                # Sort by timestamp descending
                logs.sort(key=lambda x: x["timestamp"], reverse=True)

                return {
                    "logs": logs[:limit],
                    "total": len(logs),
                    "timestamp": datetime.utcnow().isoformat()
                }
            else:
                # Fallback: read from log files
                return await get_logs_from_files(service, level, limit)

    except Exception as e:
        # Fallback: read from log files
        return await get_logs_from_files(service, level, limit)


def normalize_timestamp(timestamp_str: str) -> str:
    """
    Normalize timestamp to ISO 8601 format that JavaScript can parse.
    Converts Python logging format (2026-02-04 13:30:08,282) to ISO format.
    """
    try:
        # Replace comma with period for milliseconds
        if ',' in timestamp_str:
            timestamp_str = timestamp_str.replace(',', '.')

        # Try to parse and convert to ISO format
        try:
            # Try with milliseconds
            dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
            return dt.isoformat()
        except ValueError:
            # Try without milliseconds
            dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
            return dt.isoformat()
    except Exception:
        # If parsing fails, return as-is
        return timestamp_str


async def get_logs_from_files(service: str = None, level: str = None, limit: int = 100):
    """Fallback: Read logs from files when Loki is unavailable."""
    import os
    from pathlib import Path

    log_dir = Path("/Users/hariprasath/trading-chitti/logs")
    logs = []

    # Determine which log files to read
    if service:
        log_files = [log_dir / f"{service}.log"]
    else:
        log_files = list(log_dir.glob("*.log"))

    # Calculate lines to read per file
    if service:
        lines_per_file = limit
    else:
        # When reading all services, read fewer lines per file to get a mix
        lines_per_file = max(10, limit // max(1, len(log_files)))

    try:
        for log_file in log_files:
            if not log_file.exists():
                continue

            # Read last N lines
            with open(log_file, 'r') as f:
                lines = f.readlines()[-lines_per_file:]

            # Get file modification time for logs without timestamps
            file_mtime = datetime.fromtimestamp(log_file.stat().st_mtime)

            for line_idx, line in enumerate(lines):
                line = line.strip()
                if not line:
                    continue

                # Try to parse different log formats
                log_entry = None

                # Format 1: Python logging (timestamp - name - level - message)
                if ' - ' in line:
                    parts = line.split(' - ', 3)
                    if len(parts) >= 4:
                        timestamp_str, name, log_level, message = parts
                        log_entry = {
                            "timestamp": normalize_timestamp(timestamp_str),
                            "service": log_file.stem,
                            "level": log_level.lower(),
                            "message": message
                        }

                # Format 2: uvicorn/FastAPI (LEVEL:     IP - "request" status)
                if not log_entry and line.startswith(('INFO:', 'DEBUG:', 'WARNING:', 'ERROR:')):
                    # Extract level and message
                    if ':' in line:
                        log_level, rest = line.split(':', 1)
                        # Use file mtime minus offset to preserve order
                        log_time = file_mtime - timedelta(seconds=(len(lines) - line_idx))
                        log_entry = {
                            "timestamp": log_time.isoformat(),
                            "service": log_file.stem,
                            "level": log_level.lower(),
                            "message": rest.strip()
                        }

                # Format 3: Plain text (fallback)
                if not log_entry and line:
                    # Use file mtime minus offset to preserve order
                    log_time = file_mtime - timedelta(seconds=(len(lines) - line_idx))
                    log_entry = {
                        "timestamp": log_time.isoformat(),
                        "service": log_file.stem,
                        "level": "info",
                        "message": line
                    }

                if log_entry:
                    # Filter by level if specified
                    if level and log_entry["level"] != level.lower():
                        continue

                    logs.append(log_entry)

        # Sort by timestamp descending
        logs.sort(key=lambda x: x["timestamp"], reverse=True)

        return {
            "logs": logs[:limit],
            "total": len(logs),
            "source": "log_files",
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        return {
            "error": str(e),
            "logs": [],
            "timestamp": datetime.utcnow().isoformat()
        }


@router.get("/logs/errors")
async def get_error_logs(limit: int = Query(50, ge=1, le=500)):
    """
    Get recent error logs from all services.

    Args:
        limit: Number of error entries to return

    Returns:
        Recent error log entries
    """
    # Directly read error logs from files (don't call get_recent_logs)
    return await get_logs_from_files(service=None, level="error", limit=limit)


@router.get("/system/resources")
async def get_system_resources():
    """
    Get system resource usage (CPU, memory, disk).

    Returns:
        System resource metrics
    """
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            metrics = {}

            # CPU usage
            cpu_query = '100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)'
            cpu_response = await client.get(
                f"{PROMETHEUS_URL}/api/v1/query",
                params={"query": cpu_query}
            )
            if cpu_response.status_code == 200:
                cpu_data = cpu_response.json()
                if cpu_data.get("status") == "success" and cpu_data.get("data", {}).get("result"):
                    cpu_value = float(cpu_data["data"]["result"][0]["value"][1])
                    metrics["cpu_usage_pct"] = round(cpu_value, 2)

            # Memory usage
            mem_query = '100 * (1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes))'
            mem_response = await client.get(
                f"{PROMETHEUS_URL}/api/v1/query",
                params={"query": mem_query}
            )
            if mem_response.status_code == 200:
                mem_data = mem_response.json()
                if mem_data.get("status") == "success" and mem_data.get("data", {}).get("result"):
                    mem_value = float(mem_data["data"]["result"][0]["value"][1])
                    metrics["memory_usage_pct"] = round(mem_value, 2)

            # Database connections
            db_query = 'pg_stat_database_numbackends{datname="trading_chitti"}'
            db_response = await client.get(
                f"{PROMETHEUS_URL}/api/v1/query",
                params={"query": db_query}
            )
            if db_response.status_code == 200:
                db_data = db_response.json()
                if db_data.get("status") == "success" and db_data.get("data", {}).get("result"):
                    db_value = float(db_data["data"]["result"][0]["value"][1])
                    metrics["db_connections"] = int(db_value)

            return {
                "resources": metrics,
                "timestamp": datetime.utcnow().isoformat()
            }
    except Exception as e:
        return {
            "error": str(e),
            "resources": {},
            "timestamp": datetime.utcnow().isoformat()
        }

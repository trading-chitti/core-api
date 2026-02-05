"""
Prometheus metrics for core-api service.
"""

from prometheus_client import Counter, Histogram, Gauge, Info, generate_latest, CONTENT_TYPE_LATEST
from fastapi import Response
import time

# Service info
service_info = Info('core_api_service', 'Core API service information')
service_info.info({
    'version': '1.0.0',
    'service': 'core-api'
})

# HTTP request metrics
http_requests_total = Counter(
    'http_requests_total',
    'Total HTTP requests',
    ['method', 'endpoint', 'status']
)

http_request_duration_seconds = Histogram(
    'http_request_duration_seconds',
    'HTTP request duration in seconds',
    ['method', 'endpoint'],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
)

http_requests_in_progress = Gauge(
    'http_requests_in_progress',
    'HTTP requests currently in progress',
    ['method', 'endpoint']
)

# Database metrics
db_query_duration_seconds = Histogram(
    'db_query_duration_seconds',
    'Database query duration in seconds',
    ['query_type'],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)
)

db_connections_active = Gauge(
    'db_connections_active',
    'Number of active database connections'
)

db_queries_total = Counter(
    'db_queries_total',
    'Total database queries',
    ['query_type', 'status']
)

# Cache metrics
cache_hits_total = Counter(
    'cache_hits_total',
    'Total cache hits',
    ['cache_type']
)

cache_misses_total = Counter(
    'cache_misses_total',
    'Total cache misses',
    ['cache_type']
)

# Service dependency metrics
service_requests_total = Counter(
    'service_requests_total',
    'Total requests to downstream services',
    ['service', 'status']
)

service_request_duration_seconds = Histogram(
    'service_request_duration_seconds',
    'Downstream service request duration',
    ['service'],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)
)

# Business metrics
predictions_served_total = Counter(
    'predictions_served_total',
    'Total predictions served',
    ['category']  # gainer or loser
)

news_articles_served_total = Counter(
    'news_articles_served_total',
    'Total news articles served'
)

# Error metrics
errors_total = Counter(
    'errors_total',
    'Total errors',
    ['error_type', 'endpoint']
)


def get_metrics() -> Response:
    """
    Get Prometheus metrics in exposition format.

    Returns:
        Response with metrics in Prometheus format
    """
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )


class MetricsMiddleware:
    """Middleware to track HTTP request metrics."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method = scope["method"]
        path = scope["path"]

        # Skip metrics endpoint itself
        if path == "/metrics":
            await self.app(scope, receive, send)
            return

        # Track request
        http_requests_in_progress.labels(method=method, endpoint=path).inc()
        start_time = time.time()

        try:
            await self.app(scope, receive, send)
        finally:
            duration = time.time() - start_time
            http_requests_in_progress.labels(method=method, endpoint=path).dec()
            http_request_duration_seconds.labels(method=method, endpoint=path).observe(duration)


def record_request(method: str, endpoint: str, status: int):
    """Record HTTP request metrics."""
    http_requests_total.labels(method=method, endpoint=endpoint, status=status).inc()


def record_db_query(query_type: str, duration: float, success: bool = True):
    """Record database query metrics."""
    db_query_duration_seconds.labels(query_type=query_type).observe(duration)
    status = 'success' if success else 'error'
    db_queries_total.labels(query_type=query_type, status=status).inc()


def record_cache_access(cache_type: str, hit: bool):
    """Record cache access metrics."""
    if hit:
        cache_hits_total.labels(cache_type=cache_type).inc()
    else:
        cache_misses_total.labels(cache_type=cache_type).inc()


def record_service_request(service: str, duration: float, success: bool = True):
    """Record downstream service request metrics."""
    service_request_duration_seconds.labels(service=service).observe(duration)
    status = 'success' if success else 'error'
    service_requests_total.labels(service=service, status=status).inc()


def record_error(error_type: str, endpoint: str):
    """Record error metrics."""
    errors_total.labels(error_type=error_type, endpoint=endpoint).inc()

"""Core API Gateway - Routes requests to Mojo services.

This is a thin FastAPI layer that handles:
- HTTP/REST endpoints
- SSE (Server-Sent Events) streaming
- WebSocket connections (future)
- Authentication & authorization
- Request validation (Pydantic)
- Response serialization

It does NOT contain business logic - just routes to Mojo services via Unix sockets.
"""

from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncGenerator

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, make_asgi_app

from .config import settings

# Prometheus metrics
requests_total = Counter(
    "core_api_requests_total",
    "Total API requests",
    ["method", "endpoint", "status"],
)
request_duration = Histogram(
    "core_api_request_duration_seconds",
    "Request duration in seconds",
    ["method", "endpoint"],
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Startup and shutdown events."""
    print("🚀 Starting core-api initialization...")

    # Core-API now operates as a standalone REST API with direct database access
    # No need for service clients - monitoring endpoint handles service health checks
    print("✅ Core-API ready to accept requests")

    yield

    # Shutdown
    print("🛑 Shutting down core-api...")
    print("✅ Shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Trading-Chitti Core API",
    description="API Gateway routing to Mojo services for maximum performance",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS middleware (allow dashboard to connect)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add Prometheus metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


def now_iso() -> str:
    """Return current timestamp in ISO format."""
    return datetime.utcnow().isoformat() + "Z"


@app.get("/health")
async def health_check():
    """Check health of core-api service."""
    # Core-API is standalone - just check if we're running
    # For detailed service health, use /api/monitoring/services/health
    return JSONResponse(
        status_code=200,
        content={
            "status": "healthy",
            "service": "core-api",
            "version": "0.1.0",
            "timestamp": now_iso(),
        },
    )


@app.get("/")
async def root():
    """Root endpoint with API info."""
    return {
        "name": "Trading-Chitti Core API",
        "version": "0.1.0",
        "description": "API Gateway to Mojo services",
        "docs": "/docs",
        "health": "/health",
    }


# Import and include routers (defined in separate files)
from .routes import (  # noqa: E402
    compute,
    signals,
    news,
    ws,
    backtest,
    analytics,
    market,
    config,
    zerodha_auth,
    indmoney_auth,
    stocks,
    stock_config,
    portfolio,
    ml,
    watchlist,
    sentiment,
    predictions,
    monitoring,
)

app.include_router(compute.router, prefix="/api/compute", tags=["Compute"])
app.include_router(signals.router, prefix="/api/signals", tags=["Signals"])
app.include_router(news.router, prefix="/api/news", tags=["News"])
app.include_router(ws.router, tags=["WebSocket"])
app.include_router(backtest.router, tags=["Backtesting"])
app.include_router(analytics.router, prefix="/api/analytics", tags=["Analytics"])
app.include_router(market.router, prefix="/api/market", tags=["Market"])
app.include_router(config.router, prefix="/api/config", tags=["Configuration"])
app.include_router(stocks.router, prefix="/api/stocks", tags=["Stocks"])
app.include_router(stock_config.router, prefix="/api/stock-config", tags=["Stock Configuration"])
app.include_router(portfolio.router, prefix="/api/portfolio", tags=["Portfolio"])
app.include_router(ml.router, prefix="/api/ml", tags=["ML Analysis"])
app.include_router(watchlist.router, prefix="/api/watchlist", tags=["Watchlist"])
app.include_router(sentiment.router, prefix="/api", tags=["Sentiment"])
app.include_router(predictions.router, prefix="/api/predictions", tags=["Predictions"])
app.include_router(monitoring.router, prefix="/api", tags=["Monitoring"])

app.include_router(zerodha_auth.router, prefix="/auth/zerodha")
app.include_router(indmoney_auth.router, prefix="/auth/indmoney")

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler for unhandled errors."""
    requests_total.labels(
        method=request.method,
        endpoint=request.url.path,
        status="error",
    ).inc()

    return JSONResponse(
        status_code=500,
        content={
            "error": "internal_server_error",
            "message": str(exc),
            "timestamp": now_iso(),
        },
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=6001)

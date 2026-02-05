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

from .clients.mojo_compute_client import MojoComputeClient
from .clients.signal_service_client import SignalServiceClient
from .clients.signal_service_http_client import SignalServiceHTTPClient
from .clients.news_nlp_client import NewsNLPClient
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

    try:
        # Startup: Initialize clients with TCP (no more Unix sockets!)
        app.state.mojo_compute = MojoComputeClient(
            host=settings.MOJO_COMPUTE_HOST,
            port=settings.MOJO_COMPUTE_PORT,
            use_unix=False
        )
        app.state.signal_service = SignalServiceHTTPClient(
            f"http://{settings.SIGNAL_SERVICE_HOST}:{settings.SIGNAL_SERVICE_PORT}"
        )
        app.state.news_nlp = NewsNLPClient(
            host=settings.NEWS_NLP_HOST,
            port=settings.NEWS_NLP_PORT,
            use_unix=False
        )

        print("✅ Clients initialized (mojo-compute:6101, signal-service:6002, news-nlp:6102)")

        # Test connections (with timeout) - Python 3.9 compatible
        try:
            import asyncio

            # Use asyncio.wait_for for Python 3.9 compatibility (asyncio.timeout added in 3.11)
            await asyncio.wait_for(app.state.mojo_compute.ping(), timeout=5.0)
            print("✅ Mojo compute connected")

            await asyncio.wait_for(app.state.signal_service.ping(), timeout=5.0)
            print("✅ Signal service connected")

            await asyncio.wait_for(app.state.news_nlp.ping(), timeout=5.0)
            print("✅ News NLP connected")

        except asyncio.TimeoutError:
            print("⚠️  Warning: Timeout connecting to services (>5s)")
            print("   Continuing anyway...")
        except Exception as e:
            print(f"⚠️  Warning: Could not connect to all services: {e}")
            print("   Continuing anyway...")

        print("✅ Core-API ready to accept requests")
    except Exception as e:
        print(f"❌ Error during startup: {e}")
        print("   Continuing anyway...")

    yield

    # Shutdown: Close socket connections
    print("🛑 Shutting down core-api...")
    try:
        await app.state.mojo_compute.close()
        await app.state.signal_service.close()
        await app.state.news_nlp.close()
        print("✅ Connections closed")
    except Exception as e:
        print(f"⚠️  Error during shutdown: {e}")


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
    """Check health of API gateway and all Mojo services."""
    services_status = {}

    # Check mojo-compute
    try:
        await app.state.mojo_compute.ping()
        services_status["mojo-compute"] = "healthy"
    except Exception as e:
        services_status["mojo-compute"] = f"unhealthy: {str(e)}"

    # Check signal-service
    try:
        await app.state.signal_service.ping()
        services_status["signal-service"] = "healthy"
    except Exception as e:
        services_status["signal-service"] = f"unhealthy: {str(e)}"

    # Check news-nlp
    try:
        await app.state.news_nlp.ping()
        services_status["news-nlp"] = "healthy"
    except Exception as e:
        services_status["news-nlp"] = f"unhealthy: {str(e)}"

    all_healthy = all(status == "healthy" for status in services_status.values())

    return JSONResponse(
        status_code=200 if all_healthy else 503,
        content={
            "status": "healthy" if all_healthy else "degraded",
            "services": services_status,
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

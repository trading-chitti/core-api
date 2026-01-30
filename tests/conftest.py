"""Shared pytest fixtures for core-api tests."""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, MagicMock
from typing import List, Dict, Any

from core_api.app import app
from core_api.clients.mojo_compute_client import MojoComputeClient
from core_api.clients.signal_service_client import SignalServiceClient
from core_api.clients.news_nlp_client import NewsNLPClient


@pytest.fixture
def sample_prices() -> List[float]:
    """Sample price data for testing technical indicators."""
    return [
        100.0, 102.0, 101.5, 103.0, 104.5,
        103.0, 105.0, 106.5, 105.0, 107.0,
        108.5, 107.0, 109.0, 110.5, 109.0,
        111.0, 112.5, 111.0, 113.0, 114.5,
    ]


@pytest.fixture
def sample_sma_response() -> Dict[str, Any]:
    """Sample SMA response from mojo-compute."""
    return {
        "status": "success",
        "symbol": "AAPL",
        "indicator": "sma",
        "period": 5,
        "values": [101.8, 102.8, 103.4, 104.4, 105.2],
        "timestamp": "2024-01-15T10:30:00Z",
    }


@pytest.fixture
def sample_rsi_response() -> Dict[str, Any]:
    """Sample RSI response from mojo-compute."""
    return {
        "status": "success",
        "symbol": "AAPL",
        "indicator": "rsi",
        "period": 14,
        "values": [55.2, 58.3, 62.1, 59.8, 61.5],
        "timestamp": "2024-01-15T10:30:00Z",
    }


@pytest.fixture
def sample_ema_response() -> Dict[str, Any]:
    """Sample EMA response from mojo-compute."""
    return {
        "status": "success",
        "symbol": "AAPL",
        "indicator": "ema",
        "period": 12,
        "values": [102.1, 103.5, 104.8, 106.2, 107.5],
        "timestamp": "2024-01-15T10:30:00Z",
    }


@pytest.fixture
def mock_mojo_compute_client(sample_sma_response, sample_rsi_response, sample_ema_response):
    """Mock MojoComputeClient for testing."""
    client = AsyncMock(spec=MojoComputeClient)
    client.ping = AsyncMock(return_value=True)
    client.compute_sma = AsyncMock(return_value=sample_sma_response)
    client.compute_rsi = AsyncMock(return_value=sample_rsi_response)
    client.compute_ema = AsyncMock(return_value=sample_ema_response)
    client.compute_macd = AsyncMock(return_value={
        "status": "success",
        "symbol": "AAPL",
        "indicator": "macd",
        "macd_line": [1.2, 1.5, 1.8],
        "signal_line": [1.0, 1.3, 1.6],
        "histogram": [0.2, 0.2, 0.2],
    })
    client.compute_bollinger = AsyncMock(return_value={
        "status": "success",
        "symbol": "AAPL",
        "indicator": "bollinger",
        "upper_band": [115.0, 116.0, 117.0],
        "middle_band": [110.0, 111.0, 112.0],
        "lower_band": [105.0, 106.0, 107.0],
    })
    client.close = AsyncMock()
    return client


@pytest.fixture
def mock_signal_service_client():
    """Mock SignalServiceClient for testing."""
    client = AsyncMock(spec=SignalServiceClient)
    client.ping = AsyncMock(return_value=True)
    client.get_alerts = AsyncMock(return_value={
        "status": "success",
        "alerts": [
            {
                "id": "alert_1",
                "symbol": "AAPL",
                "type": "buy",
                "confidence": 0.85,
                "message": "Strong buy signal detected",
                "timestamp": "2024-01-15T10:30:00Z",
            }
        ],
        "total": 1,
    })
    client.generate_signals = AsyncMock(return_value={
        "status": "success",
        "symbol": "AAPL",
        "signals": [
            {
                "type": "buy",
                "strength": 0.75,
                "reason": "RSI oversold + positive news sentiment",
            }
        ],
    })
    client.get_patterns = AsyncMock(return_value={
        "status": "success",
        "symbol": "AAPL",
        "patterns": [
            {
                "type": "head_and_shoulders",
                "confidence": 0.82,
                "direction": "bearish",
            }
        ],
    })
    client.close = AsyncMock()
    return client


@pytest.fixture
def mock_news_nlp_client():
    """Mock NewsNLPClient for testing."""
    client = AsyncMock(spec=NewsNLPClient)
    client.ping = AsyncMock(return_value=True)
    client.get_sentiment = AsyncMock(return_value={
        "status": "success",
        "symbol": "AAPL",
        "sentiment": {
            "score": 0.75,
            "label": "positive",
            "confidence": 0.88,
        },
        "articles_analyzed": 15,
    })
    client.close = AsyncMock()
    return client


@pytest.fixture
def test_client(mock_mojo_compute_client, mock_signal_service_client, mock_news_nlp_client):
    """TestClient with mocked service clients."""
    # Directly inject mocked clients into app.state before creating TestClient
    app.state.mojo_compute = mock_mojo_compute_client
    app.state.signal_service = mock_signal_service_client
    app.state.news_nlp = mock_news_nlp_client

    # Use raise_server_exceptions=False to not raise on lifespan errors
    with TestClient(app, raise_server_exceptions=False) as client:
        # Ensure the state is set even after TestClient initialization
        client.app.state.mojo_compute = mock_mojo_compute_client
        client.app.state.signal_service = mock_signal_service_client
        client.app.state.news_nlp = mock_news_nlp_client
        yield client


@pytest.fixture
def pg_dsn() -> str:
    """PostgreSQL DSN for integration tests."""
    return "postgresql://postgres:postgres@localhost:5432/trading_chitti_test"


@pytest.fixture
def redis_url() -> str:
    """Redis URL for integration tests."""
    return "redis://localhost:6379/1"

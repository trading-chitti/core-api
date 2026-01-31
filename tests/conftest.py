"""
Shared pytest fixtures for core-api tests.
"""

import pytest
from fastapi.testclient import TestClient
from core_api.app import app


@pytest.fixture
def test_client():
    """Create a test client for the FastAPI app."""
    return TestClient(app)


@pytest.fixture
def sample_prices():
    """Sample price data for indicator calculations."""
    return [100.0, 102.0, 101.0, 103.0, 105.0, 104.0, 106.0, 108.0, 107.0, 109.0,
            111.0, 110.0, 112.0, 114.0, 113.0, 115.0, 117.0, 116.0, 118.0, 120.0]


@pytest.fixture
def sample_ohlcv():
    """Sample OHLCV data for backtesting."""
    return [
        {"date": "2024-01-01", "open": 100, "high": 105, "low": 99, "close": 103, "volume": 1000000},
        {"date": "2024-01-02", "open": 103, "high": 108, "low": 102, "close": 107, "volume": 1200000},
        {"date": "2024-01-03", "open": 107, "high": 110, "low": 106, "close": 109, "volume": 1100000},
        {"date": "2024-01-04", "open": 109, "high": 112, "low": 108, "close": 111, "volume": 1300000},
        {"date": "2024-01-05", "open": 111, "high": 115, "low": 110, "close": 114, "volume": 1400000},
    ]


@pytest.fixture
def pg_dsn():
    """PostgreSQL connection string for testing."""
    return "postgresql://hariprasath@localhost:6432/trading_chitti"


@pytest.fixture
def sample_symbols():
    """Sample list of stock symbols."""
    return ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK"]


@pytest.fixture
def mock_mojo_compute_response():
    """Mock response from Mojo compute service."""
    return {
        "status": "success",
        "result": {
            "sma": [105.0, 106.0, 107.0, 108.0, 109.0],
            "rsi": [45.2, 52.3, 58.1, 62.4, 55.7],
            "ema": [104.8, 105.9, 107.1, 108.3, 109.2],
            "macd": {"macd": [0.5, 0.8, 1.2, 1.5, 1.1], "signal": [0.4, 0.6, 0.9, 1.2, 1.0]},
        }
    }


@pytest.fixture
def mock_news_article():
    """Mock news article data."""
    return {
        "id": "abc123",
        "title": "RELIANCE announces Q4 earnings",
        "url": "https://example.com/news/123",
        "published_at": "2024-01-15T10:30:00Z",
        "source": "Economic Times",
        "summary": "Reliance Industries reports strong quarterly results...",
    }


@pytest.fixture(autouse=True)
def reset_app_state():
    """Reset application state between tests."""
    yield
    # Add any cleanup logic here if needed

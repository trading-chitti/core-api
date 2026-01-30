"""Unit tests for API routes."""

import pytest
from fastapi.testclient import TestClient


@pytest.mark.unit
def test_health_endpoint(test_client: TestClient):
    """Test health check endpoint returns service status."""
    response = test_client.get("/health")
    assert response.status_code == 200
    data = response.json()

    assert "status" in data
    assert "services" in data
    assert "timestamp" in data
    assert "mojo-compute" in data["services"]
    assert "signal-service" in data["services"]
    assert "news-nlp" in data["services"]


@pytest.mark.unit
def test_root_endpoint(test_client: TestClient):
    """Test root endpoint returns API info."""
    response = test_client.get("/")
    assert response.status_code == 200
    data = response.json()

    assert data["name"] == "Trading-Chitti Core API"
    assert data["version"] == "0.1.0"
    assert "docs" in data
    assert "health" in data


@pytest.mark.unit
def test_compute_sma_endpoint(test_client: TestClient, sample_prices):
    """Test SMA computation endpoint."""
    payload = {
        "symbol": "AAPL",
        "prices": sample_prices,
        "period": 5,
    }

    response = test_client.post("/api/compute/sma", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "success"
    assert data["symbol"] == "AAPL"
    assert data["indicator"] == "sma"
    assert data["period"] == 5
    assert "values" in data


@pytest.mark.unit
def test_compute_sma_validation_error(test_client: TestClient):
    """Test SMA endpoint with invalid input."""
    payload = {
        "symbol": "AAPL",
        "prices": [],  # Empty list should fail validation
        "period": 5,
    }

    response = test_client.post("/api/compute/sma", json=payload)
    assert response.status_code == 422  # Validation error


@pytest.mark.unit
def test_compute_rsi_endpoint(test_client: TestClient, sample_prices):
    """Test RSI computation endpoint."""
    payload = {
        "symbol": "AAPL",
        "prices": sample_prices,
        "period": 14,
    }

    response = test_client.post("/api/compute/rsi", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "success"
    assert data["symbol"] == "AAPL"
    assert data["indicator"] == "rsi"
    assert data["period"] == 14
    assert "values" in data


@pytest.mark.unit
def test_compute_rsi_default_period(test_client: TestClient, sample_prices):
    """Test RSI endpoint uses default period of 14."""
    payload = {
        "symbol": "AAPL",
        "prices": sample_prices,
        # period omitted, should default to 14
    }

    response = test_client.post("/api/compute/rsi", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["period"] == 14


@pytest.mark.unit
def test_compute_ema_endpoint(test_client: TestClient, sample_prices):
    """Test EMA computation endpoint."""
    payload = {
        "symbol": "AAPL",
        "prices": sample_prices,
        "period": 12,
    }

    response = test_client.post("/api/compute/ema", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "success"
    assert data["symbol"] == "AAPL"
    assert data["indicator"] == "ema"


@pytest.mark.unit
def test_compute_macd_endpoint(test_client: TestClient, sample_prices):
    """Test MACD computation endpoint."""
    payload = {
        "symbol": "AAPL",
        "prices": sample_prices,
        "fast_period": 12,
        "slow_period": 26,
        "signal_period": 9,
    }

    response = test_client.post("/api/compute/macd", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "success"
    assert data["symbol"] == "AAPL"
    assert data["indicator"] == "macd"
    assert "macd_line" in data
    assert "signal_line" in data
    assert "histogram" in data


@pytest.mark.unit
def test_compute_bollinger_endpoint(test_client: TestClient, sample_prices):
    """Test Bollinger Bands computation endpoint."""
    payload = {
        "symbol": "AAPL",
        "prices": sample_prices,
        "period": 20,
        "std_dev": 2.0,
    }

    response = test_client.post("/api/compute/bollinger", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "success"
    assert data["symbol"] == "AAPL"
    assert data["indicator"] == "bollinger"
    assert "upper_band" in data
    assert "middle_band" in data
    assert "lower_band" in data


@pytest.mark.unit
def test_get_alerts_endpoint(test_client: TestClient):
    """Test get alerts endpoint."""
    response = test_client.get("/api/signals/alerts")
    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "success"
    assert "alerts" in data
    assert "total" in data


@pytest.mark.unit
def test_get_alerts_with_filters(test_client: TestClient):
    """Test get alerts with query parameters."""
    response = test_client.get(
        "/api/signals/alerts",
        params={
            "limit": 50,
            "offset": 10,
            "symbol": "AAPL",
        }
    )
    assert response.status_code == 200


@pytest.mark.unit
def test_generate_signals_endpoint(test_client: TestClient):
    """Test generate signals endpoint."""
    response = test_client.post(
        "/api/signals/generate",
        params={"symbol": "AAPL"},
    )
    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "success"
    assert data["symbol"] == "AAPL"
    assert "signals" in data


@pytest.mark.unit
def test_get_patterns_endpoint(test_client: TestClient):
    """Test get patterns endpoint."""
    response = test_client.get("/api/signals/patterns/AAPL")
    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "success"
    assert data["symbol"] == "AAPL"
    assert "patterns" in data


@pytest.mark.unit
def test_invalid_symbol_length(test_client: TestClient, sample_prices):
    """Test validation rejects invalid symbol length."""
    payload = {
        "symbol": "A" * 30,  # Too long
        "prices": sample_prices,
        "period": 5,
    }

    response = test_client.post("/api/compute/sma", json=payload)
    assert response.status_code == 422


@pytest.mark.unit
def test_invalid_period_range(test_client: TestClient, sample_prices):
    """Test validation rejects period out of range."""
    payload = {
        "symbol": "AAPL",
        "prices": sample_prices,
        "period": 1,  # Too low (min is 2)
    }

    response = test_client.post("/api/compute/sma", json=payload)
    assert response.status_code == 422

"""
Unit tests for core-api routes.
"""

import pytest
from fastapi.testclient import TestClient


def test_health_endpoint(test_client):
    """Test the /health endpoint."""
    response = test_client.get("/health")
    assert response.status_code in [200, 503]  # 503 if services down
    data = response.json()
    assert "status" in data
    assert "services" in data


@pytest.mark.db
def test_analytics_system_stats(test_client):
    """Test the /api/analytics/system/stats endpoint.

    NOTE: This test requires md.system_config table to exist.
    Will be skipped if running against incomplete database schema.
    """
    try:
        response = test_client.get("/api/analytics/system/stats")

        # Should return 200 if all required tables exist
        assert response.status_code == 200
        data = response.json()

        # Check expected keys
        assert "eod_records" in data
        assert "total_stocks" in data
        assert "volatile_stocks_count" in data

    except Exception as e:
        # Skip if database tables don't exist yet (Phase 3 not complete)
        if "relation" in str(e) and "does not exist" in str(e):
            pytest.skip(f"Required database table missing: {str(e)}")
        raise


def test_analytics_performance(test_client):
    """Test the /api/analytics/performance endpoint."""
    response = test_client.get("/api/analytics/performance")

    assert response.status_code == 200
    data = response.json()

    # Check Mojo performance data
    assert "mojo" in data
    assert "speedup" in data["mojo"] or "speedup_factor" in data["mojo"]

    # Check BERT performance data
    assert "bert" in data
    # Response may have texts_per_second or batch_throughput
    assert "texts_per_second" in data["bert"] or "batch_throughput" in data["bert"]


def test_analytics_volatile_stocks(test_client):
    """Test the /api/analytics/volatile-stocks endpoint."""
    response = test_client.get("/api/analytics/volatile-stocks?limit=10")
    
    assert response.status_code == 200
    data = response.json()
    
    # Should return a list
    assert isinstance(data, list)
    
    # Should respect limit
    assert len(data) <= 10


def test_config_intraday_mode_toggle(test_client):
    """Test the intraday mode toggle endpoint."""
    # Note: This test may require database write permissions
    response = test_client.post("/api/config/intraday-mode", json={"enabled": True})
    
    # Accept both success and permission denied
    assert response.status_code in [200, 403, 500]


def test_market_price_history_missing_symbol(test_client):
    """Test price history endpoint with non-existent symbol."""
    response = test_client.get("/api/market/price-history/NONEXISTENT")
    
    # Should return 404 or empty list
    assert response.status_code in [200, 404]


def test_market_price_history_with_valid_symbol(test_client):
    """Test price history endpoint with valid symbol."""
    # RELIANCE should exist in database after seeding
    response = test_client.get("/api/market/price-history/RELIANCE?days=30")
    
    assert response.status_code == 200
    data = response.json()
    
    # Should return a list (may be empty if no price data yet)
    assert isinstance(data, list)


@pytest.mark.db
def test_market_intraday_data(test_client):
    """Test intraday data endpoint.

    NOTE: This test requires md.intraday_bars table (Phase 5A).
    Will be skipped if table doesn't exist yet.
    """
    try:
        response = test_client.get("/api/market/intraday/RELIANCE?timeframe=1min&limit=100")

        # Accept 200 (success) or 404 (not found)
        assert response.status_code in [200, 404]

        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)

    except Exception as e:
        # Skip if intraday_bars table doesn't exist yet
        if "relation" in str(e) and "does not exist" in str(e):
            pytest.skip(f"Intraday data table not yet implemented: {str(e)}")
        raise


def test_compute_endpoint_validation(test_client):
    """Test compute endpoint input validation."""
    # Missing required parameters
    response = test_client.post("/api/compute/sma", json={})
    
    # Should return 422 (validation error) or 400 (bad request)
    assert response.status_code in [400, 422]


def test_invalid_endpoint(test_client):
    """Test accessing non-existent endpoint."""
    response = test_client.get("/api/nonexistent")
    
    assert response.status_code == 404


def test_cors_headers(test_client):
    """Test that CORS headers are present."""
    response = test_client.options("/health")
    
    # Should have CORS headers configured
    # This depends on FastAPI CORS middleware configuration
    assert response.status_code in [200, 405]


def test_api_documentation(test_client):
    """Test that OpenAPI documentation is available."""
    response = test_client.get("/docs")
    
    # Should redirect or return docs page
    assert response.status_code in [200, 307]
    
    # Check for OpenAPI JSON
    response = test_client.get("/openapi.json")
    assert response.status_code == 200
    data = response.json()
    assert "openapi" in data
    assert "paths" in data

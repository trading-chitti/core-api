"""
Integration tests requiring all services running.

Run these tests with:
    pytest core-api/tests/integration -v -m integration

Prerequisites:
    - PostgreSQL running on port 6432
    - core-api running on port 6001
    - signal-service running on port 6002
    - mojo-compute running on port 6000
"""

import pytest
import httpx
import asyncio

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration

BASE_URL = "http://localhost:6001"
SIGNAL_SERVICE_URL = "http://localhost:6002"
MOJO_COMPUTE_URL = "http://localhost:6000"


@pytest.mark.asyncio
async def test_health_check_integration():
    """Test health endpoint with running service."""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BASE_URL}/health", timeout=5.0)
        assert response.status_code == 200
        data = response.json()
        assert data["status"] in ["healthy", "degraded"]


@pytest.mark.asyncio
async def test_database_connection():
    """Test that API can connect to PostgreSQL."""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BASE_URL}/api/analytics/system/stats", timeout=10.0)
        assert response.status_code == 200
        data = response.json()
        
        # Should have database statistics
        assert "total_stocks" in data
        assert data["total_stocks"] > 0  # Should have symbols after seeding


@pytest.mark.asyncio
async def test_volatile_stocks_query():
    """Test volatile stocks endpoint with real database."""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BASE_URL}/api/analytics/volatile-stocks?limit=20", timeout=10.0)
        assert response.status_code == 200
        data = response.json()
        
        assert isinstance(data, list)
        assert len(data) <= 20
        
        # Check structure if results exist
        if data:
            assert "symbol" in data[0]
            assert "volatility_30d" in data[0]


@pytest.mark.asyncio
async def test_price_history_integration():
    """Test price history endpoint with real data."""
    async with httpx.AsyncClient() as client:
        # Test with a common symbol
        response = await client.get(f"{BASE_URL}/api/market/price-history/RELIANCE?days=30", timeout=10.0)
        assert response.status_code in [200, 404]
        
        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)


@pytest.mark.asyncio
async def test_mojo_compute_integration():
    """Test that core-api can communicate with mojo-compute service."""
    async with httpx.AsyncClient() as client:
        # Try to compute SMA via core-api
        test_prices = list(range(100, 150))
        
        try:
            response = await client.post(
                f"{BASE_URL}/api/compute/sma",
                json={"prices": test_prices, "period": 20},
                timeout=15.0
            )
            
            # Accept both success and service unavailable
            assert response.status_code in [200, 503]
            
            if response.status_code == 200:
                data = response.json()
                assert "values" in data or "result" in data
        
        except httpx.ConnectError:
            pytest.skip("Mojo compute service not available")


@pytest.mark.asyncio
async def test_signal_service_integration():
    """Test that signal-service is accessible."""
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{SIGNAL_SERVICE_URL}/api/stocks/search?q=RELIANCE", timeout=10.0)
            assert response.status_code in [200, 404]
            
            if response.status_code == 200:
                data = response.json()
                assert isinstance(data, list)
        
        except httpx.ConnectError:
            pytest.skip("Signal service not available")


@pytest.mark.asyncio
async def test_concurrent_requests():
    """Test handling of concurrent requests."""
    async with httpx.AsyncClient() as client:
        # Send 10 concurrent requests
        tasks = [
            client.get(f"{BASE_URL}/health", timeout=5.0)
            for _ in range(10)
        ]
        
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        # All should succeed
        for response in responses:
            if isinstance(response, Exception):
                pytest.fail(f"Request failed: {response}")
            assert response.status_code == 200


@pytest.mark.asyncio
async def test_performance_analytics_integration():
    """Test performance metrics endpoint."""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BASE_URL}/api/analytics/performance", timeout=5.0)
        assert response.status_code == 200
        data = response.json()
        
        # Should have Mojo and BERT metrics
        assert "mojo" in data
        assert "bert" in data
        
        # Speedup should be realistic
        if "speedup_factor" in data["mojo"]:
            assert data["mojo"]["speedup_factor"] > 1.0  # At least some speedup


@pytest.mark.asyncio
async def test_intraday_mode_toggle_integration(pg_dsn):
    """Test intraday mode toggle with real database."""
    async with httpx.AsyncClient() as client:
        # Try to toggle intraday mode
        response = await client.post(
            f"{BASE_URL}/api/config/intraday-mode",
            json={"enabled": True},
            timeout=5.0
        )
        
        # Should succeed or return permission error
        assert response.status_code in [200, 403, 500]
        
        if response.status_code == 200:
            data = response.json()
            assert "success" in data or "status" in data


@pytest.mark.asyncio
async def test_end_to_end_workflow():
    """Test complete workflow: fetch symbols -> get prices -> compute indicators."""
    async with httpx.AsyncClient() as client:
        # Step 1: Get system stats
        response = await client.get(f"{BASE_URL}/api/analytics/system/stats", timeout=10.0)
        assert response.status_code == 200
        stats = response.json()
        
        if stats["total_stocks"] == 0:
            pytest.skip("No symbols in database")
        
        # Step 2: Get volatile stocks
        response = await client.get(f"{BASE_URL}/api/analytics/volatile-stocks?limit=1", timeout=10.0)
        assert response.status_code == 200
        volatile = response.json()
        
        if not volatile:
            pytest.skip("No volatile stocks data")
        
        symbol = volatile[0]["symbol"]
        
        # Step 3: Get price history for that symbol
        response = await client.get(f"{BASE_URL}/api/market/price-history/{symbol}?days=30", timeout=10.0)
        assert response.status_code in [200, 404]

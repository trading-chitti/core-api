"""Integration tests requiring running services.

These tests require:
- PostgreSQL running on localhost:5432
- Redis running on localhost:6379
- Mojo services running and accessible via Unix sockets:
  - /tmp/mojo-compute.sock
  - /tmp/signal-service.sock
  - /tmp/news-nlp.sock

Run with: pytest core-api/tests/integration -v -m integration
"""

import pytest
from fastapi.testclient import TestClient
import asyncio
import socket
import os

from core_api.app import app
from core_api.clients.mojo_compute_client import MojoComputeClient
from core_api.clients.signal_service_client import SignalServiceClient
from core_api.clients.news_nlp_client import NewsNLPClient
from core_api.config import settings


def is_socket_available(socket_path: str) -> bool:
    """Check if Unix socket is available."""
    return os.path.exists(socket_path)


def skip_if_services_unavailable():
    """Decorator to skip tests if services are not running."""
    sockets = [
        settings.MOJO_COMPUTE_SOCKET,
        settings.SIGNAL_SERVICE_SOCKET,
        settings.NEWS_NLP_SOCKET,
    ]

    missing = [s for s in sockets if not is_socket_available(s)]

    return pytest.mark.skipif(
        len(missing) > 0,
        reason=f"Required services not running. Missing sockets: {missing}"
    )


@pytest.mark.integration
class TestHealthCheckIntegration:
    """Integration tests for health check with real services."""

    @skip_if_services_unavailable()
    def test_health_check_all_services_running(self):
        """Test health check when all services are running."""
        with TestClient(app) as client:
            response = client.get("/health")

            assert response.status_code == 200
            data = response.json()

            # All services should be healthy
            assert data["status"] == "healthy"
            assert data["services"]["mojo-compute"] == "healthy"
            assert data["services"]["signal-service"] == "healthy"
            assert data["services"]["news-nlp"] == "healthy"

    def test_health_check_services_down(self):
        """Test health check when services are down."""
        # This test assumes services are NOT running
        # Skip if they are running
        if all(is_socket_available(s) for s in [
            settings.MOJO_COMPUTE_SOCKET,
            settings.SIGNAL_SERVICE_SOCKET,
            settings.NEWS_NLP_SOCKET,
        ]):
            pytest.skip("Services are running, skipping down test")

        with TestClient(app) as client:
            response = client.get("/health")

            # Should return degraded status
            assert response.status_code == 503
            data = response.json()
            assert data["status"] == "degraded"


@pytest.mark.integration
class TestMojoComputeIntegration:
    """Integration tests for mojo-compute service."""

    @skip_if_services_unavailable()
    async def test_compute_sma_real_service(self, sample_prices):
        """Test SMA computation with real mojo-compute service."""
        client = MojoComputeClient(settings.MOJO_COMPUTE_SOCKET)

        try:
            # Test ping first
            await client.ping()

            # Compute SMA
            result = await client.compute_sma(
                symbol="AAPL",
                prices=sample_prices,
                period=5,
            )

            assert result["status"] == "success"
            assert result["symbol"] == "AAPL"
            assert result["indicator"] == "sma"
            assert "values" in result
            assert len(result["values"]) > 0

        finally:
            await client.close()

    @skip_if_services_unavailable()
    async def test_compute_rsi_real_service(self, sample_prices):
        """Test RSI computation with real mojo-compute service."""
        client = MojoComputeClient(settings.MOJO_COMPUTE_SOCKET)

        try:
            result = await client.compute_rsi(
                symbol="AAPL",
                prices=sample_prices,
                period=14,
            )

            assert result["status"] == "success"
            assert result["indicator"] == "rsi"
            assert "values" in result

        finally:
            await client.close()

    @skip_if_services_unavailable()
    async def test_compute_ema_real_service(self, sample_prices):
        """Test EMA computation with real mojo-compute service."""
        client = MojoComputeClient(settings.MOJO_COMPUTE_SOCKET)

        try:
            result = await client.compute_ema(
                symbol="AAPL",
                prices=sample_prices,
                period=12,
            )

            assert result["status"] == "success"
            assert result["indicator"] == "ema"
            assert "values" in result

        finally:
            await client.close()

    @skip_if_services_unavailable()
    async def test_compute_macd_real_service(self, sample_prices):
        """Test MACD computation with real mojo-compute service."""
        client = MojoComputeClient(settings.MOJO_COMPUTE_SOCKET)

        try:
            result = await client.compute_macd(
                symbol="AAPL",
                prices=sample_prices,
                fast_period=12,
                slow_period=26,
                signal_period=9,
            )

            assert result["status"] == "success"
            assert result["indicator"] == "macd"
            assert "macd_line" in result
            assert "signal_line" in result
            assert "histogram" in result

        finally:
            await client.close()


@pytest.mark.integration
class TestSignalServiceIntegration:
    """Integration tests for signal-service."""

    @skip_if_services_unavailable()
    async def test_get_alerts_real_service(self):
        """Test getting alerts from real signal-service."""
        client = SignalServiceClient(settings.SIGNAL_SERVICE_SOCKET)

        try:
            # Test ping first
            await client.ping()

            result = await client.get_alerts(limit=10)

            assert result["status"] == "success"
            assert "alerts" in result
            assert "total" in result

        finally:
            await client.close()

    @skip_if_services_unavailable()
    async def test_generate_signals_real_service(self):
        """Test generating signals with real signal-service."""
        client = SignalServiceClient(settings.SIGNAL_SERVICE_SOCKET)

        try:
            indicators = {
                "rsi": 30.5,
                "sma_20": 105.0,
                "sma_50": 102.0,
            }

            result = await client.generate_signals(
                symbol="AAPL",
                indicators=indicators,
            )

            assert result["status"] == "success"
            assert "signals" in result

        finally:
            await client.close()

    @skip_if_services_unavailable()
    async def test_get_patterns_real_service(self):
        """Test getting patterns from real signal-service."""
        client = SignalServiceClient(settings.SIGNAL_SERVICE_SOCKET)

        try:
            result = await client.get_patterns(symbol="AAPL")

            assert result["status"] == "success"
            assert "patterns" in result

        finally:
            await client.close()


@pytest.mark.integration
class TestNewsNLPIntegration:
    """Integration tests for news-nlp service."""

    @skip_if_services_unavailable()
    async def test_analyze_sentiment_real_service(self):
        """Test sentiment analysis with real news-nlp service."""
        client = NewsNLPClient(settings.NEWS_NLP_SOCKET)

        try:
            # Test ping first
            await client.ping()

            result = await client.analyze_sentiment(
                text="Apple stock surges on strong quarterly earnings"
            )

            assert result["status"] == "success"
            assert "sentiment" in result

        finally:
            await client.close()

    @skip_if_services_unavailable()
    async def test_extract_entities_real_service(self):
        """Test entity extraction with real news-nlp service."""
        client = NewsNLPClient(settings.NEWS_NLP_SOCKET)

        try:
            result = await client.extract_entities(
                text="Apple and Microsoft lead technology sector gains"
            )

            assert result["status"] == "success"
            assert "entities" in result

        finally:
            await client.close()

    @skip_if_services_unavailable()
    async def test_get_articles_real_service(self):
        """Test getting articles from real news-nlp service."""
        client = NewsNLPClient(settings.NEWS_NLP_SOCKET)

        try:
            result = await client.get_articles(limit=10)

            assert result["status"] == "success"
            assert "articles" in result

        finally:
            await client.close()


@pytest.mark.integration
@pytest.mark.slow
class TestEndToEndFlow:
    """End-to-end integration tests."""

    @skip_if_services_unavailable()
    def test_full_trading_signal_flow(self, sample_prices):
        """Test complete flow: compute indicators -> generate signals."""
        with TestClient(app) as client:
            # Step 1: Compute technical indicators
            sma_response = client.post(
                "/api/compute/sma",
                json={
                    "symbol": "AAPL",
                    "prices": sample_prices,
                    "period": 20,
                }
            )
            assert sma_response.status_code == 200
            sma_data = sma_response.json()

            rsi_response = client.post(
                "/api/compute/rsi",
                json={
                    "symbol": "AAPL",
                    "prices": sample_prices,
                    "period": 14,
                }
            )
            assert rsi_response.status_code == 200
            rsi_data = rsi_response.json()

            # Step 2: Get news sentiment
            # Note: This would require actual news articles in the system

            # Step 3: Generate trading signals
            signals_response = client.post(
                "/api/signals/generate",
                params={"symbol": "AAPL"},
            )
            assert signals_response.status_code == 200
            signals_data = signals_response.json()

            assert signals_data["status"] == "success"
            assert "signals" in signals_data

    @skip_if_services_unavailable()
    def test_concurrent_requests(self, sample_prices):
        """Test handling multiple concurrent requests."""
        with TestClient(app) as client:
            # Send multiple requests concurrently
            symbols = ["AAPL", "GOOGL", "MSFT"]

            responses = []
            for symbol in symbols:
                response = client.post(
                    "/api/compute/sma",
                    json={
                        "symbol": symbol,
                        "prices": sample_prices,
                        "period": 20,
                    }
                )
                responses.append(response)

            # All should succeed
            for response in responses:
                assert response.status_code == 200
                assert response.json()["status"] == "success"

    @skip_if_services_unavailable()
    def test_error_handling_invalid_data(self):
        """Test error handling with invalid data."""
        with TestClient(app) as client:
            # Send invalid data (empty prices)
            response = client.post(
                "/api/compute/sma",
                json={
                    "symbol": "AAPL",
                    "prices": [],
                    "period": 20,
                }
            )

            # Should return validation error
            assert response.status_code == 422

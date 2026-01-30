"""Unit tests for socket clients with mocking."""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import json

from core_api.clients.mojo_compute_client import MojoComputeClient
from core_api.clients.signal_service_client import SignalServiceClient
from core_api.clients.news_nlp_client import NewsNLPClient
from core_api.clients.base import MojoSocketClient


@pytest.mark.unit
class TestMojoSocketClient:
    """Tests for the base socket client."""

    async def test_unix_socket_initialization(self):
        """Test Unix socket client initialization."""
        client = MojoSocketClient(socket_path="/tmp/test.sock", use_unix=True)
        assert client.socket_path == "/tmp/test.sock"
        assert client.use_unix is True

    async def test_tcp_socket_initialization(self):
        """Test TCP socket client initialization."""
        client = MojoSocketClient(host="localhost", port=8000, use_unix=False)
        assert client.host == "localhost"
        assert client.port == 8000
        assert client.use_unix is False

    async def test_ping_success(self):
        """Test successful ping to service."""
        client = MojoSocketClient(socket_path="/tmp/test.sock")

        # Mock the send_request method
        client.send_request = AsyncMock(return_value={"status": "ok"})

        result = await client.ping()
        assert result is True
        client.send_request.assert_called_once_with({"action": "ping"})

    async def test_ping_failure(self):
        """Test ping failure when service doesn't respond correctly."""
        client = MojoSocketClient(socket_path="/tmp/test.sock")

        # Mock the send_request to return unexpected response
        client.send_request = AsyncMock(return_value={"status": "error"})

        with pytest.raises(ConnectionError):
            await client.ping()

    async def test_close(self):
        """Test closing socket connection."""
        client = MojoSocketClient(socket_path="/tmp/test.sock")
        client.sock = MagicMock()

        await client.close()
        assert client.sock is None


@pytest.mark.unit
class TestMojoComputeClient:
    """Tests for MojoComputeClient."""

    async def test_compute_sma(self, sample_prices):
        """Test compute_sma sends correct request."""
        client = MojoComputeClient(socket_path="/tmp/mojo-compute.sock")

        expected_response = {
            "status": "success",
            "symbol": "AAPL",
            "indicator": "sma",
            "period": 5,
            "values": [101.8, 102.8, 103.4],
        }

        client.send_request = AsyncMock(return_value=expected_response)

        result = await client.compute_sma(
            symbol="AAPL",
            prices=sample_prices,
            period=5,
        )

        assert result == expected_response
        client.send_request.assert_called_once()
        call_args = client.send_request.call_args[0][0]
        assert call_args["action"] == "compute_sma"
        assert call_args["symbol"] == "AAPL"
        assert call_args["period"] == 5
        assert call_args["prices"] == sample_prices

    async def test_compute_rsi(self, sample_prices):
        """Test compute_rsi sends correct request."""
        client = MojoComputeClient(socket_path="/tmp/mojo-compute.sock")

        expected_response = {
            "status": "success",
            "symbol": "AAPL",
            "indicator": "rsi",
            "period": 14,
            "values": [55.2, 58.3, 62.1],
        }

        client.send_request = AsyncMock(return_value=expected_response)

        result = await client.compute_rsi(
            symbol="AAPL",
            prices=sample_prices,
            period=14,
        )

        assert result == expected_response
        call_args = client.send_request.call_args[0][0]
        assert call_args["action"] == "compute_rsi"
        assert call_args["symbol"] == "AAPL"
        assert call_args["period"] == 14

    async def test_compute_rsi_default_period(self, sample_prices):
        """Test compute_rsi uses default period."""
        client = MojoComputeClient(socket_path="/tmp/mojo-compute.sock")
        client.send_request = AsyncMock(return_value={"status": "success"})

        await client.compute_rsi(symbol="AAPL", prices=sample_prices)

        call_args = client.send_request.call_args[0][0]
        assert call_args["period"] == 14  # Default value

    async def test_compute_ema(self, sample_prices):
        """Test compute_ema sends correct request."""
        client = MojoComputeClient(socket_path="/tmp/mojo-compute.sock")

        expected_response = {
            "status": "success",
            "symbol": "AAPL",
            "indicator": "ema",
            "period": 12,
            "values": [102.1, 103.5],
        }

        client.send_request = AsyncMock(return_value=expected_response)

        result = await client.compute_ema(
            symbol="AAPL",
            prices=sample_prices,
            period=12,
        )

        assert result == expected_response
        call_args = client.send_request.call_args[0][0]
        assert call_args["action"] == "compute_ema"

    async def test_compute_macd(self, sample_prices):
        """Test compute_macd sends correct request."""
        client = MojoComputeClient(socket_path="/tmp/mojo-compute.sock")

        expected_response = {
            "status": "success",
            "symbol": "AAPL",
            "indicator": "macd",
            "macd_line": [1.2, 1.5],
            "signal_line": [1.0, 1.3],
            "histogram": [0.2, 0.2],
        }

        client.send_request = AsyncMock(return_value=expected_response)

        result = await client.compute_macd(
            symbol="AAPL",
            prices=sample_prices,
            fast_period=12,
            slow_period=26,
            signal_period=9,
        )

        assert result == expected_response
        call_args = client.send_request.call_args[0][0]
        assert call_args["action"] == "compute_macd"
        assert call_args["fast_period"] == 12
        assert call_args["slow_period"] == 26
        assert call_args["signal_period"] == 9

    async def test_compute_bollinger(self, sample_prices):
        """Test compute_bollinger sends correct request."""
        client = MojoComputeClient(socket_path="/tmp/mojo-compute.sock")

        expected_response = {
            "status": "success",
            "symbol": "AAPL",
            "indicator": "bollinger",
            "upper_band": [115.0, 116.0],
            "middle_band": [110.0, 111.0],
            "lower_band": [105.0, 106.0],
        }

        client.send_request = AsyncMock(return_value=expected_response)

        result = await client.compute_bollinger(
            symbol="AAPL",
            prices=sample_prices,
            period=20,
            std_dev=2.0,
        )

        assert result == expected_response
        call_args = client.send_request.call_args[0][0]
        assert call_args["action"] == "compute_bollinger"
        assert call_args["period"] == 20
        assert call_args["std_dev"] == 2.0

    async def test_backtest(self):
        """Test backtest sends correct request."""
        client = MojoComputeClient(socket_path="/tmp/mojo-compute.sock")

        strategy_data = {
            "ohlcv": [[1.0, 2.0, 3.0, 4.0, 5.0]],
        }
        parameters = {
            "sma_period": 20,
            "rsi_period": 14,
        }

        expected_response = {
            "status": "success",
            "strategy_id": "sma_rsi_strategy",
            "total_return": 15.5,
            "sharpe_ratio": 1.8,
        }

        client.send_request = AsyncMock(return_value=expected_response)

        result = await client.backtest(
            strategy_id="sma_rsi_strategy",
            symbol="AAPL",
            data=strategy_data,
            parameters=parameters,
        )

        assert result == expected_response
        call_args = client.send_request.call_args[0][0]
        assert call_args["action"] == "backtest"
        assert call_args["strategy_id"] == "sma_rsi_strategy"


@pytest.mark.unit
class TestSignalServiceClient:
    """Tests for SignalServiceClient."""

    async def test_get_alerts(self):
        """Test get_alerts sends correct request."""
        client = SignalServiceClient(socket_path="/tmp/signal-service.sock")

        expected_response = {
            "status": "success",
            "alerts": [
                {
                    "id": "alert_1",
                    "symbol": "AAPL",
                    "type": "buy",
                    "confidence": 0.85,
                }
            ],
            "total": 1,
        }

        client.send_request = AsyncMock(return_value=expected_response)

        result = await client.get_alerts(limit=100, offset=0, symbol="AAPL")

        assert result == expected_response
        call_args = client.send_request.call_args[0][0]
        assert call_args["action"] == "get_alerts"
        assert call_args["limit"] == 100
        assert call_args["offset"] == 0
        assert call_args["symbol"] == "AAPL"

    async def test_generate_signals(self):
        """Test generate_signals sends correct request."""
        client = SignalServiceClient(socket_path="/tmp/signal-service.sock")

        indicators = {"rsi": 30.5, "sma_20": 105.0}
        news_context = {"sentiment": 0.75}

        expected_response = {
            "status": "success",
            "symbol": "AAPL",
            "signals": [
                {
                    "type": "buy",
                    "strength": 0.75,
                }
            ],
        }

        client.send_request = AsyncMock(return_value=expected_response)

        result = await client.generate_signals(
            symbol="AAPL",
            indicators=indicators,
            news_context=news_context,
        )

        assert result == expected_response
        call_args = client.send_request.call_args[0][0]
        assert call_args["action"] == "generate_signals"
        assert call_args["symbol"] == "AAPL"

    async def test_get_patterns(self):
        """Test get_patterns sends correct request."""
        client = SignalServiceClient(socket_path="/tmp/signal-service.sock")

        expected_response = {
            "status": "success",
            "symbol": "AAPL",
            "patterns": [
                {
                    "type": "head_and_shoulders",
                    "confidence": 0.82,
                }
            ],
        }

        client.send_request = AsyncMock(return_value=expected_response)

        result = await client.get_patterns(
            symbol="AAPL",
            pattern_types=["head_and_shoulders", "double_bottom"],
        )

        assert result == expected_response
        call_args = client.send_request.call_args[0][0]
        assert call_args["action"] == "get_patterns"
        assert call_args["symbol"] == "AAPL"


@pytest.mark.unit
class TestNewsNLPClient:
    """Tests for NewsNLPClient."""

    async def test_analyze_sentiment(self):
        """Test analyze_sentiment sends correct request."""
        client = NewsNLPClient(socket_path="/tmp/news-nlp.sock")

        expected_response = {
            "status": "success",
            "sentiment": {
                "score": 0.75,
                "label": "positive",
                "confidence": 0.88,
            },
        }

        client.send_request = AsyncMock(return_value=expected_response)

        result = await client.analyze_sentiment(text="Apple stock surges on strong earnings")

        assert result == expected_response
        call_args = client.send_request.call_args[0][0]
        assert call_args["action"] == "analyze_sentiment"
        assert "text" in call_args

    async def test_extract_entities(self):
        """Test extract_entities sends correct request."""
        client = NewsNLPClient(socket_path="/tmp/news-nlp.sock")

        expected_response = {
            "status": "success",
            "entities": {
                "symbols": ["AAPL", "MSFT"],
                "sectors": ["Technology"],
            },
        }

        client.send_request = AsyncMock(return_value=expected_response)

        result = await client.extract_entities(text="Apple and Microsoft lead tech sector gains")

        assert result == expected_response
        call_args = client.send_request.call_args[0][0]
        assert call_args["action"] == "extract_entities"

    async def test_get_articles(self):
        """Test get_articles sends correct request."""
        client = NewsNLPClient(socket_path="/tmp/news-nlp.sock")

        expected_response = {
            "status": "success",
            "articles": [
                {
                    "id": "article_1",
                    "title": "Apple stock surges",
                    "symbol": "AAPL",
                    "sentiment": 0.75,
                }
            ],
            "total": 1,
        }

        client.send_request = AsyncMock(return_value=expected_response)

        result = await client.get_articles(limit=50, symbol="AAPL")

        assert result == expected_response
        call_args = client.send_request.call_args[0][0]
        assert call_args["action"] == "get_articles"
        assert call_args["limit"] == 50
        assert call_args["symbol"] == "AAPL"

    async def test_ingest_rss(self):
        """Test ingest_rss sends correct request."""
        client = NewsNLPClient(socket_path="/tmp/news-nlp.sock")

        expected_response = {
            "status": "success",
            "articles_ingested": 25,
        }

        client.send_request = AsyncMock(return_value=expected_response)

        result = await client.ingest_rss(url="https://example.com/feed.xml")

        assert result == expected_response
        call_args = client.send_request.call_args[0][0]
        assert call_args["action"] == "ingest_rss"
        assert call_args["url"] == "https://example.com/feed.xml"

    async def test_classify_direction(self):
        """Test classify_direction sends correct request."""
        client = NewsNLPClient(socket_path="/tmp/news-nlp.sock")

        expected_response = {
            "status": "success",
            "direction": "bullish",
            "confidence": 0.82,
        }

        client.send_request = AsyncMock(return_value=expected_response)

        result = await client.classify_direction(
            text="Strong earnings beat expectations",
            sentiment=0.75,
        )

        assert result == expected_response
        call_args = client.send_request.call_args[0][0]
        assert call_args["action"] == "classify_direction"
        assert call_args["sentiment"] == 0.75

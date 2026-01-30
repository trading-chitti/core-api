"""Client for signal-service (Mojo)."""

from typing import List, Optional

from .base import MojoSocketClient


class SignalServiceClient(MojoSocketClient):
    """Client for communicating with signal-service (Mojo)."""

    async def get_alerts(
        self,
        limit: int = 100,
        offset: int = 0,
        symbol: Optional[str] = None,
        sector: Optional[str] = None,
    ) -> dict:
        """Get alerts from signal-service.

        Args:
            limit: Maximum number of alerts to return
            offset: Offset for pagination
            symbol: Filter by symbol (optional)
            sector: Filter by sector (optional)

        Returns:
            Response with alerts list
        """
        request = {
            "action": "get_alerts",
            "limit": limit,
            "offset": offset,
        }

        if symbol:
            request["symbol"] = symbol
        if sector:
            request["sector"] = sector

        return await self.send_request(request)

    async def generate_signals(
        self,
        symbol: str,
        indicators: Optional[dict] = None,
        news_context: Optional[dict] = None,
    ) -> dict:
        """Generate trading signals for a symbol.

        Args:
            symbol: Stock symbol
            indicators: Technical indicator values (optional)
            news_context: News/sentiment context (optional)

        Returns:
            Response with generated signals
        """
        request = {
            "action": "generate_signals",
            "symbol": symbol,
        }

        if indicators:
            request["indicators"] = indicators
        if news_context:
            request["news_context"] = news_context

        return await self.send_request(request)

    async def get_patterns(
        self,
        symbol: str,
        pattern_types: Optional[List[str]] = None,
    ) -> dict:
        """Get pattern matches for a symbol.

        Args:
            symbol: Stock symbol
            pattern_types: List of pattern types to check (optional)

        Returns:
            Response with matched patterns
        """
        request = {
            "action": "get_patterns",
            "symbol": symbol,
        }

        if pattern_types:
            request["pattern_types"] = pattern_types

        return await self.send_request(request)

    async def subscribe_stream(self) -> dict:
        """Subscribe to SSE event stream.

        Note: This is a placeholder. Actual SSE implementation
        will be in the route handler.

        Returns:
            Subscription confirmation
        """
        request = {"action": "subscribe_stream"}
        return await self.send_request(request)

"""HTTP Client for signal-service (when running as FastAPI HTTP service)."""

import httpx
from typing import Optional, List


class SignalServiceHTTPClient:
    """Client for communicating with signal-service via HTTP."""

    def __init__(self, base_url: str = "http://localhost:6002"):
        self.base_url = base_url.rstrip('/')
        self.client = httpx.AsyncClient(timeout=30.0)

    async def ping(self) -> dict:
        """Check if signal-service is alive."""
        try:
            response = await self.client.get(f"{self.base_url}/health")
            response.raise_for_status()
            return {"status": "ok"}
        except Exception as e:
            raise Exception(f"Signal service not reachable: {e}")

    async def get_alerts(
        self,
        limit: int = 100,
        offset: int = 0,
        symbol: Optional[str] = None,
        sector: Optional[str] = None,
    ) -> dict:
        """Get alerts from signal-service."""
        params = {"limit": limit, "offset": offset}
        if symbol:
            params["symbol"] = symbol
        if sector:
            params["sector"] = sector

        response = await self.client.get(f"{self.base_url}/api/alerts", params=params)
        response.raise_for_status()
        return response.json()

    async def generate_signals(
        self,
        symbol: str,
        indicators: Optional[dict] = None,
        news_context: Optional[dict] = None,
    ) -> dict:
        """Generate trading signals for a symbol."""
        payload = {"symbol": symbol}
        if indicators:
            payload["indicators"] = indicators
        if news_context:
            payload["news_context"] = news_context

        response = await self.client.post(f"{self.base_url}/api/signals/generate", json=payload)
        response.raise_for_status()
        return response.json()

    async def get_patterns(
        self,
        symbol: str,
        pattern_types: Optional[List[str]] = None,
    ) -> dict:
        """Get pattern matches for a symbol."""
        params = {"symbol": symbol}
        if pattern_types:
            params["pattern_types"] = ",".join(pattern_types)

        response = await self.client.get(f"{self.base_url}/api/patterns", params=params)
        response.raise_for_status()
        return response.json()

    async def close(self):
        """Close HTTP client."""
        await self.client.aclose()

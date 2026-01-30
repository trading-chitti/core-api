"""
SSE-to-WebSocket Bridge.

Subscribes to signal-service SSE streams and forwards messages to WebSocket clients.
This bridges the signal-service PubSub to core-api WebSocket manager.
"""

import asyncio
import json
import logging
from typing import Optional
import httpx
from .websocket import manager
from .config import settings

logger = logging.getLogger(__name__)


class SSEBridge:
    """
    Bridges signal-service SSE streams to WebSocket clients.

    Subscribes to:
    - /api/alerts/stream (alerts channel)
    - /api/activity/stream (activity channel)

    Forwards messages to WebSocket manager's broadcast_to_channel().
    """

    def __init__(self, signal_service_url: str = "http://localhost:6002"):
        self.signal_service_url = signal_service_url
        self._alerts_task: Optional[asyncio.Task] = None
        self._activity_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self):
        """Start SSE bridge tasks."""
        if self._running:
            logger.warning("SSE bridge already running")
            return

        self._running = True

        # Start both stream bridges
        self._alerts_task = asyncio.create_task(self._bridge_alerts_stream())
        self._activity_task = asyncio.create_task(self._bridge_activity_stream())

        logger.info("SSE-to-WebSocket bridge started")

    async def stop(self):
        """Stop SSE bridge tasks."""
        self._running = False

        if self._alerts_task:
            self._alerts_task.cancel()
            try:
                await self._alerts_task
            except asyncio.CancelledError:
                pass

        if self._activity_task:
            self._activity_task.cancel()
            try:
                await self._activity_task
            except asyncio.CancelledError:
                pass

        logger.info("SSE-to-WebSocket bridge stopped")

    async def _bridge_alerts_stream(self):
        """
        Subscribe to signal-service /api/alerts/stream and forward to WebSocket.
        """
        url = f"{self.signal_service_url}/api/alerts/stream"

        while self._running:
            try:
                async with httpx.AsyncClient() as client:
                    async with client.stream("GET", url, timeout=None) as response:
                        logger.info(f"Connected to alerts stream: {url}")

                        async for line in response.aiter_lines():
                            if not self._running:
                                break

                            # Parse SSE format: "data: {...}"
                            if line.startswith("data: "):
                                try:
                                    data_str = line[6:]  # Remove "data: " prefix
                                    data = json.loads(data_str)

                                    # Forward to WebSocket clients subscribed to "alerts" channel
                                    await manager.broadcast_to_channel("alerts", {
                                        "type": "alert",
                                        "data": data,
                                    })

                                    logger.debug(f"Forwarded alert to WebSocket: {data.get('id', 'unknown')}")

                                except json.JSONDecodeError as e:
                                    logger.warning(f"Failed to parse SSE data: {e}")

            except httpx.HTTPError as e:
                logger.error(f"Error connecting to alerts stream: {e}")
                if self._running:
                    # Retry after 5 seconds
                    await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"Unexpected error in alerts bridge: {e}")
                if self._running:
                    await asyncio.sleep(5)

    async def _bridge_activity_stream(self):
        """
        Subscribe to signal-service /api/activity/stream and forward to WebSocket.
        """
        url = f"{self.signal_service_url}/api/activity/stream"

        while self._running:
            try:
                async with httpx.AsyncClient() as client:
                    async with client.stream("GET", url, timeout=None) as response:
                        logger.info(f"Connected to activity stream: {url}")

                        async for line in response.aiter_lines():
                            if not self._running:
                                break

                            # Parse SSE format: "data: {...}"
                            if line.startswith("data: "):
                                try:
                                    data_str = line[6:]  # Remove "data: " prefix
                                    data = json.loads(data_str)

                                    # Forward to WebSocket clients subscribed to "activity" channel
                                    await manager.broadcast_to_channel("activity", {
                                        "type": "activity",
                                        "data": data,
                                    })

                                    logger.debug(f"Forwarded activity to WebSocket: {data.get('kind', 'unknown')}")

                                except json.JSONDecodeError as e:
                                    logger.warning(f"Failed to parse SSE data: {e}")

            except httpx.HTTPError as e:
                logger.error(f"Error connecting to activity stream: {e}")
                if self._running:
                    # Retry after 5 seconds
                    await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"Unexpected error in activity bridge: {e}")
                if self._running:
                    await asyncio.sleep(5)


# Global bridge instance
bridge = SSEBridge()

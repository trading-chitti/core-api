"""
WebSocket Connection Manager for Real-time Data Streaming.

Manages WebSocket connections, channel subscriptions, and message broadcasting
for real-time alerts, prices, indicators, and news updates.
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Set, List, Any
from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)


class ConnectionManager:
    """
    Manages WebSocket connections and message broadcasting.

    Features:
    - Multiple channel subscriptions per connection
    - Message fanout to subscribed clients
    - Heartbeat/ping-pong for connection health
    - Automatic cleanup of disconnected clients
    """

    def __init__(self):
        # Active connections: connection_id -> WebSocket
        self.active_connections: Dict[str, WebSocket] = {}

        # Channel subscriptions: channel_name -> set of connection_ids
        self.subscriptions: Dict[str, Set[str]] = {
            "alerts": set(),
            "prices": set(),
            "indicators": set(),
            "news": set(),
            "activity": set(),
        }

        # Connection metadata
        self.connection_metadata: Dict[str, Dict[str, Any]] = {}

        # Heartbeat task
        self._heartbeat_task = None

    async def connect(self, websocket: WebSocket, connection_id: str, channels: List[str] = None):
        """
        Accept WebSocket connection and subscribe to channels.

        Args:
            websocket: FastAPI WebSocket instance
            connection_id: Unique connection identifier
            channels: List of channels to subscribe to
        """
        await websocket.accept()

        self.active_connections[connection_id] = websocket
        self.connection_metadata[connection_id] = {
            "connected_at": datetime.utcnow().isoformat(),
            "channels": channels or [],
            "last_ping": datetime.utcnow().isoformat(),
        }

        # Subscribe to channels
        if channels:
            for channel in channels:
                if channel in self.subscriptions:
                    self.subscriptions[channel].add(connection_id)

        logger.info(f"WebSocket connection {connection_id} established. Channels: {channels}")

        # Send welcome message
        await self.send_personal_message(
            {
                "type": "connection",
                "status": "connected",
                "connection_id": connection_id,
                "channels": channels or [],
                "timestamp": datetime.utcnow().isoformat(),
            },
            connection_id
        )

    def disconnect(self, connection_id: str):
        """
        Remove connection and clean up subscriptions.

        Args:
            connection_id: Connection to remove
        """
        if connection_id in self.active_connections:
            del self.active_connections[connection_id]

        if connection_id in self.connection_metadata:
            del self.connection_metadata[connection_id]

        # Remove from all channel subscriptions
        for channel_subs in self.subscriptions.values():
            channel_subs.discard(connection_id)

        logger.info(f"WebSocket connection {connection_id} disconnected")

    async def send_personal_message(self, message: Dict[str, Any], connection_id: str):
        """
        Send message to specific connection.

        Args:
            message: Message payload (will be JSON-encoded)
            connection_id: Target connection
        """
        if connection_id in self.active_connections:
            try:
                websocket = self.active_connections[connection_id]
                await websocket.send_json(message)
            except Exception as e:
                logger.error(f"Failed to send message to {connection_id}: {e}")
                self.disconnect(connection_id)

    async def broadcast_to_channel(self, channel: str, message: Dict[str, Any]):
        """
        Broadcast message to all connections subscribed to channel.

        Args:
            channel: Channel name
            message: Message payload
        """
        if channel not in self.subscriptions:
            logger.warning(f"Unknown channel: {channel}")
            return

        # Add channel and timestamp to message
        message_with_metadata = {
            **message,
            "channel": channel,
            "timestamp": datetime.utcnow().isoformat(),
        }

        # Send to all subscribers
        subscribers = self.subscriptions[channel].copy()  # Copy to avoid modification during iteration

        for connection_id in subscribers:
            await self.send_personal_message(message_with_metadata, connection_id)

    async def broadcast_to_all(self, message: Dict[str, Any]):
        """
        Broadcast message to all active connections.

        Args:
            message: Message payload
        """
        message_with_timestamp = {
            **message,
            "timestamp": datetime.utcnow().isoformat(),
        }

        for connection_id in list(self.active_connections.keys()):
            await self.send_personal_message(message_with_timestamp, connection_id)

    async def subscribe(self, connection_id: str, channels: List[str]):
        """
        Subscribe connection to additional channels.

        Args:
            connection_id: Connection to subscribe
            channels: Channels to subscribe to
        """
        for channel in channels:
            if channel in self.subscriptions:
                self.subscriptions[channel].add(connection_id)

                if connection_id in self.connection_metadata:
                    self.connection_metadata[connection_id]["channels"].append(channel)

        await self.send_personal_message(
            {
                "type": "subscription",
                "action": "subscribed",
                "channels": channels,
            },
            connection_id
        )

    async def unsubscribe(self, connection_id: str, channels: List[str]):
        """
        Unsubscribe connection from channels.

        Args:
            connection_id: Connection to unsubscribe
            channels: Channels to unsubscribe from
        """
        for channel in channels:
            if channel in self.subscriptions:
                self.subscriptions[channel].discard(connection_id)

                if connection_id in self.connection_metadata:
                    metadata_channels = self.connection_metadata[connection_id]["channels"]
                    if channel in metadata_channels:
                        metadata_channels.remove(channel)

        await self.send_personal_message(
            {
                "type": "subscription",
                "action": "unsubscribed",
                "channels": channels,
            },
            connection_id
        )

    async def start_heartbeat(self, interval: int = 30):
        """
        Start heartbeat task to keep connections alive.

        Args:
            interval: Heartbeat interval in seconds
        """
        async def heartbeat_loop():
            while True:
                await asyncio.sleep(interval)
                await self.broadcast_to_all({"type": "ping"})
                logger.debug(f"Heartbeat sent to {len(self.active_connections)} connections")

        self._heartbeat_task = asyncio.create_task(heartbeat_loop())

    def get_stats(self) -> Dict[str, Any]:
        """
        Get connection statistics.

        Returns:
            Dictionary with connection stats
        """
        return {
            "total_connections": len(self.active_connections),
            "subscriptions": {
                channel: len(subs) for channel, subs in self.subscriptions.items()
            },
            "connections": [
                {
                    "connection_id": conn_id,
                    **metadata
                }
                for conn_id, metadata in self.connection_metadata.items()
            ]
        }


# Global connection manager instance
manager = ConnectionManager()

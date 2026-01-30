"""
WebSocket API Routes.

Handles WebSocket connections, subscriptions, and real-time message streaming.
"""

import uuid
import logging
from typing import List
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from core_api.websocket import manager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ws", tags=["websocket"])


@router.websocket("")
async def websocket_endpoint(
    websocket: WebSocket,
    channels: List[str] = Query(default=["alerts"]),  # Default to alerts channel
):
    """
    WebSocket endpoint for real-time data streaming.

    Query Parameters:
        channels: List of channels to subscribe to (alerts, prices, indicators, news, activity)

    Example:
        ws://localhost:6001/ws?channels=alerts&channels=prices

    Message Format:
        {
            "type": "alert" | "price" | "indicator" | "news" | "ping" | "connection",
            "channel": "alerts" | "prices" | "indicators" | "news" | "activity",
            "data": {...},
            "timestamp": "2024-01-30T12:34:56.789Z"
        }

    Client Commands:
        Subscribe:   {"action": "subscribe", "channels": ["prices", "news"]}
        Unsubscribe: {"action": "unsubscribe", "channels": ["prices"]}
        Ping:        {"action": "ping"}
    """
    connection_id = str(uuid.uuid4())

    try:
        # Connect and subscribe
        await manager.connect(websocket, connection_id, channels)

        # Message loop
        while True:
            # Receive message from client
            data = await websocket.receive_json()

            # Handle client commands
            action = data.get("action")

            if action == "subscribe":
                subscribe_channels = data.get("channels", [])
                await manager.subscribe(connection_id, subscribe_channels)
                logger.info(f"Connection {connection_id} subscribed to {subscribe_channels}")

            elif action == "unsubscribe":
                unsubscribe_channels = data.get("channels", [])
                await manager.unsubscribe(connection_id, unsubscribe_channels)
                logger.info(f"Connection {connection_id} unsubscribed from {unsubscribe_channels}")

            elif action == "ping":
                # Respond to ping
                await manager.send_personal_message(
                    {"type": "pong"}, connection_id
                )

            elif action == "get_stats":
                # Return connection statistics
                stats = manager.get_stats()
                await manager.send_personal_message(
                    {"type": "stats", "data": stats}, connection_id
                )

            else:
                # Unknown action
                await manager.send_personal_message(
                    {
                        "type": "error",
                        "message": f"Unknown action: {action}"
                    },
                    connection_id
                )

    except WebSocketDisconnect:
        manager.disconnect(connection_id)
        logger.info(f"WebSocket connection {connection_id} disconnected")

    except Exception as e:
        logger.error(f"WebSocket error for connection {connection_id}: {e}")
        manager.disconnect(connection_id)


@router.get("/stats")
async def get_websocket_stats():
    """
    Get WebSocket connection statistics.

    Returns:
        {
            "total_connections": 42,
            "subscriptions": {
                "alerts": 35,
                "prices": 20,
                "indicators": 10,
                "news": 15,
                "activity": 5
            },
            "connections": [...]
        }
    """
    return manager.get_stats()

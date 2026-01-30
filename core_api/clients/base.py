"""Base socket client for communicating with Mojo services."""

import asyncio
import json
import socket
from typing import Any, Dict, Optional


class MojoSocketClient:
    """Base client for Unix/TCP socket communication with Mojo services."""

    def __init__(
        self,
        socket_path: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        use_unix: bool = True,
    ):
        """Initialize socket client.

        Args:
            socket_path: Path to Unix socket (if use_unix=True)
            host: TCP host (if use_unix=False)
            port: TCP port (if use_unix=False)
            use_unix: Use Unix sockets (True) or TCP (False)
        """
        self.socket_path = socket_path
        self.host = host
        self.port = port
        self.use_unix = use_unix
        self.sock: Optional[socket.socket] = None

    async def connect(self) -> None:
        """Establish connection to Mojo service."""
        if self.use_unix:
            if not self.socket_path:
                raise ValueError("socket_path required for Unix sockets")
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            await asyncio.get_event_loop().run_in_executor(
                None, self.sock.connect, self.socket_path
            )
        else:
            if not self.host or not self.port:
                raise ValueError("host and port required for TCP sockets")
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            await asyncio.get_event_loop().run_in_executor(
                None, self.sock.connect, (self.host, self.port)
            )

    async def send_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Send JSON request and receive JSON response.

        Args:
            request: Dictionary to send as JSON

        Returns:
            Response dictionary

        Raises:
            ConnectionError: If not connected or connection fails
            ValueError: If response is not valid JSON
        """
        if not self.sock:
            await self.connect()

        # Serialize request to JSON
        request_json = json.dumps(request)
        request_bytes = request_json.encode("utf-8")

        # Send length prefix (4 bytes) + request
        length = len(request_bytes)
        length_bytes = length.to_bytes(4, byteorder="big")

        await asyncio.get_event_loop().run_in_executor(
            None, self.sock.sendall, length_bytes + request_bytes
        )

        # Receive length prefix (4 bytes)
        length_bytes = await asyncio.get_event_loop().run_in_executor(
            None, self._recv_exact, 4
        )
        response_length = int.from_bytes(length_bytes, byteorder="big")

        # Receive response
        response_bytes = await asyncio.get_event_loop().run_in_executor(
            None, self._recv_exact, response_length
        )

        # Deserialize JSON response
        response_json = response_bytes.decode("utf-8")
        return json.loads(response_json)

    def _recv_exact(self, num_bytes: int) -> bytes:
        """Receive exactly num_bytes from socket.

        Args:
            num_bytes: Number of bytes to receive

        Returns:
            Received bytes

        Raises:
            ConnectionError: If connection closes before receiving all bytes
        """
        chunks = []
        bytes_received = 0

        while bytes_received < num_bytes:
            chunk = self.sock.recv(num_bytes - bytes_received)
            if not chunk:
                raise ConnectionError("Socket connection closed")
            chunks.append(chunk)
            bytes_received += len(chunk)

        return b"".join(chunks)

    async def ping(self) -> bool:
        """Ping the service to check if it's alive.

        Returns:
            True if service responds to ping

        Raises:
            ConnectionError: If service doesn't respond
        """
        response = await self.send_request({"action": "ping"})
        if response.get("status") == "ok":
            return True
        raise ConnectionError(f"Unexpected ping response: {response}")

    async def close(self) -> None:
        """Close the socket connection."""
        if self.sock:
            await asyncio.get_event_loop().run_in_executor(None, self.sock.close)
            self.sock = None

"""Client — the dial-out side of a tunnel."""

import asyncio
import json
import urllib.parse

import websockets

from .tunnel import Tunnel


class Client(Tunnel):
    """Dial-out tunnel client.

    Connects to a host, registers with an id + metadata, and listens
    for incoming requests. Reconnects with exponential backoff.
    """

    def __init__(
        self,
        url: str,
        tunnel_id: str,
        metadata: dict | None = None,
        on_reconnect=None,
    ):
        super().__init__()
        self._url = url.rstrip("/")
        self._tunnel_id = tunnel_id
        self._metadata = metadata or {}
        self._on_reconnect = on_reconnect
        self._reconnect_delay = 1.0
        self._max_reconnect_delay = 60.0
        self._connected_once = False
        self._connected = asyncio.Event()

    async def connect(self) -> None:
        """Connect, register, and listen. Reconnects on failure."""
        self._running = True
        while self._running:
            self._connected.clear()
            try:
                await self._connect_and_listen()
            except Exception:
                pass

            if not self._running:
                break

            if self._connected_once and self._on_reconnect:
                await self._on_reconnect()

            if not self._running:
                break

            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(self._reconnect_delay * 2, self._max_reconnect_delay)
            self._connected_once = True

    async def _connect_and_listen(self) -> None:
        ws_url = _ws_url(self._url) + "/tunnel/connect"
        ws = await websockets.connect(ws_url)
        self._ws = ws
        try:
            await self._register()
            self._reconnect_delay = 1.0
            self._connected_once = True
            self._connected.set()
            await self.listen()
        finally:
            await ws.close()

    async def _register(self) -> None:
        msg = json.dumps({
            "type": "register",
            "tunnel_id": self._tunnel_id,
            "metadata": self._metadata,
        })
        await self._ws.send(msg)
        raw = await self._ws.recv()
        response = json.loads(raw)
        if response.get("type") != "register_ok":
            raise RegistrationRejected(response.get("error", "rejected"))

    async def wait_connected(self, timeout: float = 5.0) -> bool:
        """Wait for the connection to be established and registered."""
        try:
            await asyncio.wait_for(self._connected.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False


class RegistrationRejected(Exception):
    pass


def _ws_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme == "https":
        return parsed._replace(scheme="wss").geturl()
    if parsed.scheme == "http":
        return parsed._replace(scheme="ws").geturl()
    return url

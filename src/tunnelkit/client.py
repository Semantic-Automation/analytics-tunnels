"""Client — the dial-out side of a tunnel.

A ``Client`` is a :class:`TunnelRegistry` node that dials out to hosts. The
``Host`` (its counterpart) is the one that exposes a public ``wss://`` address.

Each dialed connection is a :class:`ClientTunnel` that manages its own
reconnect loop; the ``Client`` node just creates, tracks, and closes them.
"""

import asyncio
import json
import urllib.parse

import websockets

from .registry import TunnelRegistry
from .tunnel import Tunnel


class ClientTunnel(Tunnel):
    """A single dialed-out tunnel connection.

    Connects, registers with an id + metadata, and listens. Reconnects with
    exponential backoff on its own.
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


class Client(TunnelRegistry):
    """A node that dials out to hosts and tracks its connections."""

    def __init__(self, auth=None):
        super().__init__(auth)

    async def connect(
        self,
        url: str,
        tunnel_id: str,
        metadata: dict | None = None,
        on_reconnect=None,
    ) -> ClientTunnel:
        """Dial out to a host and register the resulting tunnel.

        Returns the :class:`ClientTunnel` immediately; it connects and
        reconnects in the background. Displaces any existing tunnel with the
        same ``tunnel_id``.
        """
        tunnel = ClientTunnel(url, tunnel_id, metadata, on_reconnect)
        await self._register_tunnel(tunnel_id, tunnel)
        asyncio.create_task(tunnel.connect())
        return tunnel


class RegistrationRejected(Exception):
    pass


def _ws_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme == "https":
        return parsed._replace(scheme="wss").geturl()
    if parsed.scheme == "http":
        return parsed._replace(scheme="ws").geturl()
    return url
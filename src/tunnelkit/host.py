"""Host — the accept side of a tunnel.

The ``Host`` is a :class:`TunnelRegistry` node that accepts inbound dial-out
connections and serves them. It holds a public ``wss://`` address; the
``Client`` (its counterpart) is the one that dials out.
"""

import asyncio
import json

import websockets

from .auth import Auth, NoAuth
from .registry import TunnelRegistry
from .tunnel import Tunnel


class HostTunnel(Tunnel):
    """A single accepted tunnel connection on the host side."""

    def __init__(self, ws, auth: Auth | None = None):
        super().__init__()
        self._ws = ws
        self._auth = auth or NoAuth()
        self.tunnel_id: str | None = None
        self._registered = asyncio.Event()

    async def listen(self) -> None:
        """Read registration first, then enter the main listen loop."""
        try:
            raw = await self._ws.recv()
            msg = json.loads(raw)
            if msg.get("type") != "register":
                await self._reject("expected register message")
                return

            tunnel_id = msg.get("tunnel_id", "")
            metadata = msg.get("metadata", {})

            if not self._auth.verify_peer(tunnel_id, **metadata):
                await self._reject("authentication failed")
                return

            self.tunnel_id = tunnel_id
            self._registered.set()
            await self._ws.send(json.dumps({"type": "register_ok"}))
            await super().listen()

        except websockets.ConnectionClosed:
            pass

    async def _reject(self, error: str) -> None:
        try:
            await self._ws.send(json.dumps({"type": "register_reject", "error": error}))
        except websockets.ConnectionClosed:
            pass
        self._registered.set()
        await self._ws.close()

    async def wait_registered(self) -> bool:
        """Wait for registration to complete. Returns True if successful."""
        await self._registered.wait()
        return self.tunnel_id is not None


class Host(TunnelRegistry):
    """Accepts and manages inbound tunnel connections."""

    def __init__(self, auth: Auth | None = None):
        super().__init__(auth)

    async def accept(self, ws) -> HostTunnel | None:
        """Accept a new WebSocket connection.

        Verifies the peer via auth and registers it under its ``tunnel_id``.
        Returns the tunnel if registration succeeds, ``None`` if rejected.
        """
        tunnel = HostTunnel(ws, self._auth)
        task = asyncio.create_task(tunnel.listen())

        success = await tunnel.wait_registered()
        if not success:
            return None

        await self._register_tunnel(tunnel.tunnel_id, tunnel)
        return tunnel
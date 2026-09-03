"""TunnelRegistry — role-agnostic tracking of tunnel connections.

Both ``Client`` (dial-out) and ``Host`` (accept) manage their live tunnels
through this shared node layer. ``get_tunnel`` / ``disconnect_tunnel`` /
``list_tunnels`` / ``close`` work identically regardless of which role owns
the registry, so callers don't need to know whether a connection was dialed
out or accepted in.
"""

import asyncio

from .auth import Auth, NoAuth
from .tunnel import Tunnel


class TunnelRegistry:
    """A node that tracks its tunnel connections, keyed by ``tunnel_id``.

    Role-agnostic: both ``Client`` and ``Host`` subclass this and only differ
    in how connections get established (dial-out vs. accept).
    """

    def __init__(self, auth: Auth | None = None):
        self._auth = auth or NoAuth()
        self._tunnels: dict[str, Tunnel] = {}
        self._lock = asyncio.Lock()

    @property
    def auth(self) -> Auth:
        return self._auth

    def get_tunnel(self, tunnel_id: str) -> Tunnel | None:
        return self._tunnels.get(tunnel_id)

    async def disconnect_tunnel(self, tunnel_id: str) -> None:
        tunnel = self._tunnels.pop(tunnel_id, None)
        if tunnel is not None:
            await tunnel.close()

    def list_tunnels(self) -> list[dict]:
        return [{"tunnel_id": tid} for tid in self._tunnels]

    async def close(self) -> None:
        async with self._lock:
            tunnels = list(self._tunnels.values())
            self._tunnels.clear()
        for tunnel in tunnels:
            await tunnel.close()

    async def _register_tunnel(self, tunnel_id: str, tunnel: Tunnel) -> None:
        """Store a tunnel, closing any displaced connection for the same id.

        A second connection registering with the same ``tunnel_id`` displaces
        (closes) the first. That is intentional (a tunnel_id is unique to one
        entity) but it is almost always a bug or a duplicate client, so we log
        it loudly instead of silently killing a live connection.
        """
        async with self._lock:
            old = self._tunnels.pop(tunnel_id, None)
            self._tunnels[tunnel_id] = tunnel
        if old is not None and old is not tunnel:
            print(
                f"[tunnelkit] DISPLACED {tunnel_id}: new {type(tunnel).__name__} "
                f"replaced existing {type(old).__name__}; closing the old connection",
                flush=True,
            )
            await old.close()
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

#: Seconds a connecting peer has to send its ``register`` message.
DEFAULT_REGISTRATION_TIMEOUT = 10.0


class HostTunnel(Tunnel):
    """A single accepted tunnel connection on the host side.

    When the peer drops — cleanly or silently (see ``Tunnel``) — the receive
    loop exits, the connection is torn down and the owning :class:`Host`
    evicts it from its registry, so no dead tunnel lingers.
    """

    def __init__(
        self,
        ws,
        auth: Auth | None = None,
        *,
        registration_timeout: float = DEFAULT_REGISTRATION_TIMEOUT,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._ws = ws
        self._auth = auth or NoAuth()
        self._registration_timeout = registration_timeout
        self.tunnel_id: str | None = None
        self._registered = asyncio.Event()

    async def listen(self) -> None:
        """Read registration first, then enter the main listen loop."""
        try:
            if not await self._register_phase():
                return
            await super().listen()
        except websockets.ConnectionClosed:
            pass
        finally:
            self._registered.set()
            ws = self._ws
            if ws is not None:
                # ``super().listen()`` finalizes (and nulls ``self._ws``) on
                # exit; if it is still set we never reached the receive loop,
                # so close the socket and mark the connection over.
                self._mark_lost()
                await self._close_socket(ws)
                self._ws = None

    async def _register_phase(self) -> bool:
        """Consume + verify the peer's register message. True if accepted."""
        try:
            raw = await asyncio.wait_for(
                self._ws.recv(), timeout=self._registration_timeout
            )
        except asyncio.TimeoutError:
            await self._reject("registration timed out")
            return False
        except websockets.ConnectionClosed:
            return False

        try:
            msg = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            await self._reject("expected register message")
            return False

        if msg.get("type") != "register":
            await self._reject("expected register message")
            return False

        tunnel_id = msg.get("tunnel_id", "")
        metadata = msg.get("metadata", {})

        if not self._auth.verify_peer(tunnel_id, **metadata):
            await self._reject("authentication failed")
            return False

        self.tunnel_id = tunnel_id
        self._registered.set()
        await self._ws.send(json.dumps({"type": "register_ok"}))
        return True

    async def _reject(self, error: str) -> None:
        try:
            await self._ws.send(json.dumps({"type": "register_reject", "error": error}))
        except websockets.ConnectionClosed:
            pass
        self._registered.set()

    async def wait_registered(self) -> bool:
        """Wait for registration to complete. Returns True if successful."""
        await self._registered.wait()
        return self.tunnel_id is not None


class Host(TunnelRegistry):
    """Accepts and manages inbound tunnel connections."""

    def __init__(self, auth: Auth | None = None):
        super().__init__(auth)

    async def accept(
        self,
        ws,
        *,
        heartbeat_interval: float | None = None,
        heartbeat_timeout: float | None = None,
        registration_timeout: float | None = None,
    ) -> HostTunnel | None:
        """Accept a new WebSocket connection.

        Verifies the peer via auth and registers it under its ``tunnel_id``.
        Returns the tunnel if registration succeeds, ``None`` if rejected.
        ``None`` is also returned when the peer registers and then drops
        before ``accept`` returns.
        """
        kwargs: dict = {}
        if heartbeat_interval is not None:
            kwargs["heartbeat_interval"] = heartbeat_interval
        if heartbeat_timeout is not None:
            kwargs["heartbeat_timeout"] = heartbeat_timeout
        if registration_timeout is not None:
            kwargs["registration_timeout"] = registration_timeout

        tunnel = HostTunnel(ws, self._auth, **kwargs)
        # Evict this tunnel from the registry the moment its connection ends,
        # so a silently-dropped peer never leaves a phantom HostTunnel behind.
        tunnel._lost_handler = self._eviction_handler_for(tunnel)

        task = asyncio.create_task(tunnel.listen())
        try:
            success = await tunnel.wait_registered()
        except asyncio.CancelledError:
            task.cancel()
            raise
        if not success:
            return None

        await self._register_tunnel(tunnel.tunnel_id, tunnel)

        # The peer could have registered and dropped before we got here; the
        # teardown has already evicted it, so don't hand out a dead tunnel.
        if tunnel.state == "closed":
            await self._eviction_handler_for(tunnel)()
            return None
        return tunnel

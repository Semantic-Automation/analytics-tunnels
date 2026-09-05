"""Base Tunnel — shared protocol logic for both sides.

Both ``Client`` and ``HostTunnel`` inherit from this class. Once connected,
both ends speak the same language: ``request()`` to send, ``on_request()``
to handle incoming, ``close()`` to disconnect.

Connection health
-----------------

Each side treats *the underlying socket dying* as a single, observable event
and funnels every cause into one teardown path:

* **Clean / crash drop** — the peer closed, its process died, or the TCP
  connection was reset. ``websockets`` surfaces this to the receive loop as a
  ``ConnectionClosed``, so it is detected immediately.
* **Silent drop** — a network partition, power loss or NAT timeout where no
  TCP segment ever arrives. Nothing can signal this faster than probing the
  peer, so each side sends an application-level ``ping`` every
  ``heartbeat_interval`` seconds and declares the connection lost if no frame
  (including the peer's ``pong``) arrives within ``heartbeat_timeout``
  seconds. An RFC-6455 control-frame ping can't drive this deadline because
  ``recv()`` never surfaces automatic ``pong`` frames.

Whichever cause fires first, ``listen()`` exits and runs the same teardown:
pending requests fail fast, the tunnel is marked disconnected, the socket is
closed, and the owning registry (and any ``on_disconnect`` handler) is
notified so no phantom tunnel lingers.
"""

import asyncio
import base64
import inspect
import json
import uuid

import websockets

DEFAULT_HEARTBEAT_INTERVAL = 10.0  # seconds between outbound pings
DEFAULT_HEARTBEAT_TIMEOUT = 30.0   # no inbound frame for this long => peer is gone

#: Seconds to spend attempting a graceful close of a socket that may belong to
#: a dead peer before giving up. Teardown must not stall on a silent peer.
_CLOSE_GRACE = 1.0

_STATE_IDLE = "idle"
_STATE_CONNECTING = "connecting"
_STATE_CONNECTED = "connected"
_STATE_RECONNECTING = "reconnecting"
_STATE_CLOSED = "closed"


class Tunnel:
    """A bidirectional request/response tunnel over WebSocket.

    Subclasses must set ``self._ws`` before calling ``listen()``.
    """

    def __init__(
        self,
        *,
        heartbeat_interval: float | None = DEFAULT_HEARTBEAT_INTERVAL,
        heartbeat_timeout: float | None = DEFAULT_HEARTBEAT_TIMEOUT,
    ):
        self._ws = None
        self._handler = None
        self._pending: dict[str, asyncio.Future] = {}
        self._running = False

        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_timeout = heartbeat_timeout

        # Set once the peer is registered and the connection is usable.
        self._connected = asyncio.Event()
        # Fired (per connection attempt) when that connection ends.
        self._disconnected = asyncio.Event()
        self._state = _STATE_IDLE
        self._closed = False

        # Optional hooks. ``on_disconnect`` is public; ``_lost_handler`` is set
        # by the owning registry so a dead tunnel is evicted (no phantoms).
        self._on_disconnect_handler = None
        self._lost_handler = None

    # -- public lifecycle / health -------------------------------------------

    def on_request(self, handler) -> None:
        """Register a handler for incoming requests.

        ``handler(path, body, headers) -> (status, body)`` — may be sync or
        ``async``; an awaitable result is awaited.
        """
        self._handler = handler

    def on_disconnect(self, handler) -> None:
        """Register a handler invoked once when the connection is lost.

        ``handler()`` may be sync or ``async``. Fires on silent drops and
        deliberate closes alike, after pending requests have been failed.
        """
        self._on_disconnect_handler = handler

    @property
    def state(self) -> str:
        """Current health state: idle|connecting|connected|reconnecting|closed."""
        return self._state

    @property
    def healthy(self) -> bool:
        return self._state == _STATE_CONNECTED

    async def wait_disconnected(self, timeout: float | None = None) -> bool:
        """Wait until the current connection ends.

        Returns ``True`` once the connection is gone (immediately if it already
        is), ``False`` on ``timeout``.
        """
        if self._closed:
            return True
        event = self._disconnected
        try:
            if timeout is None:
                await event.wait()
            else:
                await asyncio.wait_for(event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def request(self, path: str, body: bytes, headers: dict | None = None, timeout: float = 300.0) -> tuple[int, bytes]:
        """Send a request and await the response.

        Returns ``(status, body)``. Fails fast (``TunnelClosed``) when the
        tunnel is not connected instead of parking a request on a dead socket;
        otherwise times out after ``timeout`` seconds.
        """
        if self._closed or self._ws is None:
            raise TunnelClosed("tunnel is not connected")

        request_id = str(uuid.uuid4())
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[request_id] = future

        msg = json.dumps({
            "type": "request",
            "request_id": request_id,
            "path": path,
            "body": base64.b64encode(body).decode("ascii"),
            "headers": headers or {},
        })
        try:
            await self._ws.send(msg)
        except Exception as e:
            self._pending.pop(request_id, None)
            raise TunnelError(f"send failed: {e}") from e

        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending.pop(request_id, None)
            raise TunnelTimeout(f"request {request_id} timed out after {timeout}s")

    async def send_response(self, request_id: str, status: int, body: bytes) -> None:
        """Send a response to a received request."""
        if self._closed or self._ws is None:
            raise TunnelClosed("tunnel is not connected")
        msg = json.dumps({
            "type": "response",
            "request_id": request_id,
            "status": status,
            "body": base64.b64encode(body).decode("ascii"),
        })
        await self._ws.send(msg)

    async def close(self) -> None:
        """Close the tunnel. Either end can call it."""
        self._running = False
        self._closed = True
        self._fail_pending(TunnelClosed("tunnel closed"))
        self._pending.clear()
        if self._ws is not None:
            await self._close_socket(self._ws)

    # -- receive loop + heartbeat --------------------------------------------

    async def listen(self) -> None:
        """Read messages from the socket and dispatch them until it dies.

        Detects both clean and silent drops; see the module docstring. When the
        connection ends for any reason this runs the shared teardown and
        returns.
        """
        self._running = True
        self._closed = False
        self._state = _STATE_CONNECTED
        self._disconnected.clear()

        heartbeat = None
        try:
            heartbeat = asyncio.create_task(self._heartbeat())
            timeout = self._heartbeat_timeout
            while self._running and self._ws is not None:
                try:
                    if not timeout:
                        raw = await self._ws.recv()
                    else:
                        raw = await asyncio.wait_for(self._ws.recv(), timeout=timeout)
                except asyncio.TimeoutError:
                    # No inbound frame (peer pings/pongs included) for a full
                    # heartbeat window: the peer is gone.
                    break
                try:
                    msg = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    continue
                await self._dispatch(msg)
        except websockets.ConnectionClosed:
            pass
        finally:
            if heartbeat is not None:
                heartbeat.cancel()
            await self._finalize()

    async def _heartbeat(self) -> None:
        """Periodically ping the peer so a live-but-idle tunnel stays busy.

        Detection itself lives in ``listen()``'s bounded ``recv`` — this task
        only generates traffic, so it never decides anything and can't race
        the teardown.
        """
        interval = self._heartbeat_interval
        if not interval:
            return
        try:
            while True:
                await asyncio.sleep(interval)
                ws = self._ws
                if ws is None or not self._running or self._closed:
                    return
                try:
                    await ws.send(json.dumps({"type": "ping"}))
                except Exception:
                    # Sending failed => the socket is dying; the recv loop's
                    # teardown will take care of it.
                    return
        except asyncio.CancelledError:
            raise

    # -- dispatch -------------------------------------------------------------

    def _fail_pending(self, exc: Exception) -> None:
        for future in self._pending.values():
            if not future.done():
                future.set_exception(exc)

    async def _dispatch(self, msg: dict) -> None:
        match msg.get("type"):
            case "request":
                if self._handler:
                    asyncio.create_task(self._handle_request(msg))
            case "response":
                self._handle_response(msg)
            case "ping":
                await self._ws.send(json.dumps({"type": "pong"}))
            case "close":
                await self.close()

    async def _handle_request(self, msg: dict) -> None:
        request_id = msg.get("request_id", "")
        path = msg.get("path", "/")
        body = base64.b64decode(msg.get("body", ""))
        headers = msg.get("headers", {})
        try:
            result = self._handler(path, body, headers)
            if inspect.isawaitable(result):
                result = await result
            status, response_body = result
        except Exception as e:
            status, response_body = 500, json.dumps({"error": str(e)}).encode()
        try:
            await self.send_response(request_id, status, response_body)
        except TunnelClosed:
            # The connection died while the handler ran; there is nobody left
            # to deliver the response to.
            pass

    def _handle_response(self, msg: dict) -> None:
        request_id = msg.get("request_id", "")
        future = self._pending.pop(request_id, None)
        if future and not future.done():
            status = msg.get("status", 500)
            body = base64.b64decode(msg.get("body", ""))
            future.set_result((status, body))

    # -- teardown -------------------------------------------------------------

    def _set_state(self, state: str) -> None:
        self._state = state

    def _mark_lost(self) -> None:
        """Mark the current connection as over (used outside ``listen``)."""
        self._closed = True
        self._state = _STATE_CLOSED
        self._connected.clear()
        self._fail_pending(TunnelClosed("connection lost"))
        self._pending.clear()
        self._disconnected.set()

    async def _finalize(self) -> None:
        """Single teardown path for a dead connection (run by ``listen``)."""
        self._closed = True
        self._state = _STATE_CLOSED
        self._connected.clear()
        self._fail_pending(TunnelClosed("connection lost"))
        self._pending.clear()

        if self._on_disconnect_handler is not None:
            try:
                result = self._on_disconnect_handler()
                if inspect.isawaitable(result):
                    await result
            except Exception:
                pass
        if self._lost_handler is not None:
            try:
                await self._lost_handler()
            except Exception:
                pass

        # Signal only once the registry has evicted the dead tunnel, so a
        # waiter observing the disconnect also observes a clean registry.
        self._disconnected.set()

        ws, self._ws = self._ws, None
        if ws is not None:
            await self._close_socket(ws)

    async def _close_socket(self, ws) -> None:
        """Best-effort socket close that can't stall on a silent peer."""
        try:
            await asyncio.wait_for(ws.close(), timeout=_CLOSE_GRACE)
        except Exception:
            pass


class TunnelError(Exception):
    pass


class TunnelTimeout(TunnelError):
    pass


class TunnelClosed(TunnelError):
    pass

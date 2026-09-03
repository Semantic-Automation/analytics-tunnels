"""Base Tunnel — shared protocol logic for both sides.

Both ``Client`` and ``HostTunnel`` inherit from this class. Once connected,
both ends speak the same language: ``request()`` to send, ``on_request()``
to handle incoming, ``close()`` to disconnect.
"""

import asyncio
import base64
import inspect
import json
import uuid

import websockets


class Tunnel:
    """A bidirectional request/response tunnel over WebSocket.

    Subclasses must set ``self._ws`` before calling ``listen()``.
    """

    def __init__(self):
        self._ws: websockets.WebSocketServerProtocol | websockets.WebSocketClientProtocol | None = None
        self._handler = None
        self._pending: dict[str, asyncio.Future] = {}
        self._running = False

    def on_request(self, handler) -> None:
        """Register a handler for incoming requests.

        ``handler(path, body, headers) -> (status, body)`` — may be sync or
        ``async``; an awaitable result is awaited.
        """
        self._handler = handler

    async def request(self, path: str, body: bytes, headers: dict | None = None, timeout: float = 300.0) -> tuple[int, bytes]:
        """Send a request and await the response.

        Returns ``(status, body)``. Times out after ``timeout`` seconds.
        """
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
        msg = json.dumps({
            "type": "response",
            "request_id": request_id,
            "status": status,
            "body": base64.b64encode(body).decode("ascii"),
        })
        await self._ws.send(msg)

    async def close(self) -> None:
        """Close the tunnel."""
        self._running = False
        self._fail_pending(TunnelClosed("tunnel closed"))
        if self._ws:
            await self._ws.close()

    async def listen(self) -> None:
        """Read messages from the socket and dispatch them."""
        self._running = True
        try:
            async for raw in self._ws:
                try:
                    msg = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    continue
                await self._dispatch(msg)
        except websockets.ConnectionClosed:
            pass
        finally:
            self._fail_pending(TunnelClosed("connection lost"))
            self._pending.clear()

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
        await self.send_response(request_id, status, response_body)

    def _handle_response(self, msg: dict) -> None:
        request_id = msg.get("request_id", "")
        future = self._pending.pop(request_id, None)
        if future and not future.done():
            status = msg.get("status", 500)
            body = base64.b64decode(msg.get("body", ""))
            future.set_result((status, body))


class TunnelError(Exception):
    pass


class TunnelTimeout(TunnelError):
    pass


class TunnelClosed(TunnelError):
    pass

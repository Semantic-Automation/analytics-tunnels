"""Tunnel base class tests using a mock WebSocket pair."""

import asyncio
import json

import pytest
import websockets.exceptions

from tunnelkit.tunnel import Tunnel, TunnelClosed, TunnelTimeout


class _MockWS:
    """A mock WebSocket that can be paired with another for testing."""

    def __init__(self):
        self._queue: asyncio.Queue = asyncio.Queue()
        self._closed = False
        self.sent: list[str] = []

    async def send(self, data: str) -> None:
        self.sent.append(data)

    async def recv(self) -> str:
        # Block until a frame arrives or the socket is closed (a closed mock
        # surfaces closure to a pending recv, like a real WebSocket would).
        while True:
            if self._closed and self._queue.empty():
                raise websockets.exceptions.ConnectionClosed(None, None)
            try:
                return await asyncio.wait_for(self._queue.get(), timeout=0.05)
            except asyncio.TimeoutError:
                continue

    async def put(self, data: str) -> None:
        await self._queue.put(data)

    async def close(self) -> None:
        self._closed = True


@pytest.fixture
def paired_ws():
    """Create two mock WebSockets connected to each other."""
    a = _MockWS()
    b = _MockWS()

    original_a_send = a.send
    original_b_send = b.send

    async def a_send(data):
        await original_a_send(data)
        await b.put(data)

    async def b_send(data):
        await original_b_send(data)
        await a.put(data)

    a.send = a_send
    b.send = b_send
    return a, b


@pytest.mark.asyncio
async def test_request_response_roundtrip(paired_ws):
    a, b = paired_ws

    tunnel_a = Tunnel()
    tunnel_a._ws = a
    listen_a = asyncio.create_task(tunnel_a.listen())

    tunnel_b = Tunnel()
    tunnel_b._ws = b
    tunnel_b.on_request(lambda path, body, headers: (200, b"response"))

    listen_b = asyncio.create_task(tunnel_b.listen())

    status, body = await tunnel_a.request("/test", b"hello", {"X-Key": "val"})
    assert status == 200
    assert body == b"response"

    # Verify wire format
    msg = json.loads(a.sent[0])
    assert msg["type"] == "request"
    assert msg["path"] == "/test"
    assert msg["body"] == "aGVsbG8="  # b"hello" in base64
    assert msg["headers"] == {"X-Key": "val"}

    await tunnel_b.close()
    await tunnel_a.close()
    await listen_b
    await listen_a


@pytest.mark.asyncio
async def test_request_timeout(paired_ws):
    a, b = paired_ws

    tunnel_a = Tunnel()
    tunnel_a._ws = a

    # b never responds
    async def run():
        await a.put(json.dumps({"type": "ping"}))

    asyncio.create_task(run())

    with pytest.raises(TunnelTimeout):
        await tunnel_a.request("/test", b"hello", timeout=0.1)


@pytest.mark.asyncio
@pytest.mark.skip(reason="mock _MockWS cannot propagate peer disconnect; covered by integration tests")
async def test_close_rejects_pending_requests(paired_ws):
    a, b = paired_ws

    tunnel_a = Tunnel()
    tunnel_a._ws = a

    async def slow_handler(path, body, headers):
        await asyncio.sleep(10)
        return 200, b"ok"

    tunnel_b = Tunnel()
    tunnel_b._ws = b
    tunnel_b.on_request(slow_handler)

    listen_task = asyncio.create_task(tunnel_b.listen())

    # Start a request (will be pending)
    task = asyncio.create_task(tunnel_a.request("/slow", b"data"))
    await asyncio.sleep(0.05)

    # Close tunnel_b — pending request should fail
    await tunnel_b.close()
    await listen_task

    with pytest.raises(TunnelClosed):
        await task


@pytest.mark.asyncio
async def test_handler_error_returns_500(paired_ws):
    a, b = paired_ws

    tunnel_a = Tunnel()
    tunnel_a._ws = a
    listen_a = asyncio.create_task(tunnel_a.listen())

    def bad_handler(path, body, headers):
        raise RuntimeError("boom")

    tunnel_b = Tunnel()
    tunnel_b._ws = b
    tunnel_b.on_request(bad_handler)

    listen_b = asyncio.create_task(tunnel_b.listen())

    status, body = await tunnel_a.request("/test", b"hi")
    assert status == 500
    assert "boom" in json.loads(body)["error"]

    await tunnel_b.close()
    await tunnel_a.close()
    await listen_b
    await listen_a


@pytest.mark.asyncio
async def test_ping_pong(paired_ws):
    a, b = paired_ws

    tunnel_b = Tunnel()
    tunnel_b._ws = b

    listen_task = asyncio.create_task(tunnel_b.listen())

    await a.send(json.dumps({"type": "ping"}))
    response = await a.recv()
    msg = json.loads(response)
    assert msg["type"] == "pong"

    await tunnel_b.close()
    await listen_task


@pytest.mark.asyncio
async def test_heartbeat_detects_silent_peer():
    """A peer that stops sending entirely is declared lost after the window."""
    a = _MockWS()
    lost = asyncio.Event()
    tunnel = Tunnel(heartbeat_interval=60.0, heartbeat_timeout=0.05)
    tunnel._ws = a
    tunnel.on_disconnect(lambda: lost.set())

    task = asyncio.create_task(tunnel.listen())

    # Nothing is ever delivered to the socket: the bounded recv window elapses.
    assert await asyncio.wait_for(tunnel.wait_disconnected(), timeout=2.0)
    assert tunnel.state == "closed"
    assert tunnel.healthy is False
    assert lost.is_set()
    await task


@pytest.mark.asyncio
async def test_heartbeat_loss_fails_pending_requests():
    """A request pending at the moment of a silent drop fails fast."""
    a = _MockWS()
    tunnel = Tunnel(heartbeat_interval=60.0, heartbeat_timeout=0.3)
    tunnel._ws = a

    task = asyncio.create_task(tunnel.listen())
    request_task = asyncio.create_task(tunnel.request("/slow", b"data", timeout=300))
    await asyncio.sleep(0.05)  # let the request become pending

    assert await asyncio.wait_for(tunnel.wait_disconnected(), timeout=2.0)
    with pytest.raises(TunnelClosed):
        await request_task
    await task


@pytest.mark.asyncio
async def test_request_fails_fast_when_not_connected():
    """request() on a dead tunnel raises immediately, not after a timeout."""
    a = _MockWS()
    tunnel = Tunnel(heartbeat_interval=60.0, heartbeat_timeout=0.05)
    tunnel._ws = a

    task = asyncio.create_task(tunnel.listen())
    assert await asyncio.wait_for(tunnel.wait_disconnected(), timeout=2.0)
    await task

    with pytest.raises(TunnelClosed):
        await tunnel.request("/x", b"y", timeout=10.0)


@pytest.mark.asyncio
async def test_heartbeat_pings_keep_idle_tunnel_alive(paired_ws):
    """Periodic pings/pongs keep a healthy-but-idle tunnel from false-dropping."""
    a, b = paired_ws

    tunnel_a = Tunnel(heartbeat_interval=0.02, heartbeat_timeout=0.1)
    tunnel_a._ws = a
    tunnel_b = Tunnel()
    tunnel_b._ws = b

    listen_a = asyncio.create_task(tunnel_a.listen())
    listen_b = asyncio.create_task(tunnel_b.listen())  # peer that pongs

    await asyncio.sleep(0.4)  # several heartbeat windows

    assert tunnel_a.healthy          # kept alive by the peer's pongs
    assert tunnel_a.state == "connected"

    await tunnel_b.close()
    await tunnel_a.close()
    await listen_b
    await listen_a

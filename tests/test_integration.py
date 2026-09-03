"""Integration tests — full client↔host flow with real WebSockets."""

import asyncio

import pytest
import websockets

from tunnelkit.client import Client
from tunnelkit.host import Host
from tunnelkit.auth import StaticAuth


@pytest.mark.asyncio
async def test_host_to_client_request():
    """Host sends a request to the client and gets a response."""
    auth = StaticAuth(allowed={"spoke-1": {"sign_pub": "abc"}})
    host = Host(auth)
    client = Client(auth)

    async def handler(websocket):
        tunnel = await host.accept(websocket)
        status, body = await tunnel.request("/completion", b"prompt", {})
        assert status == 200
        assert body == b"response from client"

    async with websockets.serve(handler, "localhost", 0) as server:
        port = server.sockets[0].getsockname()[1]
        conn = await client.connect(f"ws://localhost:{port}", "spoke-1", metadata={"sign_pub": "abc"})
        conn.on_request(lambda path, body, headers: (200, b"response from client"))

        # Wait until the client is actually registered and listening before
        # the host handler sends its request (avoids a race on the 300s default
        # request timeout).
        assert await conn.wait_connected(timeout=2.0)
        await asyncio.sleep(0.2)  # let the host's request round-trip complete
        await client.close()


@pytest.mark.asyncio
async def test_client_to_host_request():
    """Client sends a request to the host and gets a response."""
    auth = StaticAuth(allowed={"spoke-1": {"sign_pub": "abc"}})
    host = Host(auth)
    client = Client(auth)

    async def handler(websocket):
        tunnel = await host.accept(websocket)
        tunnel.on_request(lambda path, body, headers: (200, b"response from host"))
        await asyncio.sleep(0.5)  # keep alive for request

    async with websockets.serve(handler, "localhost", 0) as server:
        port = server.sockets[0].getsockname()[1]
        conn = await client.connect(f"ws://localhost:{port}", "spoke-1", metadata={"sign_pub": "abc"})

        assert await conn.wait_connected(timeout=2.0)
        await asyncio.sleep(0.1)

        status, body = await conn.request("/data", b"request-body")
        assert status == 200
        assert body == b"response from host"

        await client.close()


@pytest.mark.asyncio
async def test_bidirectional_requests():
    """Both sides send requests to each other."""
    auth = StaticAuth(allowed={"spoke-1": {"sign_pub": "abc"}})
    host = Host(auth)
    client = Client(auth)
    done = asyncio.Event()

    async def handler(websocket):
        tunnel = await host.accept(websocket)
        tunnel.on_request(lambda path, body, headers: (200, b"host-echo:" + body))

        status, body = await tunnel.request("/ping", b"hello", {})
        assert status == 200
        assert body == b"client-echo:hello"

        # keep the connection alive until the test finishes its own request
        await done.wait()

    async with websockets.serve(handler, "localhost", 0) as server:
        port = server.sockets[0].getsockname()[1]
        conn = await client.connect(f"ws://localhost:{port}", "spoke-1", metadata={"sign_pub": "abc"})
        conn.on_request(lambda path, body, headers: (200, b"client-echo:" + body))

        assert await conn.wait_connected(timeout=2.0)
        await asyncio.sleep(0.2)

        status, body = await conn.request("/ping", b"world", {})
        assert status == 200
        assert body == b"host-echo:world"

        done.set()
        await client.close()
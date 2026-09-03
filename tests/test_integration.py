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

    async def handler(websocket):
        tunnel = await host.accept(websocket)
        status, body = await tunnel.request("/completion", b"prompt", {})
        assert status == 200
        assert body == b"response from client"

    async with websockets.serve(handler, "localhost", 0) as server:
        port = server.sockets[0].getsockname()[1]
        client = Client(f"ws://localhost:{port}", "spoke-1", metadata={"sign_pub": "abc"})
        client.on_request(lambda path, body, headers: (200, b"response from client"))

        task = asyncio.create_task(client.connect())
        # Wait until the client is actually registered and listening before
        # the host handler sends its request (avoids a race on the 300s default
        # request timeout).
        assert await client.wait_connected(timeout=2.0)
        await asyncio.sleep(0.2)  # let the host's request round-trip complete
        await client.close()
        await task


@pytest.mark.asyncio
async def test_client_to_host_request():
    """Client sends a request to the host and gets a response."""
    auth = StaticAuth(allowed={"spoke-1": {"sign_pub": "abc"}})
    host = Host(auth)

    async def handler(websocket):
        tunnel = await host.accept(websocket)
        tunnel.on_request(lambda path, body, headers: (200, b"response from host"))
        await asyncio.sleep(0.5)  # keep alive for request

    async with websockets.serve(handler, "localhost", 0) as server:
        port = server.sockets[0].getsockname()[1]
        client = Client(f"ws://localhost:{port}", "spoke-1", metadata={"sign_pub": "abc"})

        task = asyncio.create_task(client.connect())
        await asyncio.sleep(0.2)

        status, body = await client.request("/data", b"request-body")
        assert status == 200
        assert body == b"response from host"

        await client.close()
        await task


@pytest.mark.asyncio
async def test_bidirectional_requests():
    """Both sides send requests to each other."""
    auth = StaticAuth(allowed={"spoke-1": {"sign_pub": "abc"}})
    host = Host(auth)

    async def handler(websocket):
        tunnel = await host.accept(websocket)
        tunnel.on_request(lambda path, body, headers: (200, b"host-echo:" + body))

        status, body = await tunnel.request("/ping", b"hello", {})
        assert status == 200
        assert body == b"client-echo:hello"

        await asyncio.sleep(0.2)

    async with websockets.serve(handler, "localhost", 0) as server:
        port = server.sockets[0].getsockname()[1]
        client = Client(f"ws://localhost:{port}", "spoke-1", metadata={"sign_pub": "abc"})
        client.on_request(lambda path, body, headers: (200, b"client-echo:" + body))

        task = asyncio.create_task(client.connect())
        await asyncio.sleep(0.2)

        status, body = await client.request("/ping", b"world", {})
        assert status == 200
        assert body == b"host-echo:world"

        await client.close()
        await task

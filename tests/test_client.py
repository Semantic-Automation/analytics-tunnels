"""Client tests."""

import asyncio

import pytest
import websockets

from tunnelkit.client import Client
from tunnelkit.host import Host
from tunnelkit.auth import StaticAuth


@pytest.mark.asyncio
async def test_client_connects_and_registers():
    auth = StaticAuth(allowed={"spoke-1": {"sign_pub": "abc"}})
    host = Host(auth)
    client = Client(auth)

    async def handler(websocket):
        tunnel = await host.accept(websocket)
        assert tunnel is not None
        assert tunnel.tunnel_id == "spoke-1"

    async with websockets.serve(handler, "localhost", 0) as server:
        port = server.sockets[0].getsockname()[1]
        conn = await client.connect(
            f"ws://localhost:{port}", "spoke-1", metadata={"sign_pub": "abc"}
        )

        assert await conn.wait_connected(timeout=2.0)
        assert conn._ws is not None
        assert client.get_tunnel("spoke-1") is conn

        await client.close()


@pytest.mark.asyncio
async def test_client_rejected_by_auth():
    auth = StaticAuth(allowed={"spoke-1": {"sign_pub": "abc"}})
    host = Host(auth)
    client = Client(auth)

    async def handler(websocket):
        tunnel = await host.accept(websocket)
        assert tunnel is None

    async with websockets.serve(handler, "localhost", 0) as server:
        port = server.sockets[0].getsockname()[1]
        conn = await client.connect(
            f"ws://localhost:{port}", "spoke-2", metadata={"sign_pub": "abc"}
        )

        # Auth rejects every attempt; the tunnel never becomes connected.
        assert await conn.wait_connected(timeout=0.5) is False

        await client.close()


@pytest.mark.asyncio
async def test_client_reconnects_on_drop():
    auth = StaticAuth(allowed={"spoke-1": {"sign_pub": "abc"}})
    host = Host(auth)
    client = Client(auth)

    reconnect_count = 0

    async def handler(websocket):
        tunnel = await host.accept(websocket)
        # let the client finish registering before dropping the connection
        await asyncio.sleep(0.05)
        await tunnel.close()

    async with websockets.serve(handler, "localhost", 0) as server:
        port = server.sockets[0].getsockname()[1]
        conn = await client.connect(
            f"ws://localhost:{port}",
            "spoke-1",
            metadata={"sign_pub": "abc"},
        )
        conn._reconnect_delay = 0.01
        conn._max_reconnect_delay = 0.01

        async def on_reconnect():
            nonlocal reconnect_count
            reconnect_count += 1
            if reconnect_count >= 2:
                await conn.close()

        conn._on_reconnect = on_reconnect

        deadline = asyncio.get_event_loop().time() + 3.0
        while reconnect_count < 2 and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.05)
        assert reconnect_count == 2

        await client.close()


@pytest.mark.asyncio
async def test_client_manages_multiple_hosts():
    """A single Client node can dial and track connections to multiple hosts."""
    auth = StaticAuth(allowed={
        "spoke-1": {"sign_pub": "abc"},
        "spoke-2": {"sign_pub": "abc"},
    })
    host = Host(auth)
    client = Client(auth)
    accepted = []

    async def handler(websocket):
        tunnel = await host.accept(websocket)
        accepted.append(tunnel.tunnel_id)

    async with websockets.serve(handler, "localhost", 0) as server:
        port = server.sockets[0].getsockname()[1]
        c1 = await client.connect(f"ws://localhost:{port}", "spoke-1", metadata={"sign_pub": "abc"})
        c2 = await client.connect(f"ws://localhost:{port}", "spoke-2", metadata={"sign_pub": "abc"})

        assert await c1.wait_connected(timeout=2.0)
        assert await c2.wait_connected(timeout=2.0)

        assert client.get_tunnel("spoke-1") is c1
        assert client.get_tunnel("spoke-2") is c2
        assert len(client.list_tunnels()) == 2

        await client.close()
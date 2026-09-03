"""Client tests."""

import asyncio

import pytest
import websockets

from tunnelkit.client import Client, RegistrationRejected
from tunnelkit.host import Host
from tunnelkit.auth import StaticAuth


@pytest.mark.asyncio
async def test_client_connects_and_registers():
    auth = StaticAuth(allowed={"spoke-1": {"sign_pub": "abc"}})
    host = Host(auth)

    async def handler(websocket):
        tunnel = await host.accept(websocket)
        assert tunnel is not None
        assert tunnel.tunnel_id == "spoke-1"

    async with websockets.serve(handler, "localhost", 0) as server:
        port = server.sockets[0].getsockname()[1]
        client = Client(f"ws://localhost:{port}", "spoke-1", metadata={"sign_pub": "abc"})

        task = asyncio.create_task(client.connect())
        assert await client.wait_connected(timeout=2.0)

        assert client._ws is not None
        await client.close()
        await task


@pytest.mark.asyncio
async def test_client_rejected_by_auth():
    auth = StaticAuth(allowed={"spoke-1": {"sign_pub": "abc"}})
    host = Host(auth)

    async def handler(websocket):
        tunnel = await host.accept(websocket)
        assert tunnel is None

    async with websockets.serve(handler, "localhost", 0) as server:
        port = server.sockets[0].getsockname()[1]
        client = Client(f"ws://localhost:{port}", "spoke-2", metadata={"sign_pub": "abc"})

        with pytest.raises(RegistrationRejected):
            await client._connect_and_listen()


@pytest.mark.asyncio
async def test_client_reconnects_on_drop():
    auth = StaticAuth(allowed={"spoke-1": {"sign_pub": "abc"}})
    host = Host(auth)

    reconnect_count = 0

    async def handler(websocket):
        tunnel = await host.accept(websocket)
        # Immediately close to simulate drop
        await tunnel.close()

    async with websockets.serve(handler, "localhost", 0) as server:
        port = server.sockets[0].getsockname()[1]
        client = Client(
            f"ws://localhost:{port}",
            "spoke-1",
            metadata={"sign_pub": "abc"},
        )
        client._reconnect_delay = 0.01
        client._max_reconnect_delay = 0.01

        async def on_reconnect():
            nonlocal reconnect_count
            reconnect_count += 1
            if reconnect_count >= 2:
                await client.close()

        client._on_reconnect = on_reconnect

        await client.connect()
        assert reconnect_count == 2

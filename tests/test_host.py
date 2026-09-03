"""Host tests."""

import asyncio

import pytest
import websockets

from tunnelkit.host import Host, HostTunnel
from tunnelkit.auth import NoAuth, StaticAuth


@pytest.mark.asyncio
async def test_host_accepts_and_registers():
    host = Host(NoAuth())

    async def handler(websocket):
        tunnel = await host.accept(websocket)
        assert tunnel is not None
        assert tunnel.tunnel_id == "spoke-1"

    async with websockets.serve(handler, "localhost", 0) as server:
        port = server.sockets[0].getsockname()[1]
        async with websockets.connect(f"ws://localhost:{port}/tunnel/connect") as ws:
            await ws.send('{"type": "register", "tunnel_id": "spoke-1", "metadata": {}}')
            response = json.loads(await ws.recv())
            assert response["type"] == "register_ok"


@pytest.mark.asyncio
async def test_host_rejects_unauthorized():
    auth = StaticAuth(allowed={"spoke-1": {"sign_pub": "abc"}})
    host = Host(auth)

    async def handler(websocket):
        tunnel = await host.accept(websocket)
        assert tunnel is None

    async with websockets.serve(handler, "localhost", 0) as server:
        port = server.sockets[0].getsockname()[1]
        async with websockets.connect(f"ws://localhost:{port}/tunnel/connect") as ws:
            await ws.send('{"type": "register", "tunnel_id": "spoke-2", "metadata": {"sign_pub": "abc"}}')
            response = json.loads(await ws.recv())
            assert response["type"] == "register_reject"


@pytest.mark.asyncio
async def test_host_list_and_disconnect():
    host = Host(NoAuth())

    async def handler(websocket):
        tunnel = await host.accept(websocket)
        assert tunnel is not None

        tunnels = host.list_tunnels()
        assert len(tunnels) == 1
        assert tunnels[0]["tunnel_id"] == "spoke-1"

        await host.disconnect_tunnel("spoke-1")
        assert host.list_tunnels() == []

    async with websockets.serve(handler, "localhost", 0) as server:
        port = server.sockets[0].getsockname()[1]
        async with websockets.connect(f"ws://localhost:{port}/tunnel/connect") as ws:
            await ws.send('{"type": "register", "tunnel_id": "spoke-1", "metadata": {}}')
            await ws.recv()  # register_ok


@pytest.mark.asyncio
async def test_host_close_all():
    host = Host(NoAuth())

    async def handler(websocket):
        tunnel = await host.accept(websocket)
        assert tunnel is not None

        await host.close()
        assert host.list_tunnels() == []

    async with websockets.serve(handler, "localhost", 0) as server:
        port = server.sockets[0].getsockname()[1]
        async with websockets.connect(f"ws://localhost:{port}/tunnel/connect") as ws:
            await ws.send('{"type": "register", "tunnel_id": "spoke-1", "metadata": {}}')
            await ws.recv()  # register_ok


import json  # noqa: E402

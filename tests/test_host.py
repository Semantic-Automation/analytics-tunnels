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


@pytest.mark.asyncio
async def test_host_closes_orphaned_tunnel_on_duplicate_id():
    """Re-registering the same tunnel_id closes the displaced connection."""
    host = Host(NoAuth())
    closed_ids = []

    async def handler(websocket):
        tunnel = await host.accept(websocket)
        if tunnel is None:
            return

        original_close = tunnel.close

        async def spy_close():
            closed_ids.append(tunnel.tunnel_id)
            await original_close()

        tunnel.close = spy_close

        # Keep the handler (and thus the connection) alive until the tunnel
        # dies — by displacement or by a peer drop.
        await tunnel.wait_disconnected()

    async with websockets.serve(handler, "localhost", 0) as server:
        port = server.sockets[0].getsockname()[1]
        url = f"ws://localhost:{port}/tunnel/connect"

        # First connection registers and stays open.
        async with websockets.connect(url) as ws1:
            await ws1.send('{"type": "register", "tunnel_id": "spoke-1", "metadata": {}}')
            await ws1.recv()  # register_ok
            # wait until the host has it registered
            while host.get_tunnel("spoke-1") is None:
                await asyncio.sleep(0.01)

            # Second connection with the same id displaces the first.
            async with websockets.connect(url) as ws2:
                await ws2.send('{"type": "register", "tunnel_id": "spoke-1", "metadata": {}}')
                await ws2.recv()  # register_ok

                # The displaced tunnel is closed; its peer socket drops.
                with pytest.raises(websockets.exceptions.ConnectionClosed):
                    await asyncio.wait_for(ws1.recv(), timeout=2.0)

            # Once the second client also disconnects, the host evicts it.
            while host.get_tunnel("spoke-1") is not None:
                await asyncio.sleep(0.01)

        assert host.list_tunnels() == []
        assert closed_ids == ["spoke-1"]

        await host.close()


@pytest.mark.asyncio
async def test_host_evicts_tunnel_on_silent_peer_drop():
    """A peer that goes silent (no TCP teardown) is detected and evicted."""
    host = Host(NoAuth())
    dropped = asyncio.Event()

    async def handler(websocket):
        tunnel = await host.accept(
            websocket, heartbeat_interval=0.1, heartbeat_timeout=0.3
        )
        if tunnel is None:
            return
        await tunnel.wait_disconnected()
        dropped.set()

    async with websockets.serve(handler, "localhost", 0) as server:
        port = server.sockets[0].getsockname()[1]
        async with websockets.connect(f"ws://localhost:{port}/tunnel/connect") as ws:
            await ws.send('{"type": "register", "tunnel_id": "spoke-1", "metadata": {}}')
            await ws.recv()  # register_ok
            while host.get_tunnel("spoke-1") is None:
                await asyncio.sleep(0.01)
            assert host.get_tunnel("spoke-1") is not None

            # Go silent: never read again, so the host's pings go unanswered.
            # (No TCP teardown happens — the connection stays open.)
            assert await asyncio.wait_for(dropped.wait(), timeout=3.0)

        # After the drop the host evicted the tunnel: no phantom remains.
        assert host.get_tunnel("spoke-1") is None
        assert host.list_tunnels() == []
        await host.close()

"""TunnelRegistry — shared, role-agnostic registry tests.

get_tunnel / disconnect_tunnel / list_tunnels / close must behave identically
whether the node is a Client (dial-out) or a Host (accept).
"""

import asyncio

import pytest
import websockets

from tunnelkit.auth import NoAuth, StaticAuth
from tunnelkit.client import Client
from tunnelkit.host import Host


@pytest.mark.asyncio
async def test_registry_interface_is_identical_across_roles():
    """Both a Client and a Host expose the same registry surface."""
    client = Client(auth=None)
    host = Host(auth=None)

    for node in (client, host):
        assert hasattr(node, "get_tunnel")
        assert hasattr(node, "disconnect_tunnel")
        assert hasattr(node, "list_tunnels")
        assert hasattr(node, "close")
        assert node.list_tunnels() == []


@pytest.mark.asyncio
async def test_client_registry_orphan_close_on_duplicate_id():
    """Dialing the same tunnel_id twice closes the displaced connection."""
    auth = StaticAuth(allowed={"spoke-1": {"sign_pub": "abc"}})
    host = Host(auth)
    client = Client(auth)

    async def handler(websocket):
        tunnel = await host.accept(websocket)
        if tunnel is None:
            return
        await tunnel.wait_disconnected()

    async with websockets.serve(handler, "localhost", 0) as server:
        port = server.sockets[0].getsockname()[1]
        url = f"ws://localhost:{port}"

        async def dial_once():
            conn = await client.connect(url, "spoke-1", metadata={"sign_pub": "abc"})
            assert await conn.wait_connected(timeout=2.0)
            return conn

        # First dial registers; second dial (same id) displaces the first.
        first = await dial_once()
        await asyncio.sleep(0.1)
        second = await dial_once()

        # The first connection is closed by the displacement...
        assert await first.wait_disconnected(timeout=2.0)
        # ...and only the new connection remains, on both sides.
        assert len(client.list_tunnels()) == 1
        assert client.get_tunnel("spoke-1") is second
        assert len(host.list_tunnels()) == 1
        assert host.get_tunnel("spoke-1").tunnel_id == "spoke-1"

        await host.close()
        await client.close()


@pytest.mark.asyncio
async def test_registry_get_disconnect_list_close():
    """get/disconnect/list/close work through the shared registry."""
    auth = StaticAuth(allowed={"spoke-1": {"sign_pub": "abc"}})
    host = Host(auth)
    client = Client(auth)

    async def handler(websocket):
        tunnel = await host.accept(websocket)
        await asyncio.sleep(0.5)  # keep alive

    async with websockets.serve(handler, "localhost", 0) as server:
        port = server.sockets[0].getsockname()[1]
        conn = await client.connect(f"ws://localhost:{port}", "spoke-1", metadata={"sign_pub": "abc"})
        assert await conn.wait_connected(timeout=2.0)

        assert client.get_tunnel("spoke-1") is conn
        assert client.list_tunnels() == [{"tunnel_id": "spoke-1", "state": "connected"}]

        await client.disconnect_tunnel("spoke-1")
        assert client.get_tunnel("spoke-1") is None
        assert client.list_tunnels() == []

        await client.close()
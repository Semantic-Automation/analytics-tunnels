"""TunnelManager / TunnelConnection transport tests."""

import asyncio

from tunnelkit.tunnel import TunnelConnection, TunnelManager


def _conn(tid="spoke-1", sign_pub="sp"):
    return TunnelConnection(tid, sign_pub, lambda msg: None)


def test_manager_register_gate():
    seen = {}

    def verifier(tid, kem_pub, x_pub, sign_pub):
        seen[tid] = sign_pub
        return tid == "ok"

    m = TunnelManager(verifier=verifier)
    assert m.register("ok", "k", "x", "s1") is True
    assert m.register("bad", "k", "x", "s2") is False
    assert seen == {"ok": "s1", "bad": "s2"}


def test_manager_no_verifier_accepts_all():
    m = TunnelManager()
    assert m.register("anything", "k", "x", "s") is True


def test_manager_connect_get_disconnect():
    m = TunnelManager()
    m.connect_tunnel("spoke-1", _conn())
    assert m.get_tunnel("spoke-1") is not None
    m.disconnect_tunnel("spoke-1")
    assert m.get_tunnel("spoke-1") is None


def test_manager_revalidate_disconnects_revoked():
    revoked = {"revoked": "s1"}

    def verifier(tid, *a):
        return tid not in revoked

    m = TunnelManager(verifier=verifier)
    m.connect_tunnel("revoked", _conn())
    m.connect_tunnel("kept", _conn())
    disconnected = m.revalidate()
    assert disconnected == ["revoked"]
    assert m.get_tunnel("revoked") is None
    assert m.get_tunnel("kept") is not None


def test_connection_send_deliver_roundtrip():
    async def run():
        sent = []

        async def write_fn(data):
            sent.append(data)

        conn = TunnelConnection("spoke-1", "sp", write_fn)
        task = asyncio.create_task(conn.send_request("r1", "/completion", b"body", {}))
        # let send_request write its message
        await asyncio.sleep(0.05)
        conn.deliver_response("r1", 200, b"resp")
        status, body = await task
        return sent, status, body

    sent, status, body = asyncio.run(run())
    assert status == 200
    assert body == b"resp"
    import json

    msg = json.loads(sent[0])
    assert msg["type"] == "request" and msg["request_id"] == "r1"
    assert msg["body"] == "Ym9keQ=="  # b"body"


def test_connection_send_timeout():
    async def run():
        async def write_fn(data):
            pass

        conn = TunnelConnection("spoke-1", "sp", write_fn)
        status, body = await conn.send_request("r1", "/c", b"b", {}, timeout_s=0.05)
        return status, body

    status, body = asyncio.run(run())
    assert status == 504
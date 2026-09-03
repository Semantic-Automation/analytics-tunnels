"""TunnelManager / TunnelConnection — the accept side of reverse tunnels.

The acceptor (edge proxy today, builder acceptor tomorrow) receives dial-out
WebSocket connections from peers (spokes/builders), verifies them via an
injected ``verifier``, holds their sockets, and sends requests through them
(``send_request``). Transport only — no crypto, no manifest loading; the
consumer owns identity/role verification and routing state.

Wire format (JSON over WebSocket):

    register:  {spoke_id, kem_pub, x_pub, sign_pub}
    request:   {type: "request", request_id, path, body(b64), headers}
    response:  {type: "response", request_id, status, body(b64)}
    health:    {type: "health", report}
"""

import asyncio
import base64
import json
import threading
import time


class TunnelManager:
    """Tracks peer tunnels.

    ``verifier(tunnel_id, kem_pub, x_pub, sign_pub) -> bool`` decides whether a
    connecting peer may register. When None, every peer is accepted (dev).
    """

    def __init__(self, verifier=None):
        self._verifier = verifier
        self._tunnels: dict[str, "TunnelConnection"] = {}
        self._lock = threading.Lock()

    def register(self, tunnel_id: str, kem_pub: str, x_pub: str, sign_pub: str) -> bool:
        """Verify + accept a connecting peer. Returns True to register."""
        if self._verifier is None:
            return True
        return bool(self._verifier(tunnel_id, kem_pub, x_pub, sign_pub))

    def connect_tunnel(self, tunnel_id: str, conn: "TunnelConnection") -> None:
        """Attach an active tunnel, closing any prior connection for the id."""
        with self._lock:
            old = self._tunnels.get(tunnel_id)
            if old:
                old.close()
            self._tunnels[tunnel_id] = conn
        print(f"[tunnel] connected: {tunnel_id}", flush=True)

    def disconnect_tunnel(self, tunnel_id: str) -> None:
        with self._lock:
            conn = self._tunnels.pop(tunnel_id, None)
        if conn:
            conn.close()
        print(f"[tunnel] disconnected: {tunnel_id}", flush=True)

    def get_tunnel(self, tunnel_id: str) -> "TunnelConnection | None":
        with self._lock:
            return self._tunnels.get(tunnel_id)

    def list_tunnels(self) -> list[dict]:
        with self._lock:
            return [
                {"tunnel_id": tid, "connected_at": c.connected_at, "last_activity": c.last_activity}
                for tid, c in self._tunnels.items()
            ]

    def revalidate(self) -> list[str]:
        """Re-run the verifier against every tunnel's registered sign_pub.

        Disconnects peers the verifier now rejects (revoked/rotated). Returns
        the list of disconnected ids.
        """
        if self._verifier is None:
            return []
        to_disconnect = []
        with self._lock:
            for tid, conn in self._tunnels.items():
                if not self._verifier(tid, "", "", conn.sign_pub):
                    to_disconnect.append(tid)
        for tid in to_disconnect:
            self.disconnect_tunnel(tid)
            print(f"[tunnel] revoked: {tid}", flush=True)
        return to_disconnect


class TunnelConnection:
    """A single peer tunnel.

    Uses ``asyncio.Event`` (not ``threading.Event``) so ``send_request`` can be
    awaited from the acceptor's event loop without blocking it.
    """

    def __init__(self, tunnel_id: str, sign_pub: str, write_fn):
        self.tunnel_id = tunnel_id
        self.sign_pub = sign_pub
        self._write = write_fn
        self.connected_at = time.time()
        self.last_activity = time.time()
        self._pending: dict[str, tuple[asyncio.Event, list]] = {}
        self._lock = asyncio.Lock()

    async def send_request(
        self, request_id: str, path: str, body: bytes, headers: dict, timeout_s: float = 300.0
    ) -> tuple[int, bytes]:
        """Send a request through the tunnel and wait for the response.

        Returns ``(status_code, response_body)``; ``(504, b'')`` on timeout.
        """
        event = asyncio.Event()
        slot: list = [None, None]  # [status, body]
        async with self._lock:
            self._pending[request_id] = (event, slot)

        msg = json.dumps({
            "type": "request",
            "request_id": request_id,
            "path": path,
            "body": base64.b64encode(body).decode("ascii"),
            "headers": headers,
        }).encode("utf-8")

        self.last_activity = time.time()
        try:
            await self._write(msg)
        except Exception as e:  # noqa: BLE001
            async with self._lock:
                self._pending.pop(request_id, None)
            return 500, b'{"error": "tunnel write failed"}'

        try:
            await asyncio.wait_for(event.wait(), timeout=timeout_s)
        except asyncio.TimeoutError:
            async with self._lock:
                self._pending.pop(request_id, None)
            return 504, b'{"error": "tunnel timeout"}'

        async with self._lock:
            slot = self._pending.pop(request_id, (None, [None, None]))[1]

        status, resp_body = slot
        if status is None:
            return 504, b'{"error": "tunnel timeout"}'

        self.last_activity = time.time()
        return status, resp_body

    def deliver_response(self, request_id: str, status: int, body: bytes) -> None:
        """Deliver a peer's response to the waiting ``send_request``."""
        entry = self._pending.get(request_id)
        if entry is None:
            return
        event, slot = entry
        slot[0] = status
        slot[1] = body
        event.set()

    def close(self) -> None:
        for event, slot in self._pending.values():
            slot[0] = 504
            slot[1] = b'{"error": "tunnel disconnected"}'
            event.set()
        self._pending.clear()
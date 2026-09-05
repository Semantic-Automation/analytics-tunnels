# analytics-tunnels

Symmetric WebSocket tunnels. Both ends of a connection share the **same**
request/response API — the tunnel doesn't care which side is the "client" or
the "host".

Transport-only: the library moves bytes and frames. **No crypto, no keys.**
Anything security-related lives in an injected `Auth` implementation (typically
backed by `secretskit`).

## Install

```bash
pip install "analytics-tunnels @ git+https://github.com/Semantic-Automation/analytics-tunnels.git@v0.2.0"
```

Requires Python >= 3.11 and [`websockets`](https://websockets.readthedocs.io/) >= 12.

## Public API

Everything lives in the `tunnelkit` package.

### Two layers

Every node is a **`TunnelRegistry`** — a role-agnostic registry of its live
connections, keyed by `tunnel_id`. `Client` (dial-out) and `Host` (accept) are
both registries; they differ *only* in how connections get established.

```python
class TunnelRegistry:                  # shared node layer
    def get_tunnel(self, tunnel_id) -> Tunnel | None
    async def disconnect_tunnel(self, tunnel_id) -> None
    def list_tunnels(self) -> list[dict]
    async def close(self) -> None
```

Because the registry is shared, `get_tunnel`, `disconnect_tunnel`,
`list_tunnels` and `close` behave identically whether you're calling a client
or a host — callers don't need to know which role a tunnel belongs to.

### Tunnel — a single connection (either end)

Each connection is a `Tunnel`, whether a `ClientTunnel` (dialed) or a
`HostTunnel` (accepted). Both support the same symmetric methods:

| Method | Signature | Purpose |
|--------|-----------|---------|
| `request` | `await tunnel.request(path, body, headers=None, timeout=300.0) -> (int, bytes)` | Send a request, await the response. Raises `TunnelTimeout` on timeout. |
| `on_request` | `tunnel.on_request(handler)` | Register `handler(path, body, headers) -> (int, bytes)` for incoming requests. |
| `send_response` | `await tunnel.send_response(request_id, status, body)` | Manually reply to a received request. |
| `state` | `tunnel.state` | Health: `idle`/`connecting`/`connected`/`reconnecting`/`closed`. |
| `healthy` | `tunnel.healthy` | `True` while the tunnel reports `connected`. |
| `on_disconnect` | `tunnel.on_disconnect(handler)` | Register `handler()` invoked once when the connection is lost. |
| `wait_disconnected` | `await tunnel.wait_disconnected(timeout=None) -> bool` | Wait until the current connection ends. |
| `close` | `await tunnel.close()` | Close the connection. Either end can call it. |

`request()` **fails fast**: it raises `TunnelClosed` immediately when the
tunnel isn't connected, instead of parking a 300s request on a dead socket.

### Client — dial-out side

```python
from tunnelkit import Client

client = Client(auth)                       # a node that dials out
conn = await client.connect(
    url="wss://edge.example.com",
    tunnel_id="spoke-1",
    metadata={"sign_pub": "..."},
)                                           # returns a ClientTunnel
conn.on_request(lambda path, body, headers: (200, b"ok"))

await conn.wait_connected(timeout=5.0)      # optional: wait until registered
```

- Dials `wss://<host>/tunnel/connect`, registers with `tunnel_id` + `metadata`.
- Each `ClientTunnel` reconnects independently with exponential backoff
  (1s → 60s). `on_reconnect` (a zero-arg async callback) runs before each
  reconnect.
- One `Client` node can manage many connections (e.g. one per host).
- `client.close()` stops all of them.

### Host — accept side

```python
from tunnelkit import Host, StaticAuth

auth = StaticAuth(allowed={"spoke-1": {"sign_pub": "..."}})
host = Host(auth)                       # a node that accepts inbound connections
```

`Host` manages accepted connections. Wire it into your WebSocket server:

```python
async def ws_handler(websocket):
    tunnel = await host.accept(websocket)   # verifies via auth; None if rejected
    if tunnel is None:
        return
    tunnel.on_request(lambda path, body, headers: (200, b"ok"))
    await tunnel.request("/completion", b"prompt")   # symmetric: host can also call
    await asyncio.sleep(...)  # keep the connection alive while serving
```

| Method | Purpose |
|--------|---------|
| `accept(ws)` | Accept a connection; returns a `HostTunnel` or `None` if rejected. |

Since `Host` is a `TunnelRegistry`, it also supports `get_tunnel`,
`disconnect_tunnel`, `list_tunnels` and `close` — identical to `Client`.

### Duplicate `tunnel_id`

Registering/dialing a `tunnel_id` that already exists **closes the displaced
connection** — on both client and host — so no tunnel is ever orphaned.

## Connection health

Both sides detect when the underlying connection dies, funneling **every**
cause into one teardown path:

* **Clean / crash drop** — the peer closed, its process died, or the TCP
  connection was reset. The `websockets` library surfaces this to the receive
  loop immediately, so it is detected instantly.
* **Silent drop** — a network partition, power loss or NAT timeout where no TCP
  segment ever arrives. Nothing can detect this faster than probing the peer:
  each side sends an application-level `ping` every `heartbeat_interval`
  seconds and declares the connection lost if no frame (including the peer's
  `pong`) arrives within `heartbeat_timeout`.

When either fires, pending requests fail with `TunnelClosed`, the socket is
closed, `on_disconnect` handlers run, and the owning registry **evicts the
dead tunnel** — a `HostTunnel` never lingers as a phantom after its peer
vanishes. A dial-out `ClientTunnel` instead keeps its registry entry (it
reconnects on its own) but reports `state == "reconnecting"` until it is
re-established.

Defaults are `heartbeat_interval=10s`, `heartbeat_timeout=30s` (so a silent
drop is noticed within ~30–40s). Pass `heartbeat_interval=0` /
`heartbeat_timeout=0` to disable. Both are configurable per connection:

```python
conn = await client.connect(url, "spoke-1", metadata={...},
                            heartbeat_interval=5.0, heartbeat_timeout=15.0)
# ...or on the accept side:
tunnel = await host.accept(websocket,
                           heartbeat_interval=5.0, heartbeat_timeout=15.0)
```

## Authentication

`Auth` is a protocol with a single method:

```python
class Auth(Protocol):
    def verify_peer(self, tunnel_id: str, **metadata) -> bool: ...
```

Built-ins:

- `NoAuth()` — accepts all peers (dev mode).
- `StaticAuth(allowed={...})` — accepts a peer only if its `tunnel_id` is present
  and its `metadata` matches the allowlist entry exactly.

Both `Client` and `Host` accept an injected `Auth`. The tunnel never verifies
anything itself.

## Wire protocol (JSON over WebSocket)

```
register:        {type: "register", tunnel_id, metadata}
register_ok:     {type: "register_ok"}
register_reject: {type: "register_reject", error}
request:         {type: "request", request_id, path, body(b64), headers}
response:        {type: "response", request_id, status, body(b64)}
ping:            {type: "ping"}  ->  {type: "pong"}
```

The `ping`/`pong` message pair doubles as the heartbeat: both sides send
`ping` periodically and treat silence as a lost connection (see Connection
health above). There are no special-purpose message types (no manifest
pushes, no health reports) beyond this.

`list_tunnels()` entries now carry each tunnel's health:
`{"tunnel_id": "...", "state": "connected"}`.

## Examples

Bidirectional requests — both ends speak identically:

```python
# host side
tunnel.on_request(lambda path, body, headers: (200, b"host-echo:" + body))
status, body = await tunnel.request("/ping", b"hello")
```

```python
# client side
conn = await client.connect(url="wss://edge.example.com", tunnel_id="spoke-1", metadata={...})
conn.on_request(lambda path, body, headers: (200, b"client-echo:" + body))
status, body = await conn.request("/ping", b"world")
```

## Testing

```bash
python -m venv .testvenv && .testvenv/bin/pip install -e . pytest pytest-asyncio
.testvenv/bin/pytest tests/ -q
```

Result: `29 passed, 1 skipped`.

### Known test issue

`tests/test_tunnel.py::test_close_rejects_pending_requests` is **skipped**. The
in-memory `_MockWS` cannot propagate a peer disconnect to the requester, so
"close rejects pending requests" can't be exercised there. That behavior is
covered by the real-WebSocket integration tests in `tests/test_integration.py`.

## Layout

```
src/tunnelkit/
  __init__.py   # exports: Tunnel, TunnelRegistry, Client, ClientTunnel, Host, HostTunnel, ...
  tunnel.py     # Tunnel base class: shared request/response/close/listen
  registry.py   # TunnelRegistry node layer: get/disconnect/list/close (role-agnostic)
  client.py     # ClientTunnel (dial-out + reconnect) + Client (node that dials)
  host.py       # HostTunnel (accepted connection) + Host (node that accepts)
  auth.py       # Auth protocol, NoAuth, StaticAuth
```

Legacy `TunnelClient` / `TunnelManager` / `TunnelConnection` code is preserved
(read-only) under `archive/`.
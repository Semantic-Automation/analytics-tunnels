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

### Shared — both ends

Both `Client` and `HostTunnel` extend the base `Tunnel` class, so both support:

| Method | Signature | Purpose |
|--------|-----------|---------|
| `request` | `await tunnel.request(path, body, headers=None, timeout=300.0) -> (int, bytes)` | Send a request, await the response. Raises `TunnelTimeout` on timeout. |
| `on_request` | `tunnel.on_request(handler)` | Register `handler(path, body, headers) -> (int, bytes)` for incoming requests. |
| `send_response` | `await tunnel.send_response(request_id, status, body)` | Manually reply to a received request. |
| `close` | `await tunnel.close()` | Close the connection. Either end can call it. |

### Client — dial-out side

```python
from tunnelkit import Client

client = Client(url="wss://edge.example.com", tunnel_id="spoke-1", metadata={"sign_pub": "..."})
client.on_request(lambda path, body, headers: (200, b"ok"))

await client.connect()      # connect, register, listen; reconnects on drop
await client.wait_connected(timeout=5.0)   # optional: wait until registered
```

- Dials `wss://<host>/tunnel/connect`, registers with `tunnel_id` + `metadata`.
- Reconnects with exponential backoff (1s → 60s). `on_reconnect` (a zero-arg
  async callback) runs before each reconnect.
- `close()` stops the reconnect loop.

### Host — accept side

```python
from tunnelkit import Host, StaticAuth

auth = StaticAuth(allowed={"spoke-1": {"sign_pub": "..."}})
host = Host(auth)
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
| `get_tunnel(tunnel_id)` | Look up a registered tunnel. |
| `disconnect_tunnel(tunnel_id)` | Close a specific tunnel. |
| `list_tunnels()` | List registered tunnel ids. |
| `close()` | Close all tunnels. |

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

There are no special-purpose message types (no manifest pushes, no health
reports). Any application-level signal travels over `request`/`response`.

## Examples

Bidirectional requests — both ends speak identically:

```python
# host side
tunnel.on_request(lambda path, body, headers: (200, b"host-echo:" + body))
status, body = await tunnel.request("/ping", b"hello")
```

```python
# client side
client.on_request(lambda path, body, headers: (200, b"client-echo:" + body))
status, body = await client.request("/ping", b"world")
```

## Testing

```bash
python -m venv .testvenv && .testvenv/bin/pip install -e . pytest pytest-asyncio
.testvenv/bin/pytest tests/ -q
```

Result: `18 passed, 1 skipped`.

### Known test issue

`tests/test_tunnel.py::test_close_rejects_pending_requests` is **skipped**. The
in-memory `_MockWS` cannot propagate a peer disconnect to the requester, so
"close rejects pending requests" can't be exercised there. That behavior is
covered by the real-WebSocket integration tests in `tests/test_integration.py`.

## Layout

```
src/tunnelkit/
  __init__.py   # exports: Tunnel, Client, Host, HostTunnel, Auth, NoAuth, StaticAuth, ...
  tunnel.py     # Tunnel base class: shared request/response/close/listen
  client.py     # Client: dial-out + reconnect
  host.py       # Host (manager) + HostTunnel (single accepted connection)
  auth.py       # Auth protocol, NoAuth, StaticAuth
```

Legacy `TunnelClient` / `TunnelManager` / `TunnelConnection` code is preserved
(read-only) under `archive/`.
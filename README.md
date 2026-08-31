# analytics-tunnels

Transport-only WebSocket reverse-tunnel machinery, shared by the spoke, the
builder, and the edge proxy. Moves bytes and frames only — **no crypto, no
keys**. Anything security-related lives in the consumer's injected
handler/verifier (usually backed by `secretskit`).

## Components

- `TunnelClient` (`tunnelkit.tunnel_client`) — dial-out side. Connects to
  `wss://<host>/tunnel/connect`, registers with an id + public keys, keeps the
  socket alive (proxy pings + local heartbeats), and dispatches incoming
  requests to an injected
  `handler(request_id, path, body, headers) -> (status, body)`.
- `TunnelManager` / `TunnelConnection` (`tunnelkit.tunnel`) — accept side.
  Verifies connecting peers via an injected
  `verifier(tunnel_id, kem_pub, x_pub, sign_pub) -> bool`, holds their sockets,
  and sends requests through them (`send_request` / `deliver_response`).

## Wire protocol (JSON over WebSocket)

```
register:  {spoke_id, kem_pub, x_pub, sign_pub}
request:   {type: "request", request_id, path, body(b64), headers}
response:  {type: "response", request_id, status, body(b64)}
ping:      {type: "ping"}  ->  {type: "pong"}
health:    {type: "health", report}
```

## Install

```bash
pip install "analytics-tunnels @ git+https://github.com/Semantic-Automation/analytics-tunnels.git@v0.1.0"
```

## Test

```bash
python -m venv .testvenv && .testvenv/bin/pip install -e . pytest
.testvenv/bin/python -m pytest tests/ -q
```
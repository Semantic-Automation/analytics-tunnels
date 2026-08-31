"""TunnelClient — the dial-out side of a reverse WebSocket tunnel.

Connects to a WS endpoint (e.g. ``wss://host/tunnel/connect``), registers with
an id + public keys, keeps the socket alive through proxy pings + local
heartbeats, and dispatches incoming ``request`` messages to an injected
handler. Reconnects with exponential backoff. Transport only — no crypto.

Wire messages (JSON over WebSocket):

    register:  {spoke_id, kem_pub, x_pub, sign_pub}
    request:   {type: "request", request_id, path, body(b64), headers}
    response:  {type: "response", request_id, status, body(b64)}
    ping:      {type: "ping"}  ->  {type: "pong"}
    health:    {type: "health", report}
"""

import base64
import json
import os
import ssl
import threading
import time

import websocket  # websocket-client


class TunnelClient:
    """Persistent dial-out reverse tunnel.

    ``handler`` is called ``handler(request_id, path, body, headers) -> (status, body)``
    for each request. ``health_fn`` is called periodically to produce a health
    report pushed to the acceptor. ``public_keys`` is a dict with
    ``kem_pub``/``x_pub``/``sign_pub`` (base64) sent at registration.
    """

    def __init__(
        self,
        proxy_url: str,
        tunnel_id: str,
        public_keys: dict | None = None,
        handler=None,
        health_fn=None,
    ):
        self._proxy_url = proxy_url.rstrip("/")
        self._tunnel_id = tunnel_id
        self._public_keys = public_keys or {}
        self._handler = handler
        self._health_fn = health_fn
        self._ws = None
        self._running = False
        self._reconnect_delay = 1.0
        self._max_reconnect_delay = 60.0

    def connect(self) -> None:
        """Connect to the acceptor and start listening (blocks; reconnects)."""
        self._running = True
        while self._running:
            try:
                self._connect_and_listen()
            except Exception as e:  # noqa: BLE001 - transport loop must survive
                print(f"[tunnel] connection error: {e}", flush=True)

            if self._running:
                print(f"[tunnel] reconnecting in {self._reconnect_delay:.1f}s...", flush=True)
                time.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, self._max_reconnect_delay)

    def _connect_and_listen(self) -> None:
        ws_url = self._proxy_url.replace("https://", "wss://").replace("http://", "ws://")
        ws_url = f"{ws_url}/tunnel/connect"

        print(f"[tunnel] connecting to {ws_url}", flush=True)

        ssl_context = ssl.create_default_context() if ws_url.startswith("wss://") else None

        self._ws = websocket.WebSocket()
        self._ws.connect(ws_url, ssl=ssl_context)

        reg_msg = {"spoke_id": self._tunnel_id, **self._public_keys}
        self._ws.send(json.dumps(reg_msg))

        response = json.loads(self._ws.recv())
        if "error" in response:
            print(f"[tunnel] registration rejected: {response['error']}", flush=True)
            self._ws.close()
            return

        print(f"[tunnel] registered: {response}", flush=True)
        self._reconnect_delay = 1.0

        if self._health_fn:
            threading.Thread(target=self._health_loop, daemon=True).start()

        self._ws.settimeout(30.0)  # Heartbeat timeout
        while self._running:
            try:
                data = self._ws.recv()
                if not data:
                    continue
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                msg = json.loads(data)
                msg_type = msg.get("type", "")

                if msg_type == "request":
                    # Handle in a background thread so the listen loop keeps
                    # calling recv()/ping() during long processing (the
                    # transport drops idle sockets).
                    threading.Thread(
                        target=self._handle_request,
                        args=(msg,),
                        daemon=True,
                    ).start()
                elif msg_type == "ping":
                    try:
                        self._ws.send(json.dumps({"type": "pong"}))
                    except Exception:  # noqa: BLE001
                        pass

            except websocket.WebSocketTimeoutException:
                try:
                    self._ws.ping()
                except Exception:  # noqa: BLE001
                    break

            except websocket.WebSocketConnectionClosedException:
                print("[tunnel] connection closed by acceptor", flush=True)
                break

            except Exception as e:  # noqa: BLE001
                print(f"[tunnel] receive error: {e}", flush=True)
                break

    def _handle_request(self, msg: dict) -> None:
        request_id = msg.get("request_id", "")
        path = msg.get("path", "/completion")
        body_b64 = msg.get("body", "")
        headers = msg.get("headers", {})

        try:
            body = base64.b64decode(body_b64)
            if self._handler is not None:
                status, response_body = self._handler(request_id, path, body, headers)
            else:
                status, response_body = 501, json.dumps({"error": "no handler"}).encode()
        except Exception as e:  # noqa: BLE001 - never let one request kill the loop
            status = 500
            response_body = json.dumps({"error": str(e)}).encode()

        response_msg = {
            "type": "response",
            "request_id": request_id,
            "status": status,
            "body": base64.b64encode(response_body).decode("ascii"),
        }
        try:
            self._ws.send(json.dumps(response_msg))
            print(f"[tunnel] {request_id} response sent (status={status})", flush=True)
        except Exception as e:  # noqa: BLE001
            print(f"[tunnel] {request_id} response send FAILED: {e}", flush=True)

    def _health_loop(self) -> None:
        while self._running:
            try:
                if self._ws and self._health_fn:
                    self.send_health(self._health_fn())
            except Exception as e:  # noqa: BLE001
                print(f"[tunnel] health report error: {e}", flush=True)
            time.sleep(5)

    def send_health(self, report: dict) -> None:
        if self._ws:
            try:
                msg = {"type": "health", "report": report}
                self._ws.send(json.dumps(msg))
            except Exception:  # noqa: BLE001
                pass

    def disconnect(self) -> None:
        self._running = False
        if self._ws:
            try:
                self._ws.close()
            except Exception:  # noqa: BLE001
                pass
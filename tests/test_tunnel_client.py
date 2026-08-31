"""TunnelClient transport tests (handler dispatch, wire framing)."""

import json

from tunnelkit.tunnel_client import TunnelClient


class _FakeWS:
    def __init__(self):
        self.sent = []

    def send(self, data):
        self.sent.append(data)

    def close(self):
        pass


def _client(**kw):
    c = TunnelClient("https://x", "peer-1", public_keys={"sign_pub": "abc"}, **kw)
    c._ws = _FakeWS()
    return c


def test_handle_request_dispatches_to_handler():
    calls = []

    def handler(request_id, path, body, headers):
        calls.append((request_id, path, body, headers))
        return 200, b"response-bytes"

    c = _client(handler=handler)
    c._handle_request({
        "type": "request", "request_id": "r1", "path": "/completion",
        "body": "aGk=",  # b"hi"
        "headers": {"Content-Type": "application/octet-stream"},
    })
    assert calls == [("r1", "/completion", b"hi", {"Content-Type": "application/octet-stream"})]
    msg = json.loads(c._ws.sent[-1])
    assert msg == {
        "type": "response", "request_id": "r1", "status": 200,
        "body": "cmVzcG9uc2UtYnl0ZXM=",  # b"response-bytes"
    }


def test_handle_request_without_handler_501():
    c = _client(handler=None)
    c._handle_request({"type": "request", "request_id": "r2", "path": "/x", "body": "aGk=", "headers": {}})
    msg = json.loads(c._ws.sent[-1])
    assert msg["status"] == 501


def test_handle_request_handler_error_500():
    def handler(*a):
        raise RuntimeError("boom")

    c = _client(handler=handler)
    c._handle_request({"type": "request", "request_id": "r3", "path": "/x", "body": "aGk=", "headers": {}})
    msg = json.loads(c._ws.sent[-1])
    assert msg["status"] == 500
    import base64

    assert "boom" in json.loads(base64.b64decode(msg["body"]))["error"]


def test_registration_message_shape():
    c = _client()
    reg = {"spoke_id": "peer-1", **c._public_keys}
    assert reg == {"spoke_id": "peer-1", "sign_pub": "abc"}
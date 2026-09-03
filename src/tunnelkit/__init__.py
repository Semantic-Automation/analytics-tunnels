"""tunnelkit — symmetric WebSocket tunnels.

Both ends of a tunnel share the same API:
- ``request(path, body, headers)`` — send a request, await the response
- ``on_request(handler)`` — handle incoming requests
- ``close()`` — disconnect

``Client`` dials out and reconnects on failure.
``Host`` accepts connections and verifies peers via injected ``Auth``.
"""

from .auth import Auth, NoAuth, StaticAuth
from .client import Client
from .host import Host, HostTunnel
from .tunnel import Tunnel, TunnelClosed, TunnelError, TunnelTimeout

__all__ = [
    "Tunnel",
    "Client",
    "Host",
    "HostTunnel",
    "Auth",
    "NoAuth",
    "StaticAuth",
    "TunnelError",
    "TunnelTimeout",
    "TunnelClosed",
]

"""tunnelkit — symmetric WebSocket tunnels.

Both ends of a tunnel share the same API:
- ``request(path, body, headers)`` — send a request, await the response
- ``on_request(handler)`` — handle incoming requests
- ``close()`` — disconnect

``TunnelRegistry`` is the role-agnostic node layer: ``get_tunnel``,
``disconnect_tunnel``, ``list_tunnels`` and ``close`` work the same whether the
node is a ``Client`` (dial-out) or a ``Host`` (accept). The only difference
between the two is who establishes the connection and who exposes a public
``wss://`` address.
"""

from .auth import Auth, NoAuth, StaticAuth
from .client import Client, ClientTunnel, RegistrationRejected
from .host import Host, HostTunnel
from .registry import TunnelRegistry
from .tunnel import Tunnel, TunnelClosed, TunnelError, TunnelTimeout

__all__ = [
    "Tunnel",
    "TunnelRegistry",
    "Client",
    "ClientTunnel",
    "Host",
    "HostTunnel",
    "Auth",
    "NoAuth",
    "StaticAuth",
    "RegistrationRejected",
    "TunnelError",
    "TunnelTimeout",
    "TunnelClosed",
]
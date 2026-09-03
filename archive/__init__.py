"""tunnelkit — transport-only reverse-tunnel machinery.

Shared by the spoke, the builder, and the edge proxy. The package moves bytes
and frames only; it performs no crypto and holds no keys:

- :class:`TunnelClient` — dial-out side. Registers with an id + public keys,
  keeps the socket alive, and dispatches incoming requests to an injected
  ``handler(request_id, path, body, headers) -> (status, body)``.
- :class:`TunnelManager` / :class:`TunnelConnection` — accept side. Verifies
  connecting peers via an injected ``verifier(...)``, holds their sockets, and
  sends requests through them.

"Transport is not the security boundary": anything security-related (envelope
crypto, signature verification, role gates) lives in the consumer's
handler/verifier.
"""

from .tunnel_client import TunnelClient
from .tunnel import TunnelConnection, TunnelManager

__all__ = ["TunnelClient", "TunnelConnection", "TunnelManager"]
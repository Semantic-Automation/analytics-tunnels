"""Authentication for tunnel peers.

The tunnel itself performs no crypto. Consumers inject an ``Auth``
implementation that decides whether a connecting peer is trusted.
"""

from typing import Protocol


class Auth(Protocol):
    """Decides whether a connecting peer may register."""

    def verify_peer(self, tunnel_id: str, **metadata) -> bool:
        """Return True if the peer is allowed to connect."""
        ...


class NoAuth:
    """Accepts all peers (dev mode)."""

    def verify_peer(self, tunnel_id: str, **metadata) -> bool:
        return True


class StaticAuth:
    """Verifies peers against a pre-configured allowlist.

    ``allowed`` maps tunnel_id -> metadata dict. A peer is accepted if
    its tunnel_id is present and its metadata matches exactly.
    """

    def __init__(self, allowed: dict[str, dict]):
        self._allowed = allowed

    def verify_peer(self, tunnel_id: str, **metadata) -> bool:
        expected = self._allowed.get(tunnel_id)
        if expected is None:
            return False
        return metadata == expected

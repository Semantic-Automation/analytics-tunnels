"""Auth tests."""

from tunnelkit.auth import NoAuth, StaticAuth


def test_no_auth_accepts_all():
    auth = NoAuth()
    assert auth.verify_peer("any-id", key="value") is True
    assert auth.verify_peer("other") is True


def test_static_auth_accepts_matching():
    auth = StaticAuth(allowed={"spoke-1": {"sign_pub": "abc"}})
    assert auth.verify_peer("spoke-1", sign_pub="abc") is True


def test_static_auth_rejects_unknown_id():
    auth = StaticAuth(allowed={"spoke-1": {"sign_pub": "abc"}})
    assert auth.verify_peer("spoke-2", sign_pub="abc") is False


def test_static_auth_rejects_mismatched_metadata():
    auth = StaticAuth(allowed={"spoke-1": {"sign_pub": "abc"}})
    assert auth.verify_peer("spoke-1", sign_pub="xyz") is False

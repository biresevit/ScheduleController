import pytest


@pytest.fixture(autouse=True)
def no_saved_logins(monkeypatch):
    """Saved BluffPoker logins are off in these tests (they never touch C:\PokerSuite)."""
    from poker_controller import token_store
    monkeypatch.setattr(token_store, "DIRECTORY", None)

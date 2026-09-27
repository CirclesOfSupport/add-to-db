"""TARGET_TABLES (staging deployments) may only point known targets at DEV tables."""
import importlib
import sys

import pytest


def _load(monkeypatch, value):
    monkeypatch.setenv("TARGET_TABLES", value)
    sys.modules.pop("config", None)
    try:
        return importlib.import_module("config")
    finally:
        monkeypatch.delenv("TARGET_TABLES")
        sys.modules.pop("config", None)
        importlib.import_module("config")


def test_points_targets_at_dev_tables(monkeypatch):
    c = _load(monkeypatch, "responses=early-alert-responses.DEV.adb_stg_response_data;users=early-alert-responses.DEV.adb_stg_users")
    assert c.ALLOWED_TARGETS["responses"].endswith("DEV.adb_stg_response_data")
    assert c.ALLOWED_TARGETS["users"].endswith("DEV.adb_stg_users")
    assert c.ALLOWED_TARGETS["triage_data"].endswith("RESPONSES.triage-message-data")


@pytest.mark.parametrize("value", ["responses=early-alert-responses.RESPONSES.response_data", "nope=early-alert-responses.DEV.x"])
def test_refuses_non_dev_or_unknown(monkeypatch, value):
    with pytest.raises(RuntimeError):
        _load(monkeypatch, value)


def test_unset_changes_nothing():
    import config
    assert config.ALLOWED_TARGETS["responses"] == "early-alert-responses.RESPONSES.response_data"

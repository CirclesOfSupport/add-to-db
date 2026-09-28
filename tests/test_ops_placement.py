"""The single-writer tables live in OPS in production (DEV for staging and proofs), never in RESPONSES."""
import importlib
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))
import flush_pause  # noqa: E402
import staged_ddl  # noqa: E402

NAMES = ("STAGING_TABLE", "DEAD_LETTER_TABLE", "FLUSH_LOG_TABLE", "FLUSH_STATE_TABLE")


def _load(monkeypatch, env):
    """A fresh import of config under env; the module every other test and src/main.py hold is put back."""
    original = sys.modules.get("config") or importlib.import_module("config")
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    sys.modules.pop("config", None)
    try:
        return importlib.import_module("config")
    finally:
        for k in env:
            monkeypatch.delenv(k)
        sys.modules["config"] = original


def test_defaults_are_ops_with_no_env(monkeypatch):
    for n in NAMES:
        monkeypatch.delenv(n, raising=False)
    c = _load(monkeypatch, {})
    assert (c.STAGING_TABLE, c.DEAD_LETTER_TABLE, c.FLUSH_LOG_TABLE, c.FLUSH_STATE_TABLE) == (
        "early-alert-responses.OPS.adb_staging", "early-alert-responses.OPS.adb_dead_letter",
        "early-alert-responses.OPS.adb_flush_log", "early-alert-responses.OPS.adb_flush_state")


@pytest.mark.parametrize("name", NAMES)
@pytest.mark.parametrize("value", ["early-alert-responses.RESPONSES.adb_x", "early-alert-responses.COPY.adb_x",
                                   "other-project.OPS.adb_x", "adb_x"])
def test_override_outside_ops_or_dev_is_refused(monkeypatch, name, value):
    with pytest.raises(RuntimeError, match=name):
        _load(monkeypatch, {name: value})


@pytest.mark.parametrize("value", ["early-alert-responses.DEV.adb_x", "early-alert-responses.OPS.adb_x"])
def test_override_to_dev_or_ops_is_accepted(monkeypatch, value):
    c = _load(monkeypatch, {n: value for n in NAMES})
    assert {c.STAGING_TABLE, c.DEAD_LETTER_TABLE, c.FLUSH_LOG_TABLE, c.FLUSH_STATE_TABLE} == {value}


def test_staged_ddl_targets_ops_and_dev_only():
    assert all("`early-alert-responses.OPS.adb_" in s for s in staged_ddl.ddl("OPS"))
    assert all("`early-alert-responses.DEV.adb_" in s for s in staged_ddl.ddl("DEV"))
    for call in (lambda: staged_ddl.ddl("RESPONSES"), lambda: staged_ddl.verify(None, "RESPONSES"),
                 lambda: staged_ddl.apply(None, "RESPONSES")):
        with pytest.raises(ValueError, match="refused"):
            call()


def test_staged_ddl_cli_defaults_to_ops_and_refuses_responses(monkeypatch, capsys):
    import runpy
    path = os.path.join(os.path.dirname(__file__), "..", "tools", "staged_ddl.py")
    monkeypatch.setattr(sys, "argv", ["staged_ddl.py"])
    runpy.run_path(path, run_name="__main__")
    out = capsys.readouterr().out
    assert "`early-alert-responses.OPS.adb_staging`" in out and "RESPONSES" not in out
    monkeypatch.setattr(sys, "argv", ["staged_ddl.py", "--verify", "RESPONSES"])
    with pytest.raises(SystemExit) as exc:
        runpy.run_path(path, run_name="__main__")
    assert exc.value.code == 2 and "refused" in capsys.readouterr().out


def test_flush_pause_defaults_to_ops_and_refuses_responses(capsys):
    seen = []

    class _Stop(Exception):
        pass

    class _Client:
        def query(self, sql, job_config=None):
            seen.append(sql)
            raise _Stop()

    with pytest.raises(_Stop):
        flush_pause.main_(["status"], client=_Client())
    assert "`early-alert-responses.OPS.adb_flush_state`" in seen[0]
    with pytest.raises(SystemExit) as exc:
        flush_pause.main_(["status", "--dataset", "RESPONSES"], client=_Client())
    assert exc.value.code == 2 and "invalid choice" in capsys.readouterr().err

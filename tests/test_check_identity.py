"""The identity gate's source part: it passes the triage-queue additions and fails anything else."""
from __future__ import annotations

import os
import subprocess
import sys

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, "..", "tools"))

import check_identity  # noqa: E402

BASE = "x = 1\n\ndef stage_calls(a):\n    return a\n\ndef enqueue_write(p):\n    return p\n"


def test_allowed_change_passes():
    branch = BASE.replace("return p", "return p + 1") + "\ndef queue_for(t):\n    return t\n"
    r = check_identity.compare_source(BASE, branch, {"enqueue_write", "queue_for"})
    assert r["bad"] == [] and r["changed"] == ["enqueue_write"] and r["added"] == ["queue_for"]


def test_change_to_the_staged_path_fails():
    branch = BASE.replace("return a", "return a * 2")
    r = check_identity.compare_source(BASE, branch, {"enqueue_write", "queue_for"})
    assert r["bad"] == ["stage_calls"]


def test_removal_and_reorder_fail():
    r = check_identity.compare_source(BASE, "x = 1\n", {"enqueue_write"})
    assert "stage_calls" in r["bad"]
    reordered = "def stage_calls(a):\n    return a\n\nx = 1\n\ndef enqueue_write(p):\n    return p\n"
    assert "(statement order)" in check_identity.compare_source(BASE, reordered, set())["bad"]


def test_this_branch_passes_against_the_cutover_revision():
    subprocess.run(["git", "-C", os.path.join(HERE, ".."), "cat-file", "-e", check_identity.BASE], check=True)
    assert check_identity.source_identity(check_identity.BASE)

"""Every name the worker code uses is defined (2026-11-11: a missing import stopped the PC worker at its first
lobby scan and the release rolled back). Static, so it covers paths the other tests never run."""
from pathlib import Path

import pytest

pyflakes_api = pytest.importorskip("pyflakes.api")
from pyflakes.reporter import Reporter


def test_no_undefined_names(tmp_path):
    root = Path(__file__).resolve().parents[1]
    packages = [root / "schedule_controller", root.parent / "PokerController" / "poker_controller"]
    out = (tmp_path / "out.txt").open("w+", encoding="utf-8")
    for package in packages:
        if package.exists():
            pyflakes_api.checkRecursive([str(package)], Reporter(out, out))
    out.seek(0)
    undefined = [line for line in out.read().splitlines() if "undefined name" in line]
    assert not undefined, undefined

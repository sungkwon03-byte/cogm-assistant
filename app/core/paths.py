from app.lib.name_resolver import resolve_names
from __future__ import annotations
from pathlib import Path

ROOT = Path(".")
OUT = ROOT / "output"
SUM = OUT / "summaries"
REP = OUT / "reports"

def summary(path: str) -> Path:
    return SUM / path

def report(path: str) -> Path:
    return REP / path

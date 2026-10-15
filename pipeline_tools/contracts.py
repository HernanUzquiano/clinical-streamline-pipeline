"""Loading and validating curated-topic data contracts (contracts/*.json)."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from jsonschema import Draft202012Validator

CONTRACTS_DIR = Path(__file__).resolve().parent.parent / "contracts"


@lru_cache
def load(topic: str) -> dict:
    return json.loads((CONTRACTS_DIR / f"{topic}.json").read_text())


def all_topics() -> list[str]:
    return sorted(p.stem for p in CONTRACTS_DIR.glob("*.json"))


def fields(topic: str) -> list[str]:
    return list(load(topic)["properties"])


def key_fields(topic: str) -> list[str]:
    return load(topic)["x-key"]


def validate(topic: str, record: dict) -> list[str]:
    """Return a list of human-readable violations (empty when the record honours the contract)."""
    validator = Draft202012Validator(load(topic))
    return [f"{'/'.join(map(str, e.path)) or '<root>'}: {e.message}" for e in validator.iter_errors(record)]

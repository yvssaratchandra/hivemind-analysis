"""Stable, independently derived pseudo-random streams."""

from __future__ import annotations

import hashlib
import json
import random


def rng(
    seed: int | str, subsystem: str, identity: str = "", sequence: int = 0
) -> random.Random:
    """Return a repeatable stream isolated by subsystem and identity.

    The JSON encoding makes component boundaries unambiguous; SHA-256 avoids
    Python's process-randomized ``hash()`` and creates the integer seed accepted
    by ``random.Random``.
    """
    if not isinstance(subsystem, str) or not subsystem:
        raise ValueError("subsystem must be a nonempty string")
    if not isinstance(identity, str):
        raise TypeError("identity must be a string")
    if isinstance(sequence, bool) or not isinstance(sequence, int):
        raise TypeError("sequence must be an integer")

    if isinstance(seed, bool) or not isinstance(seed, (int, str)):
        raise TypeError("seed must be an integer or string")

    material = {
        "seed": seed,
        "subsystem": subsystem,
        "identity": identity,
        "sequence": sequence,
    }
    try:
        encoded = json.dumps(
            material,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise TypeError("seed must be a JSON scalar with a finite value") from exc
    digest = hashlib.sha256(encoded).digest()
    return random.Random(int.from_bytes(digest, byteorder="big", signed=False))

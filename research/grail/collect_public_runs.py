#!/usr/bin/env python3
"""Compatibility entrypoint used by research/public-networks/collect_public_runs.py."""

from collect_public_data import main

if __name__ == "__main__":
    raise SystemExit(main())

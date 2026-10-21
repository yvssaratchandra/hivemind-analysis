"""Native evidence gates for the dominant-compute simulator extension.

Numerical replay alone never enables a timing sweep. The timing model must be
calibrated on the pilot and validated on separate confirmation seeds.
"""
from __future__ import annotations
import json
from pathlib import Path

from announcement_artifacts import verify


def reference_chain(reference):
    """Require the complete C0 -> C1 -> C2 evidence chain before C3."""
    current = Path(reference).resolve()
    chain = {}
    for expected in ('confirmation', 'pilot', 'markers'):
        if verify(current):
            raise ValueError(f'Inventory mismatch: {current}')
        status = json.loads((current / 'status.json').read_text())
        if status.get('mode') != expected or status.get('status') != 'complete':
            raise ValueError(f'C3 requires a complete {expected} suite')
        chain[expected] = current
        if expected != 'markers':
            if not status.get('reference'):
                raise ValueError('Native reference chain is incomplete')
            current = Path(status['reference'])
    return chain


def run_stage(args):
    if args.reference is None:
        raise ValueError('A native confirmation reference or validated simulation artifact is required')
    if args.command == 'validate':
        chain = reference_chain(args.reference)
        # Implemented after native pilot behavior and confirmation are available.
        # This explicit gate cannot be bypassed by a numerical replay result.
        raise ValueError('C3 timing calibration has not been completed; no timing sweep is authorized by numerical replay alone')
    raise ValueError('Simulation requires a passing native behavioral and held-out timing validation artifact')

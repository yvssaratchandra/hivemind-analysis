"""Run the frozen Step28 experiment with its separately versioned export checker.

The two completed smoke traces and original experiment modules are unchanged.
Only checker schema agreement is corrected; runtime policies remain frozen.
"""
from pathlib import Path
import sys

import step28_confirmation as experiment
import verify_step28_exports_v2 as verification

ORIGINAL_FILES = experiment.FROZEN_FILES
EXTRA_FILES = (Path(__file__).name, 'verify_step28_exports_v2.py', 'test_step28_exports_v2.py')
ORIGINAL_VALIDATE = experiment.validate_sources
ORIGINAL_SNAPSHOT = experiment.snapshot


def validate_sources(suite, provenance):
    current = experiment.FROZEN_FILES
    experiment.FROZEN_FILES = (ORIGINAL_FILES + EXTRA_FILES
                              if provenance.get('verification_policy_version') == 2
                              else ORIGINAL_FILES)
    try:
        return ORIGINAL_VALIDATE(suite, provenance)
    finally:
        experiment.FROZEN_FILES = current


def snapshot(suite):
    ORIGINAL_SNAPSHOT(suite)
    provenance = experiment.read_json(Path(suite) / 'provenance.json')
    provenance.update(verification_policy_version=2,
                      entrypoint=str(Path(__file__).resolve()),
                      entrypoint_sha256=experiment.sha256(__file__),
                      verification_policy='Clock valid-sample schema and scalar join-ID indexing corrected; native workload unchanged')
    experiment.atomic_json(Path(suite) / 'provenance.json', provenance)


experiment.FROZEN_FILES = ORIGINAL_FILES + EXTRA_FILES
experiment.validate_sources = validate_sources
experiment.snapshot = snapshot
sys.modules['verify_step28_confirmation'] = verification


if __name__ == '__main__':
    experiment.main()

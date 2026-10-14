"""Frozen Step30 workload with a separately versioned raw-event verifier.

The original rejected smoke and all original runtime sources are preserved.
This entrypoint changes verification only; trials use the original policies.
"""
from pathlib import Path
import shutil
import sys

import step30_harm as experiment
import verify_step30_harm_v2 as verification

ROOT = Path(__file__).resolve().parent
EXTRA_FILES = (Path(__file__).name, 'verify_step30_harm_v2.py', 'test_step30_live_v2.py')
ORIGINAL_SNAPSHOT = experiment.snapshot
ORIGINAL_VALIDATE = experiment.validate_sources


def snapshot(suite):
    ORIGINAL_SNAPSHOT(suite)
    provenance = experiment.read_json(suite / 'provenance.json')
    for name in EXTRA_FILES:
        shutil.copyfile(ROOT / name, suite / 'harness' / name)
        provenance['harness_sha256'][name] = experiment.sha256(ROOT / name)
    provenance.update(verification_policy_version=2,
                      controller_entrypoint=str(Path(__file__).resolve()),
                      controller_entrypoint_sha256=experiment.sha256(__file__),
                      verification_policy='Independent raw-event schema corrections; native workload unchanged')
    experiment.atomic_json(suite / 'provenance.json', provenance)


def validate_sources(suite):
    ORIGINAL_VALIDATE(suite)
    suite = Path(suite)
    provenance = experiment.read_json(suite / 'provenance.json')
    if provenance.get('verification_policy_version') != 2:
        raise ValueError('use original entrypoint to inspect the preserved original smoke')
    for name in EXTRA_FILES:
        digest = provenance['harness_sha256'].get(name)
        if digest != experiment.sha256(ROOT / name) or digest != experiment.sha256(suite / 'harness' / name):
            raise ValueError(f'versioned verification source changed: {name}')
    if provenance.get('controller_entrypoint_sha256') != experiment.sha256(__file__):
        raise ValueError('versioned controller changed after freeze')


experiment.snapshot = snapshot
experiment.validate_sources = validate_sources
sys.modules['verify_step30_harm'] = verification


if __name__ == '__main__':
    experiment.main()

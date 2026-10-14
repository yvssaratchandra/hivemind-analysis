"""Frozen Step30 workload with separately versioned analyzer edge-case checks.

Prior smoke evidence and runtime source remain unchanged. This revision fixes
analysis of native zero-response join streams and repeated deadline clamps.
"""
from pathlib import Path
import shutil
import sys

import run_step30_harm_v2 as previous
import analyze_step30_harm_v2 as analysis

experiment = previous.experiment
verification = previous.verification
ROOT = Path(__file__).resolve().parent
EXTRA_FILES = (Path(__file__).name, 'analyze_step30_harm_v2.py', 'test_step30_analyzer_live_v2.py')
ORIGINAL_SNAPSHOT = experiment.snapshot
ORIGINAL_VALIDATE = experiment.validate_sources
ORIGINAL_VERIFY_EXPORTS = verification.verify_exports


def snapshot(suite):
    ORIGINAL_SNAPSHOT(suite)
    provenance = experiment.read_json(suite / 'provenance.json')
    for name in EXTRA_FILES:
        shutil.copyfile(ROOT / name, suite / 'harness' / name)
        provenance['harness_sha256'][name] = experiment.sha256(ROOT / name)
    provenance.update(analysis_policy_version=2,
                      analysis_controller_entrypoint=str(Path(__file__).resolve()),
                      analysis_controller_sha256=experiment.sha256(__file__))
    experiment.atomic_json(suite / 'provenance.json', provenance)


def validate_sources(suite):
    ORIGINAL_VALIDATE(suite)
    suite = Path(suite)
    provenance = experiment.read_json(suite / 'provenance.json')
    if provenance.get('analysis_policy_version') != 2:
        raise ValueError('use original versioned entrypoint to inspect prior preserved smoke')
    for name in EXTRA_FILES:
        digest = provenance['harness_sha256'].get(name)
        if digest != experiment.sha256(ROOT / name) or digest != experiment.sha256(suite / 'harness' / name):
            raise ValueError(f'versioned analysis source changed: {name}')
    if provenance.get('analysis_controller_sha256') != experiment.sha256(__file__):
        raise ValueError('versioned analysis controller changed after freeze')


def verify_exports(suite):
    result = ORIGINAL_VERIFY_EXPORTS(suite)
    try:
        validate_sources(suite)
    except (OSError, ValueError, KeyError) as exc:
        result['errors'].append(f'analysis source closure: {exc}')
        result['pass'] = result['pass_'] = False
    return result


experiment.snapshot = snapshot
experiment.validate_sources = validate_sources
verification.verify_exports = verify_exports
sys.modules['analyze_step30_harm'] = analysis


if __name__ == '__main__':
    experiment.main()

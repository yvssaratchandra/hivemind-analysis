"""Frozen Step29 runtime with a separately versioned summary-row exporter fix.

The completed first smoke's raw evidence, provenance, and runtime sources are
unchanged. Only duplicate export metadata merging is corrected.
"""
from pathlib import Path
import shutil

import step29_policies as experiment
import analyze_step29_policies as analysis

ROOT = Path(__file__).resolve().parent
EXTRA_FILES = (Path(__file__).name, 'test_step29_exports_v2.py')
ORIGINAL_SNAPSHOT = experiment.snapshot
ORIGINAL_VALIDATE = experiment.validate_sources


def summary_trial_row(trial, phase):
    case = trial.get('case', {})
    row = {key: value for key, value in trial.get('summary_metrics', {}).items()
           if not isinstance(value, (dict, list))}
    row.update(trial_dir=trial.get('trial_dir'), phase=phase, case=case.get('name'),
               behavior=case.get('behavior'), publication_policy=case.get('publication_policy'),
               lifetime=case.get('lifetime'), seed=trial.get('seed'), duration=trial.get('duration'),
               integrity_pass=trial.get('integrity_pass'))
    return row


def validate_revision_files(provenance):
    for name, digest in provenance.get('export_revision_sha256', {}).items():
        if experiment.sha256(ROOT / name) != digest:
            raise ValueError(f'export revision source changed: {name}')


def snapshot(suite):
    ORIGINAL_SNAPSHOT(suite)
    provenance = experiment.read_json(suite / 'provenance.json')
    for name in EXTRA_FILES:
        shutil.copyfile(ROOT / name, suite / 'harness' / name)
        provenance['harness_sha256'][name] = experiment.sha256(ROOT / name)
    provenance.update(export_policy_version=2, entrypoint=str(Path(__file__).resolve()),
                      entrypoint_sha256=experiment.sha256(__file__))
    experiment.atomic_json(suite / 'provenance.json', provenance)


def validate_sources(suite):
    provenance = experiment.read_json(Path(suite) / 'provenance.json')
    if provenance.get('export_policy_version') == 2:
        if provenance['entrypoint_sha256'] != experiment.sha256(__file__):
            raise ValueError('versioned entrypoint changed since freeze')
        if not set(experiment.FROZEN_FILES + EXTRA_FILES).issubset(provenance['harness_sha256']):
            raise ValueError('missing versioned source closure')
        experiment.ancestor.validate_sources(suite, provenance)
        if provenance['script_sha256'] != experiment.sha256(experiment.__file__):
            raise ValueError('frozen runtime source changed')
    else:
        ORIGINAL_VALIDATE(suite)
        revision_path = Path(suite) / 'export_revision_v2.json'
        if not revision_path.is_file():
            raise ValueError('old suite requires separately preserved export revision marker')
        revision = experiment.read_json(revision_path)
        if revision['original_provenance_sha256'] != experiment.sha256(Path(suite) / 'provenance.json'):
            raise ValueError('original smoke provenance changed')
        validate_revision_files(revision)


analysis._summary_trial_row = summary_trial_row
experiment.snapshot = snapshot
experiment.validate_sources = validate_sources


if __name__ == '__main__':
    experiment.main()

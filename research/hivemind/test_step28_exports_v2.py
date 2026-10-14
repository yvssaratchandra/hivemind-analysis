"""Regression checks for the versioned checker against completed smoke exports."""
import json
from pathlib import Path
import unittest

import verify_step28_exports_v2 as verifier

SUITE = Path(__file__).resolve().parent / 'results/step28_smoke_20261012_a'


@unittest.skipUnless((SUITE / 'summary.json').exists(), 'completed smoke evidence unavailable')
class ExportRegressionTests(unittest.TestCase):
    def test_completed_smoke_validates_with_all_join_rows_checked(self):
        report = verifier.verify_suite(SUITE)
        self.assertTrue(report['pass'], report['errors'])
        self.assertEqual(report['totals']['total_groups'], 17)
        self.assertEqual(report['totals']['x_led_groups'], 7)
        self.assertFalse(json.loads((SUITE / 'raw_verification.json').read_text())['pass'])

    def test_join_expiration_corruption_is_detected(self):
        trial = SUITE / 'reliable_single_s2800'
        raw = verifier.raw_trial(trial)
        rows = verifier._csv_rows(SUITE / 'joins.csv')
        errors = []
        verifier._check_joins(trial.name, raw, rows, errors)
        self.assertEqual(errors, [])
        row = next(r for r in rows if r['trial_dir'] == trial.name and r['request_expiration'])
        row['request_expiration'] = str(float(row['request_expiration']) + 1.)
        verifier._check_joins(trial.name, raw, rows, errors)
        self.assertTrue(any('request_expiration differs' in e for e in errors), errors)


if __name__ == '__main__':
    unittest.main()

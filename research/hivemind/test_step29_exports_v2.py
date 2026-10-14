"""Real completed-smoke regression for duplicate Step29 summary metadata."""
from pathlib import Path
import shutil
import tempfile
import unittest

import run_step29_policies_v2 as revision

ROOT = Path(__file__).resolve().parent
SMOKE = ROOT / 'results' / 'step29_smoke_20261012_a'


class ExportTests(unittest.TestCase):
    def test_completed_smoke_export_retains_behavior_and_native_counts(self):
        trial = revision.experiment.read_json(SMOKE / 'normal_native_1s_s2900' / 'control_analysis.json')
        row = revision.summary_trial_row(trial, 'smoke')
        self.assertEqual(row['behavior'], 'normal')
        self.assertEqual(row['publication_policy'], 'native')
        self.assertEqual((row['x_led_attempts'], row['x_submitted_attempts']), (6, 6))
        self.assertEqual(row['case'], 'normal_native_1s')
        with tempfile.TemporaryDirectory() as folder:
            suite = Path(folder)
            shutil.copyfile(SMOKE / 'suite.json', suite / 'suite.json')
            path = suite / 'normal_native_1s_s2900'
            path.mkdir()
            shutil.copyfile(SMOKE / 'normal_native_1s_s2900' / 'control_analysis.json', path / 'control_analysis.json')
            result = revision.analysis.export_suite(suite)
            self.assertEqual(result['analyzed_trial_count'], 1)
            self.assertEqual(result['integrity_errors'], [])
            self.assertTrue((suite / 'REPORT.md').is_file())
            self.assertTrue((suite / 'leadership_summary.csv').is_file())

    def test_revision_preserves_scalar_metrics_and_authoritative_metadata(self):
        trial = dict(case=dict(name='normal_native_2s', behavior='normal', publication_policy='native', lifetime=2.),
                     trial_dir='normal_native_2s_s2901', seed=2901, duration=45., integrity_pass=True,
                     summary_metrics=dict(behavior='normal', publication_policy='native', x_led_attempts=10,
                                          nested={'ignored': True}))
        row = revision.summary_trial_row(trial, 'pilot')
        self.assertEqual(row['x_led_attempts'], 10)
        self.assertEqual(row['behavior'], 'normal')
        self.assertNotIn('nested', row)


if __name__ == '__main__':
    unittest.main()

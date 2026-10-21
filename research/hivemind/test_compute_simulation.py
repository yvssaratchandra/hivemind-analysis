"""Timing gates retain missing and censored evidence instead of imputing it."""
import unittest
import json
import tempfile
from pathlib import Path
from unittest.mock import patch
from compute_simulation import assess_timing, validate_stage, simulate_stage, predict


class TimingGateTests(unittest.TestCase):
    def row(self, seed=302, crossing=40, interval=4, censored=False):
        return dict(seed=seed, condition='baseline', crossing_time=crossing,
                    update_intervals={'D': interval, 'C': interval},
                    censored=censored, supported=True)

    def test_held_out_medians_and_each_seed_discrepancy_are_retained(self):
        native = [self.row(), self.row(303, 50, 5)]
        predicted = [self.row(crossing=44, interval=4.4), self.row(303, 55, 5.5)]
        result = assess_timing(native, predicted)
        self.assertTrue(result['valid'])
        self.assertEqual(len(result['cases']), 2)
        self.assertAlmostEqual(result['conditions']['baseline']['crossing_time']['relative_error'], .1)
        predicted[1]['crossing_time'] = 100
        self.assertFalse(assess_timing(native, predicted)['valid'])

    def test_censoring_is_not_imputed_and_mismatch_fails(self):
        native = [self.row(crossing=None, censored=True)]
        result = assess_timing(native, native)
        self.assertTrue(result['valid'])
        self.assertIsNone(result['conditions']['baseline']['crossing_time']['native_median'])
        self.assertFalse(assess_timing(native, [self.row()])['valid'])

    def test_missing_interval_unsupported_and_duplicate_cases_fail(self):
        native = [self.row()]
        for broken in ({**self.row(), 'supported': False},
                       {**self.row(), 'update_intervals': {'D': 4, 'C': None}}):
            self.assertFalse(assess_timing(native, [broken])['valid'])
        self.assertFalse(assess_timing(native, [])['valid'])
        with self.assertRaises(ValueError):
            assess_timing(native * 2, native)

    def test_native_recovery_cannot_pass_by_accidentally_matching_timing(self):
        native = [{**self.row(), 'unmodeled_native_paths': ['state_recovery']}]
        result = assess_timing(native, [self.row()])
        self.assertFalse(result['valid'])
        self.assertIn('native path outside candidate scope', result['errors'][0])

    def test_numerical_failure_prevents_calibration_and_timing_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            reference = root/'native'; reference.mkdir()
            (reference/'artifact_inventory.json').write_text('{}')
            destination = root/'validation'
            with patch('compute_simulation.reference_chain', return_value={}), \
                 patch('compute_simulation._freeze_model', return_value={}), \
                 patch('compute_simulation.model_sources', return_value={}), \
                 patch('compute_simulation.numerical_gate', return_value={'valid': False}), \
                 patch('compute_simulation.calibrate') as calibration, \
                 patch('compute_simulation.predict') as prediction:
                validate_stage(reference, destination)
            calibration.assert_not_called(); prediction.assert_not_called()
            status = json.loads((destination/'status.json').read_text())
            self.assertEqual(status['status'], 'gate_failed')
            self.assertFalse(status['valid'])
            with self.assertRaisesRegex(ValueError, 'requires passing'):
                simulate_stage(destination, root/'sweep')
            self.assertFalse((root/'sweep').exists())

    def test_prediction_adapter_preserves_absent_roster_and_readiness_intervals(self):
        import numpy as np
        parameters = dict(step_overhead_seconds={n: .001 for n in ('A', 'B', 'C', 'D')},
            interbatch_overhead_seconds={n: .001 for n in ('A', 'B', 'C', 'D')},
            gradient_latency_seconds=.02, state_latency_seconds=.01,
            progress_latency_seconds=0)
        def fake_run(config, policy, gradient, evaluate):
            self.assertEqual(config.members, ('B', 'D', 'C'))
            self.assertEqual(config.rates, {'B': 128, 'D': 128, 'C': 128})
            vector = gradient(config.seed, 'B', 0, np.zeros(48, dtype=np.float32))
            self.assertEqual(vector.shape, (48,))
            self.assertTrue(np.isfinite(vector).all())
            models = {n: np.zeros(48, dtype=np.float32) for n in config.members}
            return dict(evaluations=[evaluate(0, models), evaluate(300, models)], trace=[],
                update_begin_times={'D': [1, 3], 'C': [1, 3]},
                update_times={'D': [2, 5], 'C': [2, 5]},
                unsupported=False, unsupported_reasons=[])
        with tempfile.TemporaryDirectory() as tmp, \
             patch('trainsim.protocols.hivemind.compute.run_compute', side_effect=fake_run):
            result = predict(301, 'absent', .7, {'parameters': parameters}, Path(tmp)/'case')
            self.assertEqual(result['update_intervals'], {'D': 2, 'C': 2})
            self.assertTrue(result['censored'])
            self.assertTrue((Path(tmp)/'case/config.json').is_file())


if __name__ == '__main__':
    unittest.main()

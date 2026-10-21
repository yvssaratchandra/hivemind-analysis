"""Controller guards for persistent native experiments."""
import dataclasses
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch, Mock
sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_compute_exclusion as runner
from announcement_artifacts import inventory


class ControllerTests(unittest.TestCase):
    def test_shutdown_delay_preserves_completed_training_and_records_pending_worker(self):
        worker = Mock(name='worker'); worker.name = 'compute_A'; worker.exitcode = None
        evaluator = Mock(name='evaluator'); evaluator.name = 'compute_evaluator'; evaluator.exitcode = 0
        outcome = dict(status='completed', completed=True)
        runner.join_completed_workers([worker, evaluator], outcome)
        self.assertTrue(outcome['completed'])
        self.assertEqual(outcome['cleanup_status'], 'deadline_exceeded')
        self.assertEqual(outcome['cleanup_pending_nodes'], ['A'])
        evaluator.exitcode = None
        with self.assertRaises(RuntimeError):
            runner.join_completed_workers([evaluator], outcome)
        worker.exitcode = 1
        with self.assertRaises(RuntimeError):
            runner.join_completed_workers([worker], outcome)

    def test_incomplete_training_is_not_reported_as_cleanly_joined(self):
        worker = Mock(); worker.name = 'compute_A'
        outcome = dict(status='native_noncompletion', completed=False)
        runner.join_completed_workers([worker], outcome)
        worker.join.assert_not_called()
        self.assertEqual(outcome['cleanup_status'], 'training_incomplete')
        self.assertEqual(outcome['cleanup_skipped_nodes'], ['A'])

    def test_native_progress_serializes_bytes_and_nonfinite_eta(self):
        @dataclasses.dataclass
        class Progress:
            eta: float
            peer_id: bytes
        self.assertEqual(runner.plain(Progress(float('inf'), b'abc')),
                         {'eta': 'inf', 'peer_id': {'bytes_hex': '616263'}})
        json.dumps(runner.plain(Progress(float('inf'), b'abc')), allow_nan=False)

    def test_batch_metadata_node_preserves_event_identity(self):
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            emit = runner.event_writer(root, 'A', SimpleNamespace(value=0))
            emit('microbatch', node='A', batch_index=0)
            record = json.loads(next(root.glob('events_*.jsonl')).read_text())
            self.assertEqual(record['node'], 'A')
            with self.assertRaises(ValueError):
                emit('microbatch', node='B')

    def test_labels_cannot_escape_results(self):
        for value in ('../escape', '/tmp/escape', '', 'two words', 'x/y'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                runner.label_path(value)

    def test_sources_detect_change_after_freeze(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, dest = root / 'source', root / 'dest'
            source.mkdir(); dest.mkdir()
            (source / 'a.py').write_text('original')
            with patch.object(runner, 'HERE', source), patch.object(runner, 'SOURCES', ('a.py',)):
                runner.freeze(dest)
                runner.check_sources(dest)
                (source / 'a.py').write_text('changed')
                with self.assertRaisesRegex(ValueError, 'changed'):
                    runner.check_sources(dest)

    def test_complete_status_cannot_bypass_incomplete_case_matrix(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            runner.write(root / 'status.json', {'mode': 'markers', 'status': 'complete', 'cases': []})
            runner.write(root / 'artifact_inventory.json', inventory(root))
            with self.assertRaisesRegex(ValueError, 'matrix'):
                runner.check_reference(root, 'pilot')

    def test_marker_inputs_encode_actual_microbatch_origins(self):
        import numpy as np
        inputs = runner.marker_samples(11, 'A', 6)
        self.assertEqual(inputs.shape, (32, 48))
        np.testing.assert_array_equal(inputs[:, 2], np.ones(32))
        np.testing.assert_array_equal(inputs[:, 6], np.full(32, 7))
        np.testing.assert_array_equal(inputs, np.broadcast_to(inputs[0], inputs.shape))


class ReportTests(unittest.TestCase):
    def test_suspension_audit_blocks_otherwise_valid_suite(self):
        from report_compute_exclusion import check_timing_eligibility
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            audit = root/'compute_suspend_audit_test'; audit.mkdir()
            (audit/'report.json').write_text(json.dumps(dict(
                suite='/original/results/affected_suite', invalidated_cases=['exclusion_s302'])))
            with self.assertRaisesRegex(ValueError, 'suspension invalidated'):
                check_timing_eligibility(Path('/new/results/affected_suite'), root)
            check_timing_eligibility(Path('/new/results/other_suite'), root)

    def test_paired_summary_retains_censored_pairs(self):
        from report_compute_exclusion import paired_summary
        rows = [dict(seed=1, condition='baseline', censored=False, crossing_time=10),
                dict(seed=1, condition='exclusion', censored=False, crossing_time=14),
                dict(seed=2, condition='baseline', censored=False, crossing_time=11),
                dict(seed=2, condition='exclusion', censored=True, crossing_time=None)]
        result = paired_summary(rows)['exclusion']
        self.assertEqual(result['median_paired_difference_seconds'], 4)
        self.assertEqual(len(result['observed_pairs']), 1)
        self.assertEqual(result['censored_pairs'], [dict(seed=2, baseline=False, treatment=True)])


if __name__ == '__main__':
    unittest.main()

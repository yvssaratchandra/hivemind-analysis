"""Controller guards for persistent native experiments."""
import dataclasses
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_compute_exclusion as runner
from announcement_artifacts import inventory


class ControllerTests(unittest.TestCase):
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

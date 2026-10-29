import importlib.util
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location('agora_collect', Path(__file__).with_name('collect_public_runs.py'))
collector = importlib.util.module_from_spec(spec)
spec.loader.exec_module(collector)


class TelemetryTests(unittest.TestCase):
    def test_resolved_retry_is_not_an_unavailable_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            body = json.dumps({'runs': []}).encode()
            (root / 'registry.json').write_bytes(body)
            url = collector.BASE + '/api/runs'
            entries = [{'url': url, 'status': 503, 'bytes': 0},
                       {'url': url, 'status': 200, 'path': 'registry.json',
                        'bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest()}]
            (root / 'manifest.json').write_text(json.dumps({'entries': entries}))
            summary = collector.analyze(root)
            self.assertEqual(summary['failed_sources'], [])
            self.assertEqual(summary['manifest_records'], 2)
            self.assertEqual(summary['distinct_source_urls'], 1)

    def test_null_is_not_zero_and_worker_identity_is_preserved(self):
        rows = list(collector.matrix_rows('run', 'count', {
            'timestamps': [10, 20], 'workers': ['MixedCase-01', 'Other'],
            'values': [[None, 0], [3, None]]}, 'raw/file'))
        self.assertEqual([(r['worker'], r['timestamp'], r['value']) for r in rows],
                         [('MixedCase-01', 20, 0), ('Other', 10, 3)])
        self.assertTrue(all(r['source_file'] == 'raw/file' for r in rows))

    def test_misaligned_matrix_is_rejected(self):
        with self.assertRaises(ValueError):
            list(collector.matrix_rows('run', 'count', {
                'timestamps': [10, 20], 'workers': ['A'], 'values': [[1]]}, 'raw/file'))


if __name__ == '__main__':
    unittest.main()

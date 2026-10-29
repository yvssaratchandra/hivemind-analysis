import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import collect_public_runs
from collect_public_runs import EXTENDED_PROJECTS, PROJECTS
from verify_extended import inspect_batch


class ExtendedVerificationTests(unittest.TestCase):
    def test_nested_manifests_count_bytes_once_and_detect_corruption(self):
        with tempfile.TemporaryDirectory() as temp:
            research = Path(temp)
            expected = 0
            for project in EXTENDED_PROJECTS:
                root = research / project / 'data' / 'test'
                child = root / 'browser'
                child.mkdir(parents=True)
                body = child / 'page.html'
                body.write_bytes(b'archived')
                entry = {'path': 'page.html', 'bytes': 8, 'sha256': hashlib.sha256(b'archived').hexdigest(), 'status': 200}
                (child / 'manifest.json').write_text(json.dumps({'entries': [entry]}))
                (root / 'manifest.json').write_text(json.dumps({'entries': []}))
                expected += sum(p.stat().st_size for p in root.rglob('*') if p.is_file())
            result = inspect_batch(research, 'test')
            self.assertTrue(result['passed'])
            self.assertEqual(expected, result['total_bytes'])
            bad_gzip = body.parent / 'broken.jsonl.gz'
            bad_gzip.write_bytes(b'not gzip')
            self.assertFalse(inspect_batch(research, 'test')['passed'])
            bad_gzip.unlink()
            body.write_bytes(b'corrupted')
            self.assertFalse(inspect_batch(research, 'test')['passed'])

    def test_missing_projects_fail(self):
        with tempfile.TemporaryDirectory() as temp:
            self.assertFalse(inspect_batch(Path(temp), 'absent')['passed'])

    def test_legacy_scope_unchanged(self):
        self.assertEqual(PROJECTS, ('node0', 'gensyn', 'distributed-training', 'agora'))
        self.assertEqual(len(EXTENDED_PROJECTS), 6)

    def test_extended_runner_reserves_three_gb_per_project(self):
        with tempfile.TemporaryDirectory() as temp:
            with patch.object(collect_public_runs, 'RESEARCH', Path(temp)), \
                 patch('sys.argv', ['collect', '--project', 'extended', '--label', 'test']), \
                 patch.object(collect_public_runs.subprocess, 'run') as run:
                run.return_value.returncode = 0
                self.assertFalse(collect_public_runs.main())
                self.assertEqual(len(run.call_args_list), 6)
                for call in run.call_args_list:
                    command = call.args[0]
                    self.assertEqual(command[command.index('--max-bytes') + 1], '3000000000')


if __name__ == '__main__':
    unittest.main()

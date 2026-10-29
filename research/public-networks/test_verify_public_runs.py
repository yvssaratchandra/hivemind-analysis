import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from verify_public_runs import verify_snapshot


class VerificationTests(unittest.TestCase):
    def test_integrity_does_not_claim_access_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'body').write_bytes(b'data')
            manifest = {'entries': [
                {'url': 'https://example.org/ok', 'status': 200, 'path': 'body',
                 'bytes': 4, 'sha256': hashlib.sha256(b'data').hexdigest()},
                {'url': 'https://example.org/private', 'status': 403}]}
            (root / 'manifest.json').write_text(json.dumps(manifest))
            result = verify_snapshot(root)
            self.assertEqual(result['errors'], [])
            self.assertEqual(result['status_counts'], {'200': 1, '403': 1})
            self.assertEqual(result['verified_bodies'], 1)
            (root / 'body').write_bytes(b'fake')
            self.assertEqual(len(verify_snapshot(root)['errors']), 1)

    def test_missing_body_is_not_successful_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'manifest.json').write_text(json.dumps({'entries': [{'status': 200}]}))
            self.assertEqual(len(verify_snapshot(root)['errors']), 1)


if __name__ == '__main__':
    unittest.main()

import io
import http.client
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_http import Archive


class Response(io.BytesIO):
    def __init__(self, body=b'{"x":1}', code=200, headers=None):
        super().__init__(body)
        self.code, self.url = code, 'https://example.org/source'
        self.headers = headers or {}


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.output = Path(self.tmp.name) / 'snapshot'
        self.addCleanup(self.tmp.cleanup)

    def test_resume_verifies_and_does_not_refetch(self):
        with patch('archive_http.urllib.request.urlopen', return_value=Response()) as fetch:
            ar = Archive(self.output, delay=0)
            entry = ar.fetch('https://example.org/source', 'source')
            self.assertEqual(ar.json(entry), {'x': 1})
            Archive(self.output, resume=True).fetch(entry['url'], 'source')
            self.assertEqual(fetch.call_count, 1)
        (self.output / entry['path']).write_text('corruption')
        with self.assertRaisesRegex(ValueError, 'mismatch'):
            Archive(self.output, resume=True)

    def test_budget_never_leaves_partial_body(self):
        with patch('archive_http.urllib.request.urlopen', return_value=Response(b'123456')):
            ar = Archive(self.output, max_bytes=5, delay=0)
            result = ar.fetch('https://example.org/source', 'large')
        self.assertEqual(result['error'], 'budget_exhausted')
        self.assertEqual(list((self.output / 'raw').iterdir()), [])

    def test_denied_body_is_evidence_not_data(self):
        with patch('archive_http.urllib.request.urlopen', return_value=Response(b'Forbidden', 403)) as fetch:
            ar = Archive(self.output, delay=0)
            result = ar.fetch('https://example.org/source', 'denied')
        self.assertEqual(fetch.call_count, 1)
        self.assertIsNone(ar.json(result))
        self.assertEqual(result['status'], 403)
        self.assertEqual(result['bytes'], 9)

    def test_rate_limit_has_bounded_retries(self):
        with patch('archive_http.urllib.request.urlopen', side_effect=[Response(code=429), Response()]) as fetch, patch('archive_http.time.sleep'):
            ar = Archive(self.output, delay=0)
            result = ar.fetch('https://example.org/source', 'retry')
        self.assertEqual(fetch.call_count, 2)
        self.assertEqual(result['status'], 200)

    def test_long_retry_after_defers(self):
        with patch('archive_http.urllib.request.urlopen', return_value=Response(code=429, headers={'Retry-After': '1000'})) as fetch:
            ar = Archive(self.output, delay=0)
            result = ar.fetch('https://example.org/source', 'later')
        self.assertEqual(fetch.call_count, 1)
        self.assertEqual(result['error'], 'deferred_retry_after')

    def test_new_output_does_not_overwrite(self):
        Archive(self.output)
        with self.assertRaises(ValueError):
            Archive(self.output)

    def test_incomplete_stream_retries_without_partial_evidence(self):
        class Broken(Response):
            def read(self, size=-1):
                raise http.client.IncompleteRead(b'partial')
        with patch('archive_http.urllib.request.urlopen', side_effect=[Broken(), Response()]), patch('archive_http.time.sleep'):
            ar = Archive(self.output, delay=0)
            result = ar.fetch('https://example.org/source', 'retry')
        self.assertEqual(ar.json(result), {'x': 1})
        self.assertEqual(len(list((self.output / 'raw').iterdir())), 1)
        self.assertNotIn('error', result)

    def test_resume_retries_a_budget_limited_response(self):
        with patch('archive_http.urllib.request.urlopen', return_value=Response(b'123456')):
            ar = Archive(self.output, max_bytes=5, delay=0)
            ar.fetch('https://example.org/source', 'large')
        with patch('archive_http.urllib.request.urlopen', return_value=Response(b'123456')):
            resumed = Archive(self.output, max_bytes=10, resume=True, delay=0)
            result = resumed.fetch('https://example.org/source', 'large')
        self.assertEqual(result['bytes'], 6)
        self.assertEqual(len(resumed.manifest['entries']), 2)


if __name__ == '__main__':
    unittest.main()

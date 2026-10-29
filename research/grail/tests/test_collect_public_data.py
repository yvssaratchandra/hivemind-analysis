import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from research.grail.collect_public_data import (
    GRAPHQL_QUERY,
    _public_graphql_result,
    bounded_write,
    csv_bytes,
    record_public_post,
    RUN_FIELDS,
)


class FakeResponse:
    def __init__(self, body):
        self.body = body
        self.code = 200
        self.url = "https://api.wandb.ai/graphql"
        self.headers = {"Content-Type": "application/json"}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self, limit):
        return self.body


class GrailPublicDataTests(unittest.TestCase):
    def test_empty_grafana_query_is_recorded_as_window_observation(self):
        from research.grail.collect_public_data import normalize
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            snapshot = output / 'grafana'
            snapshot.mkdir()
            body = json.dumps({'results': {'A': {'status': 200, 'frames': [
                {'data': {'values': [[], []]}}]}}}).encode()
            (snapshot / 'response.json').write_bytes(body)
            manifest = {'capture_status': 'complete', 'entries': [{
                'kind': 'http_response', 'url': 'https://grail-grafana.tplr.ai/api/ds/query',
                'status': 200, 'path': 'response.json', 'bytes': len(body),
                'sha256': hashlib.sha256(body).hexdigest()}]}
            (snapshot / 'manifest.json').write_text(json.dumps(manifest))
            archive = SimpleNamespace(output=output / 'http', manifest={'entries': []})
            result = normalize(output, archive)
            self.assertEqual(result['grafana_read_queries'][0]['frame_row_counts'], [0])
            self.assertFalse(result['wandb_numerical_history_verified'])

    def test_graphql_metadata_query_omits_config_and_history(self):
        self.assertNotIn("config", GRAPHQL_QUERY.lower())
        self.assertNotIn("history(", GRAPHQL_QUERY.lower())
        self.assertIn("runs(first: 10", GRAPHQL_QUERY)

    def test_project_null_is_not_interpreted_as_empty_completed_run(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            body = b'{"data":{"project":null}}'
            path = output / "raw" / "project.json"
            path.parent.mkdir()
            path.write_bytes(body)
            archive = SimpleNamespace(output=output, manifest={"entries": [{
                "method": "POST",
                "label": "wandb_api_project_metadata",
                "url": "https://api.wandb.ai/graphql",
                "status": 200,
                "path": "raw/project.json",
                "bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
            }]})
            runs, projects = _public_graphql_result(archive)
            self.assertEqual(runs, [])
            self.assertEqual(len(projects), 1)
            self.assertFalse(projects[0]["project_resolved"])
            self.assertIsNone(projects[0]["run_count"])

    def test_bounded_write_counts_preexisting_browser_capture_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            csv_body = csv_bytes([], RUN_FIELDS)
            cap = 5 + len(csv_body) + 19
            bounded_write(root, "browser/page.pdf", b"12345", cap)
            bounded_write(root, "normalized/runs.csv", csv_body, cap)
            with self.assertRaisesRegex(ValueError, "budget"):
                bounded_write(root, "summary.json", b"12345678901234567890", cap)

    def test_changed_graphql_response_never_overwrites_archived_body(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            (output / "raw").mkdir()
            archive = SimpleNamespace(output=output, limit=4096, used=0, manifest={"entries": []}, flush=lambda: None)
            with patch("research.grail.collect_public_data.time.sleep"), patch(
                "research.grail.collect_public_data.urllib.request.urlopen",
                side_effect=[FakeResponse(b'{"data":{"project":null}}'), FakeResponse(b'{"data":{"project":{"name":"grail"}}}')],
            ) as request:
                first = record_public_post(archive, "wandb_api_project_metadata", "https://api.wandb.ai/graphql", "query A")
                same = record_public_post(archive, "wandb_api_project_metadata", "https://api.wandb.ai/graphql", "query A")
                second = record_public_post(archive, "wandb_api_project_metadata", "https://api.wandb.ai/graphql", "query B")
            self.assertEqual(first["path"], same["path"])
            self.assertNotEqual(first["path"], second["path"])
            self.assertEqual(request.call_count, 2)
            self.assertEqual((output / first["path"]).read_bytes(), b'{"data":{"project":null}}')
            self.assertEqual((output / second["path"]).read_bytes(), b'{"data":{"project":{"name":"grail"}}}')


if __name__ == "__main__":
    unittest.main()

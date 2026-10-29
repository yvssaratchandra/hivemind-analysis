#!/usr/bin/env python3
"""Capture public Grail Grafana panels with bounded, read-only Loki queries.

Grafana submits reads with POST. Only its exact datasource query endpoint is
allowed here; mutations and all other POST paths remain blocked by the shared
capture policy. This does not change the shared GET-only collector.
"""
import hashlib
import json
from pathlib import Path
from urllib.parse import urlsplit

from capture_dashboard import DashboardCapture, build_parser, validate_args, utc_now

HOST = 'grail-grafana.tplr.ai'


def permitted_query(url, body):
    parsed = urlsplit(url)
    if parsed.scheme != 'https' or parsed.hostname != HOST or parsed.path != '/api/ds/query':
        return False
    if not body or len(body.encode()) > 65536:
        return False
    try:
        data = json.loads(body)
        queries = data['queries']
        start, end = int(data['from']), int(data['to'])
        if not 0 <= end - start <= 6 * 3600 * 1000 + 1000:
            return False
        return isinstance(queries, list) and 1 <= len(queries) <= 8 and all(
            isinstance(q, dict) and q.get('datasource', {}).get('type') == 'loki'
            and q.get('datasource', {}).get('uid') == 'loki'
            and isinstance(q.get('expr'), str) and len(q['expr']) <= 8192
            and q.get('queryType') in ('range', 'instant') for q in queries)
    except (ValueError, KeyError, TypeError, AttributeError):
        return False


class GrailCapture(DashboardCapture):
    def __init__(self, args):
        super().__init__(args)
        self.query_requests = []
        self.writer.manifest['grail_read_query_wrapper_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()

    def _route_request(self, route):
        request = route.request
        if request.method == 'POST' and permitted_query(request.url, request.post_data) and len(self.query_requests) < 24:
            self.query_requests.append({'url': request.url, 'requested_at': utc_now(),
                                        'body': json.loads(request.post_data)})
            route.continue_()
            return
        super()._route_request(route)

    def _save_browser_logs(self):
        if self.query_requests:
            self._save_artifact('public_query_requests.json',
                                json.dumps(self.query_requests, indent=2).encode(),
                                'read_only_query_requests', self.args.url, 'application/json')
        super()._save_browser_logs()


def main():
    parser = build_parser()
    args = parser.parse_args()
    validate_args(args, parser)
    if urlsplit(args.url).hostname != HOST:
        parser.error('This wrapper only supports the official Grail Grafana dashboard')
    return GrailCapture(args).run()


if __name__ == '__main__':
    raise SystemExit(main())

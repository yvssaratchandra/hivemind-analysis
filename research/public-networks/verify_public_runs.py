#!/usr/bin/env python3
"""Independently verify archived body hashes, sizes, containment and total storage."""
import argparse
import collections
import json
import sys
from pathlib import Path

from archive_http import digest, save_json, utc


def verify_snapshot(snapshot):
    snapshot = Path(snapshot).resolve()
    manifest_path = snapshot / 'manifest.json'
    manifest = json.loads(manifest_path.read_text())
    entries = manifest['entries']
    checked, errors, unarchived, statuses = [], [], [], collections.Counter()
    for entry in entries:
        statuses[str(entry.get('status'))] += 1
        if not entry.get('path'):
            unarchived.append({'url': entry.get('url'), 'status': entry.get('status'),
                               'reason': entry.get('error') or entry.get('application_status')})
            if entry.get('status') == 200 and not entry.get('error') and not entry.get('application_status'):
                errors.append({'url': entry.get('url'), 'error': 'HTTP 200 without saved body'})
            continue
        path = (snapshot / entry['path']).resolve()
        if not path.is_relative_to(snapshot):
            errors.append({'path': entry['path'], 'error': 'path escapes snapshot'})
            continue
        try:
            size = path.stat().st_size
            sha = digest(path)
            if size != entry['bytes'] or sha != entry['sha256']:
                errors.append({'path': entry['path'], 'error': 'hash or size mismatch'})
            else:
                checked.append({'path': entry['path'], 'bytes': size})
        except OSError as error:
            errors.append({'path': entry['path'], 'error': str(error)})
    return {'snapshot': str(snapshot), 'manifest_sha256': digest(manifest_path),
            'entries': len(entries), 'verified_bodies': len(checked),
            'status_counts': dict(statuses), 'raw_bytes': sum(e['bytes'] for e in checked),
            'total_bytes': sum(p.stat().st_size for p in snapshot.rglob('*') if p.is_file()),
            'unarchived_responses': unarchived, 'errors': errors}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path, action='append', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = {'verified_at': utc(), 'python': sys.version, 'snapshots': [verify_snapshot(p) for p in args.snapshot]}
    result['total_bytes'] = sum(s['total_bytes'] for s in result['snapshots'])
    result['within_20gb_budget'] = result['total_bytes'] <= 20_000_000_000
    result['all_saved_bodies_verified'] = all(not s['errors'] for s in result['snapshots'])
    # This status validates integrity, not historical coverage or HTTP success.
    result['passed'] = result['within_20gb_budget'] and result['all_saved_bodies_verified']
    save_json(args.output, result)
    print(json.dumps(result, indent=2))
    return not result['passed']


if __name__ == '__main__':
    raise SystemExit(main())

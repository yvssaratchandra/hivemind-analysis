#!/usr/bin/env python3
"""Verify the six-project batch and build an offline screenshot index.

Counts files once even when browser manifests are nested below HTTP manifests.
Integrity success is deliberately separate from public-history coverage.
"""
import argparse
import gzip
import html
import json
import re
from pathlib import Path

from archive_http import digest, save_json, utc
from collect_public_runs import EXTENDED_PROJECTS, RESEARCH
from verify_public_runs import verify_snapshot


def inspect_batch(research, label):
    projects = {}
    for project in EXTENDED_PROJECTS:
        root = research / project / 'data' / label
        files = sorted(p for p in root.rglob('*') if p.is_file())
        manifests = sorted(root.rglob('manifest.json'))
        checks = []
        errors = []
        for manifest in manifests:
            try:
                checks.append(verify_snapshot(manifest.parent))
            except (OSError, ValueError, KeyError, TypeError) as exc:
                errors.append({'path': str(manifest), 'error': str(exc)})
        if not manifests:
            errors.append({'error': 'No manifests found'})
        inventory = [{'path': str(p.relative_to(research)), 'bytes': p.stat().st_size,
                      'sha256': digest(p)} for p in files if not p.is_symlink()]
        for path in files:
            if path.suffix == '.gz' and not path.is_symlink():
                try:
                    with gzip.open(path, 'rb') as stream:
                        while stream.read(1024 * 1024):
                            pass
                except (OSError, EOFError) as exc:
                    errors.append({'path': str(path), 'error': 'Invalid gzip: ' + str(exc)})
        errors.extend({'path': str(p), 'error': 'Symlink in archive'} for p in files if p.is_symlink())
        size = sum(p['bytes'] for p in inventory)
        projects[project] = {'total_bytes': size, 'manifests': checks,
                             'files': inventory, 'errors': errors,
                             'passed': bool(checks) and not errors and
                             all(not c['errors'] for c in checks) and size <= 3_000_000_000}
    total = sum(p['total_bytes'] for p in projects.values())
    return {'verified_at': utc(), 'label': label, 'projects': projects, 'total_bytes': total,
            'budget_bytes': 20_000_000_000,
            'passed': all(p['passed'] for p in projects.values()) and total <= 20_000_000_000,
            'scope': 'Saved-file integrity only; see project reports for historical coverage.'}


def gallery(result, output, research):
    import os
    blocks = []
    for project, info in result['projects'].items():
        blocks.append('<h2>' + html.escape(project) + '</h2>')
        report = research / project / 'PUBLIC_RUNS.md'
        blocks.append('<p><a href="' + html.escape(os.path.relpath(report, output.parent), quote=True) + '">Coverage report</a></p>')
        images = [f for f in info['files'] if f['path'].endswith('/full_page.png')]
        for item in images:
            path = research / item['path']
            href = html.escape(os.path.relpath(path, output.parent), quote=True)
            blocks.append('<p>' + html.escape(str(path.parent.parent.relative_to(research))) + '</p>')
            summary_path = path.parent / 'page_summary.json'
            if summary_path.is_file():
                summary = json.loads(summary_path.read_text())
                blocks.append('<p>' + html.escape(str(summary.get('title', ''))) + '</p>')
                blocks.append('<pre style="white-space:pre-wrap">' +
                              html.escape(str(summary.get('visible_text', ''))[:400]) + '</pre>')
            blocks.append(f'<a href="{href}"><img loading="lazy" src="{href}" alt="Archived dashboard screenshot"></a>')
            for name in ('page.mhtml', 'page.pdf', 'rendered_dom.html', 'page_summary.json'):
                artifact = path.parent / name
                if artifact.exists():
                    url = html.escape(os.path.relpath(artifact, output.parent), quote=True)
                    blocks.append(f'<a href="{url}">{name}</a> ')
        if not images:
            blocks.append('<p>No full-page screenshot found; consult the coverage report.</p>')
    document = ('<!doctype html><meta charset="utf-8"><title>Extended run archive</title>'
                      '<style>body{font:16px sans-serif;max-width:1100px;margin:auto}img{max-width:100%;max-height:700px;object-fit:contain;object-position:left top}</style>'
                      '<h1>Extended public-run archive</h1><p>Local snapshots, not live dashboards. '
                      'Screenshots may show errors, loading states or current runs rather than the historical run. '
                      'Read each coverage report before interpreting them.</p>' + ''.join(blocks))
    output.write_text('\n'.join(line.rstrip() for line in document.splitlines()) + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--label', default='extended_20261029')
    parser.add_argument('--output', type=Path, default=Path(__file__).parent / 'EXTENDED_VERIFICATION.json')
    parser.add_argument('--gallery', type=Path, default=Path(__file__).parent / 'EXTENDED_DASHBOARDS.html')
    args = parser.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', args.label):
        parser.error('label must be a single safe directory name')
    result = inspect_batch(RESEARCH, args.label)
    save_json(args.output, result)
    gallery(result, args.gallery, RESEARCH)
    print(json.dumps({'passed': result['passed'], 'total_bytes': result['total_bytes'],
                      'projects': {p: {'passed': v['passed'], 'bytes': v['total_bytes']}
                                   for p, v in result['projects'].items()}}, indent=2))
    return not result['passed']


if __name__ == '__main__':
    raise SystemExit(main())

#!/usr/bin/env python3
"""Build a local, network-free gallery and inventory from browser captures."""
import argparse
import hashlib
import html
import json
import os
from html.parser import HTMLParser


class ActiveTabParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.active = False
        self.text = []
    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "button":
            self.active = attrs.get("role") == "tab" and attrs.get("aria-selected") == "true"
    def handle_endtag(self, tag):
        if tag == "button": self.active = False
    def handle_data(self, data):
        if self.active: self.text.append(data)
from pathlib import Path


def build(root, label):
    base = root / 'research/public-networks'
    targets = json.loads((base / 'DASHBOARD_TARGETS.json').read_text())['targets']
    records, cards = [], []
    for target in targets:
        folder = root / 'research' / target['project'] / 'data' / label / target['label']
        manifest_path = folder / 'manifest.json'
        if not manifest_path.exists():
            records.append({'target': target, 'status': 'not_captured'})
            continue
        manifest = json.loads(manifest_path.read_text())
        artifacts = []
        for entry in manifest['entries']:
            if not entry.get('path'):
                continue
            path = (folder / entry['path']).resolve()
            if not path.is_relative_to(folder.resolve()):
                raise ValueError('Artifact escapes snapshot')
            if path.stat().st_size != entry['bytes'] or hashlib.sha256(path.read_bytes()).hexdigest() != entry['sha256']:
                raise ValueError(f'Corrupt artifact: {path}')
            if entry['kind'] != 'http_response':
                artifacts.append(entry)
        tab = ActiveTabParser()
        dom = folder / 'artifacts/rendered_dom.html'
        if dom.exists(): tab.feed(dom.read_text())
        active_tab = ''.join(tab.text).strip() or None
        record = {
            'active_tab': active_tab,
            'selection_succeeded': all(a.get('status') == 'clicked' for a in manifest.get('selector_actions', [])),
            'target': target, 'snapshot': str(folder.relative_to(root)),
            'manifest_sha256': hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
            'status': manifest['capture_status'], 'created_at': manifest['created_at'],
            'completed_at': manifest.get('completed_at'),
            'total_saved_bytes': manifest.get('total_saved_bytes'),
            'selector_actions': manifest.get('selector_actions', []),
            'artifacts': artifacts,
            'unsaved_entries': [e for e in manifest['entries'] if not e.get('path')],
        }
        records.append(record)
        esc = html.escape
        def href(path):
            return esc(os.path.relpath(path, base), quote=True)
        links = []
        images = []
        for a in artifacts:
            path = folder / a['path']
            links.append(f'<a href="{href(path)}">{esc(path.name)}</a>')
            if a['kind'] in ('screenshot_png', 'scroll_pane_screenshot'):
                images.append(f'<details {"open" if a["kind"] == "screenshot_png" else ""}><summary>{esc(path.name)}</summary><img loading="lazy" src="{href(path)}" alt="{esc(target["label"])}"></details>')
        cards.append(f'<section><h2>{esc(target["project"])} / {esc(target["label"])}</h2><p>{esc(manifest["created_at"])} — {esc(manifest["capture_status"])}</p><p>Requested view: {esc(target.get('view', target['label']))}; active tab: {esc(active_tab or 'not applicable / not detected')}. Selection completed: {record['selection_succeeded']}.</p><p>Source: {esc(target["url"])}</p><p>{" · ".join(links)}</p>{"".join(images)}</section>')
    inventory = {'snapshot_label': label, 'note': 'Separate dated page captures, not an atomic snapshot or a fully interactive offline website.', 'views': records}
    (base / 'DASHBOARD_SNAPSHOTS.json').write_text(json.dumps(inventory, indent=2) + '\n')
    page = '''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Public dashboard snapshots</title><style>body{font:16px system-ui;max-width:1500px;margin:2rem auto;padding:0 1rem;background:#f3f5f7;color:#17212c}section{background:white;padding:1rem;margin:2rem 0;border:1px solid #ccd}img{max-width:100%;height:auto}summary{cursor:pointer;padding:.6rem}a{color:#164a9b}p{overflow-wrap:anywhere}</style><h1>Public dashboard snapshots</h1><p>Local screenshots, HTML, PDFs, MHTML and response archives. These captures preserve the visible state at individual retrieval times. Errors, unavailable panels and incomplete history remain evidence gaps. The gallery itself needs no internet; HTML copies may contain original network references. Use PNG/PDF for dependable static viewing. Raw files must be present beside this repository.</p>'''
    (base / 'DASHBOARDS.html').write_text(page + ''.join(cards) + '</html>\n')
    print(json.dumps({'views': len(records), 'captured': sum(v['status'] != 'not_captured' for v in records)}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--label', required=True)
    args = parser.parse_args()
    if '/' in args.label or args.label in ('.', '..'):
        parser.error('label must be a single directory name')
    build(Path(__file__).resolve().parents[2], args.label)

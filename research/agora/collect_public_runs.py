#!/usr/bin/env python3
"""Archive public Agora dashboards and normalize operator-reported observations."""
import argparse
import csv
import gzip
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'public-networks'))
from archive_http import Archive, digest, save_json

BASE = 'https://agora.pluralis.ai'


def write_csv(path, rows, fields=None):
    rows = list(rows)
    fields = fields or sorted({key for row in rows for key in row})
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: json.dumps(v, sort_keys=True) if isinstance(v, (list, dict)) else v
                             for k, v in row.items()})


def matrix_rows(run, operation, matrix, raw):
    times, workers, values = (matrix.get(k, []) for k in ('timestamps', 'workers', 'values'))
    if len(workers) != len(values) or any(len(row) != len(times) for row in values):
        raise ValueError(f'Misaligned matrix {run}/{operation}')
    for worker, row in zip(workers, values):
        for timestamp, value in zip(times, row):
            if value is not None:
                yield dict(run_id=run, metric=operation, worker=worker, timestamp=timestamp,
                           value=value, unit='native_unverified', source_file=raw)


def analyze(output):
    output = Path(output)
    manifest = json.loads((output / 'manifest.json').read_text())
    # Verify all bodies, including documents and unavailable HTTP responses.
    for entry in manifest['entries']:
        if entry.get('path') and (digest(output / entry['path']) != entry['sha256'] or
                                 (output / entry['path']).stat().st_size != entry['bytes']):
            raise ValueError('Evidence mismatch: ' + entry['path'])
    entries = {e['url']: e for e in manifest['entries']}

    def get(url):
        entry = entries.get(url, {})
        if entry.get('status') != 200 or not entry.get('path'):
            return None, entry.get('path', '')
        try:
            return json.loads((output / entry['path']).read_text()), entry['path']
        except (ValueError, UnicodeError):
            return None, entry['path']

    registry, registry_path = get(BASE + '/api/runs')
    if not isinstance(registry, dict) or not isinstance(registry.get('runs'), list):
        raise ValueError('No valid run registry; see manifest')
    runs, nodes, leaderboards, training_rows, telemetry_summary, gpu_rows = [], [], [], [], [], []
    tables = output / 'tables'
    tables.mkdir(exist_ok=True)
    fields = ['run_id', 'metric', 'worker', 'timestamp', 'value', 'unit', 'source_file']
    with (tables / 'telemetry.csv.gz').open('wb') as compressed:
        # Fixed gzip timestamp makes offline regeneration byte-reproducible.
        import io
        with gzip.GzipFile(fileobj=compressed, mode='wb', mtime=0, filename='') as gz:
            with io.TextIOWrapper(gz, encoding='utf-8', newline='') as text:
                writer = csv.DictWriter(text, fieldnames=fields)
                writer.writeheader()
                for run in registry['runs']:
                    if run['id'].startswith('node0'):
                        continue
                    rid = run['id']
                    prefix = BASE + '/api/runs/' + rid + '/'
                    peers, peers_path = get(prefix + 'peers')
                    train, train_path = get(prefix + 'training?range=all')
                    board, board_path = get(prefix + 'leaderboard')
                    prom, prom_path = get(prefix + 'prometheus-timeseries?range=all')
                    evals, eval_path = get(prefix + 'evals')
                    info = dict(run_id=rid, **{k: v for k, v in run.items() if k != 'id'},
                                registry_source=registry_path)
                    # Missing exports remain distinguishable from valid empty ones.
                    info['peers_export_available'] = isinstance(peers, dict) and isinstance(peers.get('nodes'), list)
                    info['training_export_available'] = isinstance(train, dict) and isinstance(train.get('metrics'), dict)
                    info['telemetry_export_available'] = isinstance(prom, dict) and isinstance(prom.get('operations'), dict)
                    info['node_records'] = len(peers['nodes']) if info['peers_export_available'] else None
                    info['leaderboard_records'] = len(board) if isinstance(board, list) else None
                    info['eval_tasks'] = len(evals.get('tasks', {})) if isinstance(evals, dict) else None
                    if isinstance(peers, dict):
                        nodes.extend(dict(run_id=rid, source_file=peers_path, **node) for node in peers.get('nodes', []))
                        # Keep dataset/group index; the API provides distinct nested populations.
                        for category, groups in peers.get('gpu_temporal_data', {}).items():
                            for group_index, group in enumerate(groups):
                                for point in group:
                                    for gpu, value in point.items():
                                        if gpu != 'timestamp':
                                            gpu_rows.append(dict(run_id=rid, category=category, group_index=group_index,
                                                                 timestamp=point.get('timestamp'), gpu=gpu, value=value,
                                                                 source_file=peers_path))
                    if isinstance(board, list):
                        leaderboards.extend(dict(run_id=rid, source_file=board_path, **row) for row in board)
                    if isinstance(train, dict):
                        info['reported_progress'] = train.get('progress')
                        for metric, values in train.get('metrics', {}).items():
                            if isinstance(values, list):
                                info[metric + '_points'] = len(values)
                                for point in values:
                                    training_rows.append(dict(run_id=rid, metric=metric, source_file=train_path,
                                                              unit='tokens/second' if metric == 'tps' else 'native_loss', **point))
                                if values:
                                    info[metric + '_first'] = values[0]
                                    info[metric + '_last'] = values[-1]
                    if isinstance(prom, dict):
                        for operation, matrix in prom.get('operations', {}).items():
                            count = 0
                            for row in matrix_rows(rid, operation, matrix, prom_path):
                                writer.writerow(row)
                                count += 1
                            cells = len(matrix.get('workers', [])) * len(matrix.get('timestamps', []))
                            telemetry_summary.append(dict(run_id=rid, metric=operation,
                                worker_series=len(matrix.get('workers', [])), timestamps=len(matrix.get('timestamps', [])),
                                observed_values=count, null_values=cells-count, source_file=prom_path))
                    runs.append(info)
    write_csv(tables / 'runs.csv', runs)
    write_csv(tables / 'nodes.csv', nodes)
    write_csv(tables / 'leaderboards.csv', leaderboards)
    write_csv(tables / 'training.csv', training_rows)
    write_csv(tables / 'telemetry_summary.csv', telemetry_summary)
    write_csv(tables / 'gpu_population.csv', gpu_rows)
    summary = {'runs': runs, 'telemetry': telemetry_summary,
               'raw_bytes': sum(e.get('bytes', 0) for e in manifest['entries']),
               'manifest_records': len(manifest['entries']),
               'distinct_source_urls': len(entries),
               'failed_sources': [e for e in entries.values() if e.get('status') != 200 or not e.get('path')]}
    save_json(output / 'summary.json', summary)
    lines = ['# Public Agora run evidence', '',
             'Operator-reported dashboard exports, not an independent rerun.', '',
             '| Run | Model | Peer records | Leaderboard rows | Loss points | First / last sampled loss |',
             '| --- | --- | ---: | ---: | ---: | --- |']
    for r in runs:
        lines.append(f"| {r['run_id']} | {r.get('meta', {}).get('model')} | {r['node_records']} | "
                     f"{r['leaderboard_records']} | {r.get('train_loss_points')} | "
                     f"{r.get('train_loss_first', {}).get('y')} / {r.get('train_loss_last', {}).get('y')} |")
    lines += ['', '## Coverage and interpretation', '',
              '- `range=all` covers the exposed time window; it does not guarantee unsampled training steps. The series have gaps in step indices.',
              '- Telemetry CSV contains non-null matrix cells; raw matrices retain every null. Missing samples are not zero.',
              '- Worker names in telemetry and peer names are retained verbatim, without speculative identity matching.',
              '- Leaderboard score and active_time retain native units; no conversion to verified compute contribution is made.',
              '- The 1.3T dataset label is not the number of tokens processed by a run.',
              '- GPU population group indices are preserved without assuming disjoint populations or summing them.',
              '- Evaluation capability does not imply available evaluations; inspect eval_tasks and the raw response.',
              '- Archived queue/status fields describe the source snapshot, not present training activity.',
              '- Current source code is not evidence of the exact historical deployment version.',
              '- See manifest.json for HTTP failures and raw provenance; summary.json and tables/ are reproducible offline.']
    (output / 'report.md').write_text('\n'.join(lines) + '\n')
    return summary


def collect(args):
    archive = Archive(args.output, args.max_bytes, args.resume)
    registry = archive.json(archive.fetch(BASE + '/api/runs', 'registry'))
    home = archive.fetch(BASE + '/', 'dashboard')
    if home.get('status') == 200 and home.get('path'):
        for path in re.findall(r'<script[^>]*src="([^"]+)"', (archive.output / home['path']).read_text()):
            if path.startswith('/_next/'):
                archive.fetch(BASE + path, 'dashboard_asset')
    if not isinstance(registry, dict):
        raise ValueError('Registry inaccessible; inspect manifest')
    for run in registry.get('runs', []):
        if run['id'].startswith('node0'):
            continue
        routes = ['peers', 'training?range=all', 'leaderboard', 'prometheus-timeseries?range=all']
        if run.get('capabilities', {}).get('evals'):
            routes.append('evals')
        for route in routes:
            archive.fetch(BASE + '/api/runs/' + run['id'] + '/' + route, run['id'] + '/' + route)
    for url, label in [('https://arxiv.org/pdf/2607.13332', 'agora_paper'),
                       ('https://pluralis.ai/docs/', 'pluralis_docs')]:
        archive.fetch(url, label)
    return analyze(args.output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--max-bytes', type=int, default=4_000_000_000)
    parser.add_argument('--resume', action='store_true')
    parser.add_argument('--analyze-only', action='store_true')
    args = parser.parse_args()
    summary = analyze(args.output) if args.analyze_only else collect(args)
    print(json.dumps({'runs': len(summary['runs']), 'raw_bytes': summary['raw_bytes'],
                      'failed_sources': len(summary['failed_sources'])}, indent=2))


if __name__ == '__main__':
    main()

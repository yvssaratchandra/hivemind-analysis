#!/usr/bin/env python3
"""Plot archived observations only; no network and no interpolation of missing data."""
import argparse
from collections import Counter
import hashlib
import gzip
import json
import os
from pathlib import Path

os.environ.setdefault('MPLCONFIGDIR', str(Path(__file__).resolve().parent / '.plot-env' / 'mplconfig'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def records(snapshot):
    manifest = json.loads((snapshot / 'manifest.json').read_text())
    for entry in manifest['entries']:
        if entry.get('status') != 200 or not entry.get('path'):
            continue
        url = entry.get('url', '')
        # Verify plotted inputs here; the independent verifier covers all bodies.
        # Avoid parsing multi-gigabyte histories that this figure does not use.
        if not (('/runs/' in url and ('/training?' in url or url.endswith('/leaderboard')))
                or url.endswith('/api/mech1/history')
                or entry['path'].endswith('distributed_training_miners/page-0000.json')):
            continue
        path = snapshot / entry['path']
        content = path.read_bytes()
        if hashlib.sha256(content).hexdigest() != entry['sha256']:
            raise ValueError('Hash mismatch: ' + str(path))
        try:
            yield entry, json.loads(gzip.decompress(content) if content.startswith(b'\x1f\x8b') else content)
        except (ValueError, UnicodeError):
            continue


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path, action='append', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    series, boards, benchmark, provenance, miner_examples = [], [], [], [], []
    for snapshot in args.snapshot:
        for entry, data in records(snapshot):
            url = entry['url']
            included = False
            if '/training?' in url and isinstance(data, dict):
                points = data.get('metrics', {}).get('train_loss', [])
                points = [p for p in points if p.get('step') is not None and p.get('y') is not None]
                if points:
                    series.append((url.split('/runs/')[1].split('/')[0], snapshot.name, points))
                    included = True
            if url.endswith('/leaderboard') and isinstance(data, list):
                scores = [p['score'] for p in data if isinstance(p.get('score'), (int, float))]
                if scores:
                    boards.append((url.split('/runs/')[1].split('/')[0], snapshot.name, sorted(scores)))
                    included = True
            if url.endswith('/api/mech1/history') and isinstance(data, dict):
                benchmark.append((snapshot.name, [p for p in data.get('history', []) if p.get('block') is not None and p.get('best_loss') is not None]))
                included = True
            if entry.get('path', '').endswith('distributed_training_miners/page-0000.json') and isinstance(data, dict):
                for edge in data.get('data', {}).get('project', {}).get('runs', {}).get('edges', []):
                    node = edge.get('node', {})
                    history = node.get('history') or []
                    if len(history) != node.get('historyLineCount') or len(history) < 2:
                        continue
                    rows = [json.loads(row) if isinstance(row, str) else row for row in history]
                    points = [row for row in rows if isinstance(row.get('_step'), (int, float))
                              and isinstance(row.get('loss'), (int, float))]
                    if points and len(miner_examples) < 3:
                        miner_examples.append((node['name'], node.get('state'), sorted(points, key=lambda p: p['_step'])))
                        included = True
            if included:
                provenance.append({'snapshot': str(snapshot), 'url': url, 'path': entry['path'], 'sha256': entry['sha256']})
    plt.rcParams.update({'font.size': 10, 'axes.spines.top': False, 'axes.spines.right': False,
                         'svg.hashsalt': 'public-training-archive-v1'})
    if series:
        series.sort(key=lambda item: (item[0], item[1]))
        names = Counter(item[0] for item in series)
        fig, axes = plt.subplots(1, len(series), figsize=(5 * len(series), 4.4), squeeze=False)
        for ax, (name, snapshot_name, points) in zip(axes[0], series):
            ax.plot([p['step'] for p in points], [p['y'] for p in points], linewidth=1.1)
            ax.set(title=name if names[name] == 1 else name + '\n' + snapshot_name, xlabel='Reported training step', ylabel='Reported training loss')
            ax.grid(alpha=.2)
            ax.text(.97, .96, f'{len(points):,} exported points', transform=ax.transAxes, ha='right', va='top', fontsize=9)
        fig.suptitle('Public dashboard training curves — sampled observations, separate runs')
        fig.tight_layout()
        fig.savefig(args.output / 'training_loss.png', dpi=160)
        fig.savefig(args.output / 'training_loss.svg', metadata={'Date': None})
        plt.close(fig)
    if boards:
        boards.sort(key=lambda item: (item[0], item[1]))
        names = Counter(item[0] for item in boards)
        fig, axes = plt.subplots(1, len(boards), figsize=(5 * len(boards), 4.4), squeeze=False)
        for ax, (name, snapshot_name, scores) in zip(axes[0], boards):
            ax.step(scores, [(i + 1) / len(scores) for i in range(len(scores))], where='post')
            ax.set(title=name if names[name] == 1 else name + '\n' + snapshot_name, xlabel='Reported score (native units)', ylabel='Fraction of leaderboard rows')
            ax.grid(alpha=.2)
        fig.suptitle('Leaderboard score distributions — scores are not verified compute contribution')
        fig.tight_layout()
        fig.savefig(args.output / 'leaderboard_scores.png', dpi=160)
        fig.savefig(args.output / 'leaderboard_scores.svg', metadata={'Date': None})
        plt.close(fig)
    if benchmark:
        fig, ax = plt.subplots(figsize=(8, 4.4))
        for label, points in benchmark:
            points.sort(key=lambda p: p['block'])
            ax.plot([p['block'] for p in points], [p['best_loss'] for p in points], marker='o', markersize=3, label=label)
        if len(benchmark) > 1:
            ax.legend()
        ax.set(title='Distributed Training: best strategy benchmark loss by evaluation block',
               xlabel='Evaluation block', ylabel='Reported best benchmark loss')
        ax.grid(alpha=.2)
        fig.text(.5, .01, 'Different strategies may win each evaluation; this is not one model’s training curve.', ha='center', fontsize=9)
        fig.tight_layout(rect=(0, .04, 1, 1))
        fig.savefig(args.output / 'strategy_benchmarks.png', dpi=160)
        fig.savefig(args.output / 'strategy_benchmarks.svg', metadata={'Date': None})
        plt.close(fig)
    if miner_examples:
        fig, axes = plt.subplots(1, len(miner_examples), figsize=(5 * len(miner_examples), 4.5), squeeze=False)
        for ax, (name, state, points) in zip(axes[0], miner_examples):
            ax.plot([p['_step'] for p in points], [p['loss'] for p in points], linewidth=.8)
            ax.set(title=f'{name} ({state})', xlabel='W&B logging step (_step)', ylabel='Native loss metric')
            ax.grid(alpha=.2)
        fig.suptitle('Distributed Training: illustrative miner histories, separate runs')
        fig.text(.5, .01, 'First three qualifying runs on the first exported page; models and restart boundaries are not established.', ha='center', fontsize=9)
        fig.tight_layout(rect=(0, .04, 1, 1))
        fig.savefig(args.output / 'miner_loss_examples.png', dpi=160)
        fig.savefig(args.output / 'miner_loss_examples.svg', metadata={'Date': None})
        plt.close(fig)
    (args.output / 'PROVENANCE.json').write_text(json.dumps({
        'matplotlib': matplotlib.__version__, 'sources': provenance,
        'miner_example_selection': 'First three runs on miners page 0000 with at least two history rows, count matching historyLineCount, and numeric loss/_step. Illustrative, not representative.',
        'miner_example_runs': [name for name, _, _ in miner_examples],
    }, indent=2) + '\n')


if __name__ == '__main__':
    main()

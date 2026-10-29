#!/usr/bin/env python3
"""Run project-specific public archives sequentially within a combined budget."""
import argparse
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

RESEARCH = Path(__file__).resolve().parent.parent
PROJECTS = ('node0', 'gensyn', 'distributed-training', 'agora')
EXTENDED_PROJECTS = ('psyche', 'templar', 'grail', 'iota', 'intellect-1', 'intellect-2')
SOURCE_URLS = {
    'psyche': ['https://github.com/PsycheFoundation/nousnet', 'https://mainnet.psyche.network/'],
    'templar': ['https://github.com/one-covenant/templar', 'https://www.tplr.ai/dashboard'],
    'grail': ['https://github.com/one-covenant/grail', 'https://wandb.ai/tplr/grail', 'https://grail-grafana.tplr.ai/'],
    'iota': ['https://github.com/macrocosm-os/iota', 'https://iota.macrocosmos.ai/dashboard/overview'],
    'intellect-1': ['https://github.com/PrimeIntellect-ai/prime-diloco', 'https://huggingface.co/PrimeIntellect/INTELLECT-1'],
    'intellect-2': ['https://github.com/PrimeIntellect-ai/prime-rl', 'https://huggingface.co/PrimeIntellect/INTELLECT-2'],
    'node0': ['https://github.com/PluralisResearch/node0', 'https://agora.pluralis.ai/api/runs'],
    'gensyn': ['https://github.com/gensyn-ai/rl-swarm', 'https://dashboard.gensyn.ai/', 'https://swarm.gensyn.ai/'],
    'distributed-training': ['https://github.com/dstrbtd/DistributedTraining', 'https://dash.dstrbtd.ai/mech1',
                             'https://wandb.ai/kmfoda/distributed_training_miners'],
    'agora': ['https://github.com/PluralisResearch/agora', 'https://agora.pluralis.ai/api/runs'],
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project', choices=('all', 'extended') + PROJECTS + EXTENDED_PROJECTS, default='all')
    parser.add_argument('--label', default='public_runs_' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
    parser.add_argument('--max-bytes', type=int, default=20_000_000_000,
                        help='Total target; extended projects capped at 3 GB each; legacy raw writes capped at 80%%')
    parser.add_argument('--resume', action='store_true')
    parser.add_argument('--analyze-only', action='store_true')
    parser.add_argument('--discover-only', action='store_true', help='Print known source catalog without network or writes')
    args = parser.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', args.label):
        parser.error('label must be a single safe directory name')
    if not 1 <= args.max_bytes <= 20_000_000_000:
        parser.error('budget must be positive and no greater than 20 GB')
    projects = (PROJECTS if args.project == 'all' else
                EXTENDED_PROJECTS if args.project == 'extended' else (args.project,))
    if args.discover_only:
        print(json.dumps({p: SOURCE_URLS[p] for p in projects}, indent=2))
        return 0
    raw_budget = args.max_bytes * 4 // 5
    failed = []
    for index, project in enumerate(projects):
        script = RESEARCH / project / 'collect_public_runs.py'
        output = RESEARCH / project / 'data' / args.label
        existing = {p: sum(f.stat().st_size for f in (RESEARCH / p / 'data' / args.label / 'raw').rglob('*') if f.is_file())
                    for p in projects}
        # Return unused allocations to later projects rather than discard usable budget.
        per_project = existing[project] + max(0, raw_budget - sum(existing.values())) // (len(projects) - index)
        if project in EXTENDED_PROJECTS:
            # New collectors accept a total output budget, including derived files.
            per_project = min(3_000_000_000, args.max_bytes // len(projects))
        if per_project < 1:
            failed.append(project)
            continue
        command = [sys.executable, str(script), '--output', str(output), '--max-bytes', str(per_project)]
        if args.resume:
            command.append('--resume')
        if args.analyze_only:
            command.append('--analyze-only')
        print('PROJECT', project, 'OUTPUT', output, flush=True)
        result = subprocess.run(command, check=False)
        if result.returncode:
            failed.append(project)
        # Include normalized files in the final budget accounting; no clone data.
        size = sum(f.stat().st_size for p in projects for f in (RESEARCH / p / 'data' / args.label).rglob('*') if f.is_file())
        if size > args.max_bytes:
            raise RuntimeError('Archive budget reached after normalization; stopped subsequent collections')
    print(json.dumps({'label': args.label, 'failed_projects': failed}, indent=2))
    return bool(failed)


if __name__ == '__main__':
    raise SystemExit(main())

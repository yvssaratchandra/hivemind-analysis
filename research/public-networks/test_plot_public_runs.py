"""Regression check for plotting repeated run identities across snapshots."""
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


@unittest.skipUnless(importlib.util.find_spec('matplotlib'), 'requires optional plot environment')
class PlotTests(unittest.TestCase):
    def test_duplicate_runs_and_multiple_benchmark_snapshots(self):
        import plot_public_runs as plot
        from matplotlib.axes import Axes
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            snapshots = []
            for index in range(2):
                snapshot = root / f'snapshot-{index}'
                snapshot.mkdir()
                entries = []
                for name, url, data in [
                    ('train', 'https://example.org/api/runs/same-run/training?range=all',
                     {'metrics': {'train_loss': [{'step': 0, 'y': 3 + index}, {'step': 1, 'y': 2}]}}),
                    ('benchmark', 'https://example.org/api/mech1/history',
                     {'history': [{'block': 10, 'best_loss': 3 + index}]}),
                ]:
                    body = json.dumps(data).encode()
                    path = name + '.json'
                    (snapshot / path).write_bytes(body)
                    entries.append({'url': url, 'path': path, 'status': 200,
                                    'sha256': hashlib.sha256(body).hexdigest()})
                (snapshot / 'manifest.json').write_text(json.dumps({'entries': entries}))
                snapshots.extend(['--snapshot', str(snapshot)])
            labels = []
            original = Axes.plot

            def observed(ax, *args, **kwargs):
                if 'label' in kwargs:
                    labels.append(kwargs['label'])
                return original(ax, *args, **kwargs)

            with patch.object(sys, 'argv', ['plot', *snapshots, '--output', str(root / 'plots')]), \
                    patch.object(Axes, 'plot', observed):
                plot.main()
            self.assertEqual(labels, ['snapshot-0', 'snapshot-1'])
            self.assertTrue((root / 'plots/training_loss.png').is_file())


if __name__ == '__main__':
    unittest.main()

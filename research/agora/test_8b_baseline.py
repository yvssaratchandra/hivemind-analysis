"""Focused checks for delay assumptions, native partitioning and checkpoints."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import torch
from agora_server.core.all_reduce.partition import TensorPartContainer
from agora_server.core.averaging.state_averager_sparta import PartitionedIndexSelector
from baseline_8b import clock_discontinuous, leg_delay
from run_8b_baseline import ROOT, atomic, digest, event, make_profiles, resume, tree_hashes, validate


class HarnessTests(unittest.TestCase):
    def test_delay_units_and_self(self):
        mapping = {'a': {'rtt_ms': 20}, 'b': {'rtt_ms': 40}}
        self.assertEqual(leg_delay('a', 'a', mapping), 0)
        self.assertAlmostEqual(leg_delay('a', 'b', mapping), .030)
        self.assertEqual(leg_delay('a', 'b', mapping), leg_delay('b', 'a', mapping))

    def test_suspend_and_wall_clock_detection(self):
        a = dict(monotonic=10, boottime=10, wall=100)
        self.assertFalse(clock_discontinuous(a, dict(monotonic=12, boottime=12, wall=102)))
        self.assertTrue(clock_discontinuous(a, dict(monotonic=12, boottime=72, wall=162)))
        self.assertTrue(clock_discontinuous(a, dict(monotonic=12, boottime=12, wall=103)))

    def test_profiles_reproducible_and_role_filtered(self):
        path = ROOT/'RUN_8B_DATA_20261013.json'
        a = make_profiles(path, 8101, 'pilot')
        self.assertEqual(a, make_profiles(path, 8101, 'pilot'))
        self.assertNotEqual(a, make_profiles(path, 8102, 'pilot'))
        self.assertEqual(len({p['source_record'] for p in a.values()}), 8)
        for name, profile in a.items():
            self.assertEqual(profile['role'], 'reducer' if name.startswith('R') else 'contributor')
            self.assertGreater(profile['rtt_ms'], 0)
            self.assertLessEqual(profile['rtt_ms'], 80)

    def test_sparse_native_partition_covers_tensor_once(self):
        torch.manual_seed(7)
        param = torch.zeros(100)
        selector = PartitionedIndexSelector(.05, param)
        selected = [selector.get_indices(param, n).nonzero().flatten().tolist() for n in range(20)]
        self.assertTrue(all(len(indices)==5 for indices in selected))
        self.assertEqual(sorted(sum(selected, [])), list(range(100)))
        self.assertTrue(torch.equal(selector.get_indices(param, 0), selector.get_indices(param, 20)))

    def test_scalar_parts_exercise_four_reducers_in_interleaved_order(self):
        fractions = [0, .25, .25, 0, .25, 0, 0, .25]
        scalar_parts = TensorPartContainer([torch.tensor(float(i)) for i in range(5)],
                                           fractions, list(range(8)), part_size_bytes=2097152)
        self.assertEqual(sum(scalar_parts.num_parts_by_peer), 5)
        for fraction, count in zip(fractions, scalar_parts.num_parts_by_peer):
            self.assertEqual(count>0, fraction>0)

    def test_checkpoint_validates_completed_raw_and_preserves_interrupted(self):
        with tempfile.TemporaryDirectory() as temporary:
            suite = Path(temporary)
            (suite/'source.py').write_text('frozen')
            (suite/'done').mkdir()
            raw = suite/'done/raw.jsonl'
            raw.write_text('{}\n')
            (suite/'partial').mkdir()
            (suite/'partial/raw.jsonl').write_text('interrupted')
            atomic(suite/'provenance.json', {'frozen_sha256': {'source.py':digest(suite/'source.py')}})
            atomic(suite/'checkpoint.json', {'jobs':[
                {'seed':1, 'status':'completed', 'attempt':'done', 'result_sha256':tree_hashes(suite/'done')},
                {'seed':2, 'status':'running', 'attempt':'partial', 'attempts':['partial']}]})
            before = tree_hashes(suite)
            # Offline validation must never execute a worker or even exec a runtime.
            with patch('run_8b_baseline.os.execve', side_effect=AssertionError('launched runtime')):
                resume(suite, dry_run=True)
            self.assertEqual(before, tree_hashes(suite))
            raw.write_text('{"tampered":true}\n')
            with self.assertRaisesRegex(ValueError, 'Completed result changed'):
                validate(suite)

    def test_checkpoint_rejects_frozen_source_change(self):
        with tempfile.TemporaryDirectory() as temporary:
            suite = Path(temporary)
            (suite/'source.py').write_text('frozen')
            atomic(suite/'provenance.json', {'frozen_sha256': {'source.py':digest(suite/'source.py')}})
            atomic(suite/'checkpoint.json', {'jobs':[]})
            (suite/'source.py').write_text('changed')
            with self.assertRaisesRegex(ValueError, 'Frozen files changed'):
                validate(suite)

    def test_nested_worker_event_retains_kind(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)/'events.jsonl'
            event(path, 'worker_message', message={'kind':'boot', 'name':'R0'})
            saved = json.loads(path.read_text())
            self.assertEqual(saved['kind'], 'worker_message')
            self.assertEqual(saved['message']['kind'], 'boot')

    def test_resume_skips_completed_and_reruns_interrupted_without_overwrite(self):
        with tempfile.TemporaryDirectory() as temporary:
            suite = Path(temporary)
            (suite/'harness').mkdir()
            launcher = suite/'harness/run_8b_baseline.py'
            launcher.write_text('frozen')
            (suite/'done').mkdir()
            (suite/'done/raw.jsonl').write_text('completed raw')
            (suite/'partial').mkdir()
            (suite/'partial/raw.jsonl').write_text('interrupted raw')
            atomic(suite/'provenance.json', {'packages':{}, 'python':'unused',
                  'frozen_sha256':{'harness/run_8b_baseline.py':digest(launcher)}})
            atomic(suite/'checkpoint.json', {'phase':'pilot', 'jobs':[
                {'seed':1, 'status':'completed', 'attempt':'done', 'attempts':['done'],
                 'result_sha256':tree_hashes(suite/'done')},
                {'seed':2, 'status':'running', 'attempt':'partial', 'attempts':['partial']},
                {'seed':3, 'status':'pending', 'attempts':[]}]})
            (suite/'STOP_AFTER_TRIAL').write_text('old pause request')
            seeds = []
            def fake_trial(config, path):
                seeds.append(config['seed'])
                analysis = {'pass':True, 'valid':True, 'errors':[]}
                atomic(path/'analysis.json', analysis)
                return analysis, None
            with patch('run_8b_baseline.__file__', str(launcher)), \
                 patch('run_8b_baseline.importlib.metadata.distributions', return_value=[]), \
                 patch('run_8b_baseline.signal.signal'), \
                 patch('run_8b_baseline.trial_config', side_effect=lambda suite,seed,phase: {'seed':seed}), \
                 patch('run_8b_baseline.run_trial', side_effect=fake_trial), \
                 patch('run_8b_baseline.report'):
                resume(suite)
            state = json.loads((suite/'checkpoint.json').read_text())
            self.assertEqual(seeds, [2,3])
            self.assertEqual(state['jobs'][1]['attempts'], ['partial','seed2_attempt02'])
            self.assertEqual((suite/'partial/raw.jsonl').read_text(), 'interrupted raw')
            self.assertEqual((suite/'done/raw.jsonl').read_text(), 'completed raw')
            self.assertTrue(all(j['status']=='completed' for j in state['jobs']))


if __name__=='__main__':
    unittest.main()

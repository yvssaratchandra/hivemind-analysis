"""Offline numerical fixtures and harmless local process-lifecycle checks; no native peers."""
import json
import contextlib
import io
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import psutil

import run_vera_baseline as runner
from test_announcement_probe import _condition_artifacts


class AllocationTests(unittest.TestCase):
    def setUp(self):
        self.env = dict(SLURM_JOB_ID='123', SLURM_JOB_NUM_NODES='1', SLURM_NTASKS='1',
                        SLURM_CPUS_PER_TASK='4', SLURM_MEM_PER_NODE='8192',
                        SLURM_JOB_CPUS_PER_NODE='4',
                        SLURM_JOB_ACCOUNT='example', SLURM_JOB_PARTITION='vera')

    def test_expected_allocation(self):
        with patch('os.sched_getaffinity', return_value={0, 1, 2, 3}):
            self.assertEqual(runner.allocation(self.env)['SLURM_JOB_ID'], '123')

    def test_observed_vera_memory_is_accepted_and_recorded(self):
        with patch('os.sched_getaffinity', return_value={0, 1, 2, 3}):
            result = runner.allocation({**self.env, 'SLURM_MEM_PER_NODE': '47400'})
        self.assertEqual(result['SLURM_MEM_PER_NODE'], '47400')
        self.assertEqual(result['SLURM_JOB_CPUS_PER_NODE'], '4')

    def test_memory_allowance_does_not_allow_more_cpus(self):
        with self.assertRaises(ValueError):
            runner.allocation({**self.env, 'SLURM_MEM_PER_NODE': '47400',
                               'SLURM_JOB_CPUS_PER_NODE': '64'})

    def test_other_memory_values_still_fail(self):
        for memory in ('', '0', '4096', '32768', '47401', '758400', '8G'):
            with self.subTest(memory=memory), self.assertRaises(ValueError):
                runner.allocation({**self.env, 'SLURM_MEM_PER_NODE': memory})

    def test_rejects_login_node_and_unexpected_resources(self):
        for key, value in [('SLURM_JOB_ID', ''), ('SLURM_JOB_NUM_NODES', '2'),
                           ('SLURM_NTASKS', '4'), ('SLURM_CPUS_PER_TASK', '64'),
                           ('SLURM_MEM_PER_NODE', '32768'), ('SLURM_JOB_ACCOUNT', ''),
                           ('SLURM_JOB_PARTITION', 'gpu'), ('SLURM_JOB_GPUS', '0'),
                           ('SLURM_JOB_CPUS_PER_NODE', '64')]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                runner.allocation({**self.env, key: value})

    def test_rejects_full_node_affinity(self):
        with patch('os.sched_getaffinity', return_value=set(range(64))):
            with self.assertRaises(ValueError):
                runner.allocation(self.env)

    def test_cli_cannot_select_attack_or_repeat(self):
        for args in [('--condition', 'ordering'), ('--seed', '2'), ('--trials', '5')]:
            result = subprocess.run([sys.executable, str(Path(runner.__file__)), *args],
                                    capture_output=True)
            self.assertEqual(result.returncode, 2)


class ValidationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.case = Path(self.temp.name) / 'baseline4'
        self.case.mkdir()
        _condition_artifacts(self.case, 'baseline4')
        shutdown = json.loads((self.case / 'shutdown.json').read_text())
        shutdown.update(forced_cleanup=[], error=None, worker_exitcode=0,
                        controller_audited_after_worker_exit=True)
        (self.case / 'shutdown.json').write_text(json.dumps(shutdown))

    def mutate(self, file, fn):
        path = self.case / file
        value = json.loads(path.read_text())
        fn(value)
        path.write_text(json.dumps(value))

    def test_single_baseline_passes_without_other_conditions(self):
        self.assertTrue(runner.validate_case(self.case))
        self.assertTrue((self.case.parent / 'REPORT.md').exists())

    def test_incorrect_mean_fails(self):
        self.mutate('outputs.json', lambda data: data['A']['final'][0].__setitem__(0, -999))
        self.assertFalse(runner.validate_case(self.case))

    def test_missing_stream_evidence_fails(self):
        for path in self.case.glob('events_*.jsonl'):
            path.unlink()
        self.assertFalse(runner.validate_case(self.case))

    def test_forced_cleanup_fails_even_when_final_survivors_empty(self):
        self.mutate('shutdown.json', lambda data: data.update(forced_cleanup=[123]))
        self.assertFalse(runner.validate_case(self.case))

    def test_worker_failure_fails(self):
        self.mutate('shutdown.json', lambda data: data.update(worker_exitcode=1))
        self.assertFalse(runner.validate_case(self.case))

    def test_unsafe_shutdown_fails(self):
        self.mutate('shutdown.json', lambda data: data.update(safe=False, surviving_pids=[123]))
        self.assertFalse(runner.validate_case(self.case))

    def test_cleanup_warning_fails(self):
        with (self.case / 'controller.jsonl').open('a') as stream:
            stream.write(json.dumps({'kind': 'cleanup_error', 'error': 'example'}) + '\n')
        self.assertFalse(runner.validate_case(self.case))

    def test_missing_shutdown_cannot_pass(self):
        (self.case / 'shutdown.json').unlink()
        with self.assertRaises(FileNotFoundError):
            runner.validate_case(self.case)


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.owned = {}
        self.addCleanup(lambda: runner.cleanup(self.owned, grace=0))
        runner.STOP_SIGNAL = None
        self.addCleanup(setattr, runner, 'STOP_SIGNAL', None)

    def run_command(self, code, seconds=2):
        return runner.bounded([sys.executable, '-c', code], self.directory, 'dummy',
                              seconds, time.monotonic() + 5, self.owned)

    def test_success_and_nonzero_exit(self):
        self.assertEqual(self.run_command('print("ok")')['exitcode'], 0)
        with self.assertRaises(RuntimeError):
            self.run_command('raise SystemExit(3)')

    def test_timeout_cleans_sleeping_descendant_but_not_unrelated_process(self):
        sentinel = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
        self.addCleanup(lambda: sentinel.poll() is None and sentinel.kill())
        self.addCleanup(lambda: sentinel.poll() is not None and sentinel.wait())
        with self.assertRaises(RuntimeError):
            self.run_command('import subprocess,sys,time; '
                             'subprocess.Popen([sys.executable,"-c","import time; time.sleep(30)"]); '
                             'time.sleep(30)', seconds=.5)
        self.assertGreaterEqual(len(self.owned), 2)
        self.assertFalse(runner.live_owned(self.owned))
        self.assertIsNone(sentinel.poll())
        sentinel.terminate()
        sentinel.wait(timeout=5)

    def test_failure_before_shutdown_retains_and_cleans_orphan(self):
        with self.assertRaises(RuntimeError):
            self.run_command('import subprocess,sys,time; '
                             'subprocess.Popen([sys.executable,"-c","import time; time.sleep(30)"]); '
                             'time.sleep(.3); raise SystemExit(2)')
        self.assertGreaterEqual(len(self.owned), 2)
        audit = runner.cleanup(self.owned, grace=0)
        self.assertTrue(audit['forced_cleanup'])
        self.assertEqual(audit['surviving_pids'], [])

    def test_cancellation_cleans_owned_process(self):
        runner.STOP_SIGNAL = signal.SIGTERM
        with self.assertRaises(RuntimeError):
            self.run_command('import time; time.sleep(30)')
        self.assertFalse(runner.live_owned(self.owned))

    def test_output_limit_stops_worker(self):
        with patch.object(runner, 'OUTPUT_LIMIT', 1024), self.assertRaises(RuntimeError):
            self.run_command('print("x" * 10000); import time; time.sleep(30)')
        self.assertFalse(runner.live_owned(self.owned))

    def test_stale_pid_does_not_target_unrelated_process(self):
        proc = psutil.Process(os.getpid())
        self.assertEqual(runner.live_owned({proc.pid: proc.create_time() - 1}), [])


class ControllerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.calls = []

    def invoke(self, *, preflight=False, fail=None):
        def fake_bounded(command, directory, name, seconds, deadline, owned):
            self.calls.append(name)
            (directory / f'{name}.log').write_text('offline fixture\n')
            if name == fail:
                raise RuntimeError('injected stage failure')
            if name == 'worker':
                case = directory / 'baseline4'
                case.mkdir()
                _condition_artifacts(case, 'baseline4')
            if name == 'audit':
                path = directory / 'baseline4/shutdown.json'
                data = json.loads(path.read_text())
                data.update(forced_cleanup=[], error=None, worker_exitcode=0,
                            controller_audited_after_worker_exit=True)
                path.write_text(json.dumps(data))
            if name == 'analysis':
                if not runner.validate_case(directory / 'baseline4'):
                    raise RuntimeError('fixture did not validate')
            return {'exitcode': 0, 'reason': None}
        argv = ['run_vera_baseline.py', '--package-root', str(self.root),
                '--dependencies', str(self.root), '--output-root', str(self.root)]
        if preflight:
            argv.append('--preflight-only')
        with patch.object(sys, 'argv', argv), patch.object(sys, 'version_info', (3, 11)), \
                patch.object(sys, 'prefix', '/example-venv'), \
                patch.dict(os.environ, {'SLURM_JOB_ID': '123'}), \
                patch.object(runner, 'allocation', return_value={}), \
                patch.object(runner, 'bounded', side_effect=fake_bounded), \
                contextlib.redirect_stdout(io.StringIO()):
            return runner.main()

    def test_success_finalizes_inventory_and_baseline_status(self):
        self.assertEqual(self.invoke(), 0)
        self.assertEqual(self.calls, ['commit', 'working_tree', 'requirements',
                                      'preflight', 'worker', 'audit', 'analysis'])
        path = self.root / 'vera_baseline_123'
        self.assertEqual(json.loads((path / 'status.json').read_text())['status'], 'passed')
        self.assertTrue((path / 'artifact_inventory.json').exists())

    def test_preflight_never_starts_worker(self):
        self.assertEqual(self.invoke(preflight=True), 0)
        self.assertNotIn('worker', self.calls)

    def test_failed_preflight_cannot_start_worker_or_pass(self):
        self.assertEqual(self.invoke(fail='preflight'), 1)
        self.assertNotIn('worker', self.calls)
        path = self.root / 'vera_baseline_123'
        self.assertEqual(json.loads((path / 'status.json').read_text())['status'], 'failed')

    def test_duplicate_run_rejected_before_any_subprocess(self):
        (self.root / 'vera_baseline_123').mkdir()
        with self.assertRaises(FileExistsError):
            self.invoke()
        self.assertEqual(self.calls, [])


if __name__ == '__main__':
    unittest.main()

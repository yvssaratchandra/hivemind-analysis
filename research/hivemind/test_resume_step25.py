"""Offline regression tests for the Step 25 checkpoint and resume manager."""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
import random
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

import resume_step25 as manager


def write_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def native_report(path):
    return {"control_plane_pass": True, "trial": Path(path).name}


def choice_report(path):
    return {
        "step24_control_plane_pass": True,
        "trace_integrity_pass": True,
        "control_plane_pass": True,
        "trial": Path(path).name,
    }


class ResumeStep25Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="step25_resume_test_")
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.suite = self.base / "step25_fixture"
        self.suite.mkdir()
        self.jobs = [
            {"case": {"name": name, "mode": "refresh", "lifetime": lifetime},
             "seed": seed, "duration": duration}
            for name, lifetime, seed, duration in (
                ("fixture_a", 1.0, 2501, 10),
                ("fixture_b", 2.0, 2502, 11),
                ("fixture_c", 3.0, 2503, 12),
            )
        ]
        self.write_manifest(self.suite, self.jobs)
        self.write_provenance(self.suite)

    @staticmethod
    def write_manifest(suite_dir, jobs):
        write_json(suite_dir / "suite.json", {
            "phase": "confirm",
            "run_kind": "confirm",
            "outcome": "grouping",
            "instrumentation": "detailed",
            "order_seed": 250025,
            "jobs": jobs,
        })

    @staticmethod
    def write_provenance(suite_dir):
        write_json(suite_dir / "provenance.json", {
            "executable": sys.executable,
            "harness_sha256": {},
            "source_sha256": {},
        })

    @staticmethod
    def trial_dir(suite_dir, job):
        return suite_dir / f"{job['case']['name']}_s{job['seed']}"

    def write_trial(self, job, *, suite_dir=None, derived=True, trace_closed=True):
        suite_dir = suite_dir or self.suite
        path = self.trial_dir(suite_dir, job)
        path.mkdir(parents=True, exist_ok=True)
        write_json(path / "config.json", {
            "case": job["case"],
            "seed": job["seed"],
            "duration": job["duration"],
            "outcome": "grouping",
            "instrumentation": "detailed",
            "experiment": "step25",
        })
        event = {"kind": "trace_closed" if trace_closed else "step_start"}
        (path / "events.jsonl").write_text(json.dumps(event) + "\n", encoding="utf-8")
        write_json(path / "summary.json", {"error": None, "pending": []})
        if derived:
            write_json(path / "analysis.json", native_report(path))
            write_json(path / "choices.json", choice_report(path))
        return path

    @staticmethod
    def analyzers():
        return native_report, choice_report

    @staticmethod
    def hashes(paths):
        return {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}

    def validate(self, suite_dir=None, **kwargs):
        native, choices = self.analyzers()
        return manager.validate_checkpoint(
            suite_dir or self.suite,
            analyze_trial_fn=native,
            analyze_choices_fn=choices,
            verify_sources=False,
            require_original_design=False,
            **kwargs,
        )

    def test_checkpoint_recovers_both_derived_files_once_and_preserves_raw_hashes(self):
        job = self.jobs[0]
        trial = self.write_trial(job, derived=False)
        raw_paths = [trial / name for name in manager.RAW_FILES]
        raw_before = self.hashes(raw_paths)
        suite_metadata_before = self.hashes([self.suite / "suite.json", self.suite / "provenance.json"])
        real_replace = os.replace

        with patch.object(manager.os, "replace", wraps=real_replace) as atomic_replace:
            checkpoint = manager.checkpoint_suite(
                self.suite,
                analyze_trial_fn=native_report,
                analyze_choices_fn=choice_report,
                verify_sources=False,
                require_original_design=False,
            )

        self.assertEqual(checkpoint["recovered_choice_trials"], [trial.name])
        self.assertEqual(checkpoint["counts"]["completed_recovered_choices"], 1)
        self.assertEqual(checkpoint["completed_job_keys"], [manager._job_key(job)])
        self.assertEqual(checkpoint["jobs"][0]["status"], "completed_recovered_choices")
        self.assertEqual(json.loads((trial / "analysis.json").read_text()), native_report(trial))
        self.assertEqual(json.loads((trial / "choices.json").read_text()), choice_report(trial))
        self.assertEqual(self.hashes(raw_paths), raw_before)
        self.assertEqual(self.hashes([self.suite / "suite.json", self.suite / "provenance.json"]),
                         suite_metadata_before)
        for name, digest in raw_before.items():
            self.assertEqual(checkpoint["jobs"][0]["file_sha256"][Path(name).name], digest)
        self.assertTrue(any(Path(call.args[1]).name == "checkpoint.json"
                            for call in atomic_replace.call_args_list))
        self.assertEqual(list(self.suite.rglob("*.tmp")), [])

    def test_completed_trial_hash_change_after_checkpoint_is_rejected(self):
        job = self.jobs[0]
        trial = self.write_trial(job)
        manager.checkpoint_suite(
            self.suite,
            analyze_trial_fn=native_report,
            analyze_choices_fn=choice_report,
            verify_sources=False,
            require_original_design=False,
        )
        with (trial / "events.jsonl").open("a", encoding="utf-8") as stream:
            stream.write('{"kind":"tampered"}\n')

        with self.assertRaisesRegex(ValueError, "completed-trial file changed"):
            self.validate()

    def test_malformed_complete_triplet_and_manifest_config_drift_are_blocked(self):
        self.write_trial(self.jobs[0], derived=True, trace_closed=False)
        state = self.validate()
        self.assertEqual(state["jobs"][0]["status"], "blocked")
        self.assertIn("trace", state["jobs"][0]["error"])

        # Use a fresh suite so the first blocked report cannot affect this case.
        drift_suite = self.base / "config_drift"
        drift_suite.mkdir()
        self.write_manifest(drift_suite, [self.jobs[1]])
        self.write_provenance(drift_suite)
        trial = self.write_trial(self.jobs[1], suite_dir=drift_suite)
        config = json.loads((trial / "config.json").read_text())
        config["seed"] += 1
        write_json(trial / "config.json", config)
        native, choices = self.analyzers()
        state = manager.validate_checkpoint(
            drift_suite, analyze_trial_fn=native, analyze_choices_fn=choices,
            verify_sources=False, require_original_design=False,
        )
        self.assertEqual(state["jobs"][0]["status"], "blocked")
        self.assertIn("trial config differs", state["jobs"][0]["error"])

    def test_source_snapshot_and_provenance_digest_drift_are_rejected(self):
        preserved_suite = self.base / "source_hash_preservation"
        preserved_suite.mkdir()
        self.write_manifest(preserved_suite, [self.jobs[0]])
        live_root, project_root, source_provenance, _source_path = self.make_source_fixture(
            preserved_suite
        )
        source_files_before = self.hashes(
            [preserved_suite / "suite.json", preserved_suite / "provenance.json"]
        )
        with patch.object(manager, "ROOT", live_root), patch.object(
                manager, "PROJECT_ROOT", project_root):
            preserved = manager.checkpoint_suite(
                preserved_suite,
                analyze_trial_fn=native_report,
                analyze_choices_fn=choice_report,
                verify_sources=True,
                require_original_design=False,
            )
        self.assertEqual(preserved["source_digests"]["harness_sha256"],
                         source_provenance["harness_sha256"])
        self.assertEqual(preserved["source_digests"]["native_source_sha256"],
                         source_provenance["source_sha256"])
        self.assertEqual(self.hashes([preserved_suite / "suite.json",
                                      preserved_suite / "provenance.json"]), source_files_before)

        for label in ("source", "provenance"):
            with self.subTest(label=label):
                suite = self.base / f"drift_{label}"
                suite.mkdir()
                self.write_manifest(suite, [self.jobs[0]])
                live, project, provenance, source_path = self.make_source_fixture(suite)
                if label == "source":
                    source_path.write_bytes(source_path.read_bytes() + b"\n# source drift\n")
                else:
                    provenance["harness_sha256"]["step25_choice_measurement.py"] = "0" * 64
                    write_json(suite / "provenance.json", provenance)
                with self.assertRaisesRegex(ValueError, "hash mismatch"):
                    with patch.object(manager, "ROOT", live), patch.object(
                            manager, "PROJECT_ROOT", project):
                        manager.validate_checkpoint(
                            suite,
                            analyze_trial_fn=native_report,
                            analyze_choices_fn=choice_report,
                            verify_sources=True,
                            require_original_design=False,
                        )

    def make_source_fixture(self, suite):
        live_root = self.base / f"{suite.name}_live"
        project_root = self.base / f"{suite.name}_project"
        runtime_source = self.base / f"{suite.name}_runtime"
        live_root.mkdir()
        (suite / "harness").mkdir()
        runtime_source.mkdir()
        project_native = project_root / "hivemind"
        project_native.mkdir(parents=True)

        harness_sha256 = {}
        for name in sorted(manager.REQUIRED_HARNESS):
            data = f"test source snapshot: {name}\n".encode()
            (live_root / name).write_bytes(data)
            (suite / "harness" / name).write_bytes(data)
            harness_sha256[name] = hashlib.sha256(data).hexdigest()

        relative_source = "resume_fixture_source.py"
        source_bytes = b"NATIVE_RUNTIME_FIXTURE = True\n"
        source_path = runtime_source / relative_source
        source_path.write_bytes(source_bytes)
        (project_native / relative_source).write_bytes(source_bytes)
        source_provenance = {
            "executable": sys.executable,
            "source": str(runtime_source),
            "source_sha256": {relative_source: hashlib.sha256(source_bytes).hexdigest()},
            "harness_sha256": harness_sha256,
        }
        write_json(suite / "provenance.json", source_provenance)
        return live_root, project_root, source_provenance, source_path

    def test_original_manifest_randomized_job_order_cannot_change(self):
        policies = [
            {"name": name, "mode": "refresh", "minimum": 4, "target": 4,
             "frequency": 1.0, "lifetime": upper, "nominal_lower": lower,
             "nominal_upper": upper}
            for name, lower, upper in (
                ("range_0_1", 0.0, 1.0),
                ("range_025_125", 0.25, 1.25),
                ("range_1_2", 1.0, 2.0),
                ("range_2_3", 2.0, 3.0),
                ("range_3_4", 3.0, 4.0),
            )
        ]
        policies.extend((
            {"name": "expire_2", "mode": "expire", "minimum": 4, "target": 4,
             "lifetime": 2.0},
            {"name": "passive_stock", "mode": "passive", "minimum": 4, "target": 4,
             "lifetime": 5.0},
            {"name": "stock", "mode": "stock", "minimum": 4, "target": 4,
             "lifetime": 5.0},
        ))
        jobs = [{"case": case, "seed": seed, "duration": 60}
                for seed in range(2501, 2511) for case in policies]
        random.Random(250025).shuffle(jobs)
        sigma = math.sqrt(math.log(1 + 0.6 ** 2))
        manifest = {
            "phase": "confirm", "run_kind": "confirm", "outcome": "grouping",
            "instrumentation": "detailed", "order_seed": 250025,
            "training_delay": {"mu": math.log(1.5) - sigma ** 2 / 2,
                               "sigma": sigma, "lower": 0.1, "upper": 4.0},
            "jobs": jobs,
        }
        write_json(self.suite / "suite.json", manifest)
        native, choices = self.analyzers()
        accepted = manager.validate_checkpoint(
            self.suite, analyze_trial_fn=native, analyze_choices_fn=choices,
            verify_sources=False, require_original_design=True,
        )
        self.assertEqual(accepted["counts"]["pending"], 80)

        manifest["jobs"][0], manifest["jobs"][1] = manifest["jobs"][1], manifest["jobs"][0]
        write_json(self.suite / "suite.json", manifest)
        with self.assertRaisesRegex(ValueError, "order|manifest"):
            manager.validate_checkpoint(
                self.suite, analyze_trial_fn=native, analyze_choices_fn=choices,
                verify_sources=False, require_original_design=True,
            )

    def test_dry_run_is_read_only_and_never_enters_runtime(self):
        before = self.hashes(path for path in self.suite.rglob("*") if path.is_file())
        native, choices = self.analyzers()
        with patch.object(manager, "_validate_runtime", side_effect=AssertionError("runtime entered")):
            report = manager.dry_run(
                self.suite, analyze_trial_fn=native, analyze_choices_fn=choices,
                verify_sources=False, require_original_design=False,
            )
        after = self.hashes(path for path in self.suite.rglob("*") if path.is_file())
        self.assertEqual(report["counts"]["pending"], len(self.jobs))
        self.assertEqual(report["would_archive_interrupted"], [])
        self.assertEqual(before, after)
        self.assertFalse((self.suite / "checkpoint.json").exists())
        self.assertFalse((self.suite / "resume_provenance.json").exists())

    def test_resume_skips_completed_archives_partial_sibling_and_reruns_remaining_in_order(self):
        complete_job, interrupted_job, pending_job = self.jobs
        complete_trial = self.write_trial(complete_job)
        completed_before = self.hashes(path for path in complete_trial.iterdir() if path.is_file())
        interrupted_trial = self.write_trial(interrupted_job, derived=False, trace_closed=False)
        sentinel = interrupted_trial / "partial.raw"
        sentinel.write_bytes(b"partial native trace bytes")
        sentinel_before = hashlib.sha256(sentinel.read_bytes()).hexdigest()
        archived_raw_before = {
            name: hashlib.sha256((interrupted_trial / name).read_bytes()).hexdigest()
            for name in manager.RAW_FILES
        }

        calls = []

        def fake_runner(case, seed, path, duration, outcome="grouping"):
            calls.append((case, seed, Path(path), duration, outcome))
            path.mkdir(parents=True, exist_ok=True)
            write_json(path / "config.json", {
                "case": case, "seed": seed, "duration": duration,
                "outcome": outcome, "instrumentation": "detailed", "experiment": "step25",
            })
            (path / "events.jsonl").write_text('{"kind":"trace_closed"}\n', encoding="utf-8")
            write_json(path / "summary.json", {"error": None, "pending": []})
            write_json(path / "analysis.json", native_report(path))
            return native_report(path)

        fake_runtime = {
            "python": "test-python", "torch": "test-torch", "source": "test-source",
            "executable": sys.executable, "current_git_head": "test-commit",
            "source_sha256": {}, "script_sha256": "test", "p2pd_sha256": "test",
            "packages": [],
        }
        fake_measurement = types.SimpleNamespace(sweep=types.SimpleNamespace(run_trial=fake_runner))
        fake_analyze_module = types.ModuleType("analyze_step25_choices")
        fake_analyze_module.analyse_suite_choices = lambda *_args, **_kwargs: {"control_plane_pass": True}
        native, choices = self.analyzers()

        with patch.object(manager, "_validate_runtime", return_value=(fake_measurement, fake_runtime)), \
             patch.dict(sys.modules, {"analyze_step25_choices": fake_analyze_module}):
            state = manager.resume_suite(
                self.suite,
                analyze_trial_fn=native,
                analyze_choices_fn=choices,
                verify_sources=False,
                require_original_design=False,
            )

        self.assertEqual([call[:2] for call in calls], [
            (interrupted_job["case"], interrupted_job["seed"]),
            (pending_job["case"], pending_job["seed"]),
        ])
        self.assertEqual([call[2] for call in calls], [
            self.trial_dir(self.suite, interrupted_job),
            self.trial_dir(self.suite, pending_job),
        ])
        self.assertEqual([(call[3], call[4]) for call in calls], [
            (interrupted_job["duration"], "grouping"),
            (pending_job["duration"], "grouping"),
        ])
        self.assertEqual(self.hashes(path for path in complete_trial.iterdir() if path.is_file()),
                         completed_before)
        interrupted_row = state["jobs"][1]
        archive = Path(interrupted_row["archive"]["path"]).resolve()
        self.assertEqual(archive.parent, self.suite.parent / f"{self.suite.name}_interrupted")
        self.assertNotIn(self.suite.resolve(), archive.parents)
        self.assertEqual(hashlib.sha256((archive / "partial.raw").read_bytes()).hexdigest(),
                         sentinel_before)
        archived_raw_after = {
            name: hashlib.sha256((archive / name).read_bytes()).hexdigest()
            for name in manager.RAW_FILES
        }
        self.assertEqual(archived_raw_after, archived_raw_before)
        self.assertTrue(self.trial_dir(self.suite, interrupted_job).is_dir())
        self.assertEqual([row["status"] for row in state["jobs"]],
                         ["completed", "completed", "completed"])
        self.assertEqual(state["counts"]["completed"], 3)

    def test_stop_request_finishes_current_job_and_next_resume_continues(self):
        calls = []
        stop_path = self.suite / "STOP_AFTER_CURRENT"

        def fake_runner(case, seed, path, duration, outcome="grouping"):
            calls.append((case, seed, Path(path), duration, outcome))
            path.mkdir(parents=True, exist_ok=True)
            write_json(path / "config.json", {
                "case": case, "seed": seed, "duration": duration,
                "outcome": outcome, "instrumentation": "detailed", "experiment": "step25",
            })
            (path / "events.jsonl").write_text('{"kind":"trace_closed"}\n', encoding="utf-8")
            write_json(path / "summary.json", {"error": None, "pending": []})
            write_json(path / "analysis.json", native_report(path))
            if len(calls) == 1:
                stop_path.write_text("stop after this job\n", encoding="utf-8")
            return native_report(path)

        fake_analyze_module = types.ModuleType("analyze_step25_choices")
        fake_analyze_module.analyse_suite_choices = lambda *_args, **_kwargs: {
            "control_plane_pass": True
        }
        native, choices = self.analyzers()
        with patch.dict(sys.modules, {"analyze_step25_choices": fake_analyze_module}):
            first = manager.resume_suite(
                self.suite, run_trial=fake_runner,
                analyze_trial_fn=native, analyze_choices_fn=choices,
                verify_sources=False, require_original_design=False,
            )
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][:2], (self.jobs[0]["case"], self.jobs[0]["seed"]))
        self.assertEqual(calls[0][3:], (self.jobs[0]["duration"], "grouping"))
        self.assertEqual(first["last_resume_event"], "stopped_by_request")
        self.assertTrue(stop_path.exists())
        first_trial = self.trial_dir(self.suite, self.jobs[0])
        first_trial_before = self.hashes(path for path in first_trial.iterdir() if path.is_file())

        with patch.dict(sys.modules, {"analyze_step25_choices": fake_analyze_module}):
            second = manager.resume_suite(
                self.suite, run_trial=fake_runner,
                analyze_trial_fn=native, analyze_choices_fn=choices,
                verify_sources=False, require_original_design=False,
            )
        self.assertFalse(stop_path.exists())
        self.assertEqual([call[:2] for call in calls], [
            (job["case"], job["seed"]) for job in self.jobs
        ])
        self.assertEqual(self.hashes(path for path in first_trial.iterdir() if path.is_file()),
                         first_trial_before)
        self.assertEqual([row["status"] for row in second["jobs"]],
                         ["completed", "completed", "completed"])
        self.assertEqual(second["counts"]["completed"], 3)


if __name__ == "__main__":
    unittest.main()

"""Safety and evidence checks for the control-message runner, not LocalAI tests."""
import argparse
import datetime as dt
import io
import json
from pathlib import Path
import tempfile
import struct
import threading
import unittest
from unittest.mock import Mock, patch

import run_control_messages as lab


class CaptureChecks(unittest.TestCase):
    def test_full_peer_convergence_can_retry_a_transient_stale_view(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(lab, "ROOT", Path(folder)):
            args = argparse.Namespace(label="convergence", discovery_timeout=1, interval=0.001)
            runner = lab.ControlCapture(args)
            fresh = {"fresh_hosts": {runner.hosts[0]: [runner.hosts[0]]}}
            stale = {"fresh_hosts": {runner.hosts[0]: []}}
            with patch.object(runner, 'sample', side_effect=[fresh, stale, fresh]):
                result = runner.stage('bootstrap', [0], 0)
            self.assertTrue(result['all_peer_membership_match'])
            self.assertEqual(result['all_peer_membership_checks'], 2)

    def test_socket_logs_preserve_split_utf8_and_redact_split_token(self):
        def frame(channel, data):
            return bytes([channel, 0, 0, 0]) + struct.pack(">I", len(data)) + data
        ready = threading.Event()
        class Response(io.BytesIO):
            status = 200
            def read(self, size=-1):
                ready.wait(2)
                return super().read(size)
        body = '{"event":"ok","text":"é","token":"secret-token"}\n'.encode()
        response = Response(frame(1, body[:24]) + frame(2, b'diagnostic\n') + frame(1, body[24:39]) + frame(1, body[39:]))
        connection = Mock(sock=Mock())
        connection.getresponse.return_value = response
        with tempfile.TemporaryDirectory() as folder, patch.object(lab, "ROOT", Path(folder)):
            runner = lab.ControlCapture(argparse.Namespace(label="socket-test"))
            runner.token, runner.docker_socket = "secret-token", "/unused"
            with patch.object(lab, "UnixDockerConnection", return_value=connection):
                runner.follow("peer", "observer.jsonl", structured=True)
                runner.expect_log_end("peer")
                ready.set()
                runner.streams[0][2].join(3)
            self.assertEqual(runner.capture_errors, [])
            captured = json.loads((runner.out / 'observer.jsonl').read_text())
            self.assertEqual(captured['text'], 'é')
            self.assertEqual(captured['token'], '<local-token-redacted>')
            self.assertEqual((runner.out / 'observer.stderr.log').read_text(), 'diagnostic\n')

    def test_unexpected_log_eof_marks_capture_incomplete(self):
        response = io.BytesIO()
        response.status = 200
        connection = Mock(sock=Mock())
        connection.getresponse.return_value = response
        with tempfile.TemporaryDirectory() as folder, patch.object(lab, "ROOT", Path(folder)):
            runner = lab.ControlCapture(argparse.Namespace(label="cut-stream"))
            runner.docker_socket = '/unused'
            with patch.object(lab, "UnixDockerConnection", return_value=connection):
                runner.follow('peer', 'observer.jsonl', structured=True)
                runner.streams[0][2].join(3)
            self.assertEqual(len(runner.capture_errors), 1)
            self.assertIn('Unexpected EOF', runner.capture_errors[0]['error'])

    def test_forced_log_cancellation_marks_capture_incomplete(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(lab, "ROOT", Path(folder)):
            runner = lab.ControlCapture(argparse.Namespace(label="hung-stream"))
            thread, wire = Mock(), Mock()
            thread.is_alive.return_value = True
            runner.streams = [('peer', wire, thread, threading.Event(), threading.Event())]
            runner.cleanup()
            self.assertEqual(len(runner.capture_errors), 2)
            wire.shutdown.assert_called_once()

    def test_docker_logs_since_uses_api_unix_seconds(self):
        self.assertEqual(lab.docker_log_since("1970-01-01T00:00:01.125000+00:00"), "1.125000")
        self.assertEqual(lab.docker_log_since("1970-01-01T00:00:01Z"), "1.000000")

    def test_docker_log_demultiplexing_keeps_stdout_and_stderr_exact(self):
        def frame(channel, data):
            return bytes([channel, 0, 0, 0]) + struct.pack(">I", len(data)) + data
        stdout, stderr = b'{"event":"captured"}\n', b'library diagnostic\n'
        stream = io.BytesIO(frame(1, stdout) + frame(2, stderr))
        self.assertEqual(list(lab.docker_log_frames(stream)), [(1, stdout), (2, stderr)])

    def test_docker_log_capture_rejects_truncation_and_invalid_channels(self):
        for data in (b'\x01\x00', b'\x01\x00\x00\x00\x00\x00\x00\x05ab',
                     b'\x03\x00\x00\x00\x00\x00\x00\x00'):
            with self.assertRaises(ValueError):
                list(lab.docker_log_frames(io.BytesIO(data)))

    def test_budget_reserves_desktop_observer_and_gateway(self):
        self.assertEqual(lab.choose_peer_count(23 * 1024**3, 256), 64)
        self.assertEqual(lab.choose_peer_count(8 * 1024**3, 256), 13)
        self.assertEqual(lab.choose_peer_count(23 * 1024**3, 256, requested=4), 4)
        with self.assertRaises(RuntimeError):
            lab.choose_peer_count(4 * 1024**3, 256)

    def test_options_are_docker_flags_before_image_and_private(self):
        peer = lab.control_peer_args("p1", "h1", "internal", "image-id", 256, "token", 0.25)
        gateway = lab.gateway_args("gw", "internal", "image-id", 256, "token")
        self.assertEqual(peer[peer.index("image-id")+1:], ["run", "--p2p", "--federated", "--debug"])
        self.assertEqual(gateway[gateway.index("image-id")+1:], ["federated"])
        self.assertIn("TOKEN=token", gateway[:gateway.index("image-id")])
        self.assertIn("LOCALAI_P2P_LOGLEVEL=debug", peer[:peer.index("image-id")])
        for args in [peer, gateway]:
            self.assertIn("LOCALAI_P2P_DISABLE_DHT=true", args)
            self.assertEqual(args[args.index("--memory")+1], "256m")
            for forbidden in ["--privileged", "--gpus", "--device", "-v", "--mount", "-p", "--publish"]:
                self.assertNotIn(forbidden, args)

    def test_stale_and_future_ads_dont_become_fresh_members(self):
        now = dt.datetime.now(dt.timezone.utc)
        records = [{"ID": host + "-service", "LastSeen": (now - dt.timedelta(seconds=age)).isoformat()}
                   for host, age in [("h1", 10), ("h2", 40), ("h3", -60)]]
        records.append(records[0])
        self.assertEqual(lab.fresh_hostnames({"federated_nodes": records}, ["h1", "h2", "h3"], now), ["h1"])

    def test_denied_docker_does_not_start_any_container(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(lab, "ROOT", Path(folder)):
            args = argparse.Namespace(label="denied")
            runner = lab.ControlCapture(args)
            called = []
            def deny(argv, timeout=40):
                called.append(argv)
                return {"argv": argv, "exit_code": 1, "stdout": "", "stderr": "denied"}
            with patch.object(lab, "command", side_effect=deny):
                self.assertEqual(runner.run(), 1)
            self.assertEqual(called, [["docker", "info", "--format", "{{.ServerVersion}}"]])
            data = json.loads((runner.out / "summary.json").read_text())
            self.assertEqual(data["status"], "failed")
            self.assertEqual(data["cleanup"], [])

    def test_token_redaction_and_unique_labels(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(lab, "ROOT", Path(folder)):
            args = argparse.Namespace(label="private")
            runner = lab.ControlCapture(args)
            runner.token = "secret-token"
            runner.append("commands.jsonl", {"argv": ["TOKEN=secret-token"]})
            runner.save("config.json", {"token": "secret-token"})
            self.assertNotIn("secret-token", (runner.out / "commands.jsonl").read_text())
            self.assertNotIn("secret-token", (runner.out / "config.json").read_text())
            with self.assertRaises(FileExistsError):
                lab.ControlCapture(args)

    def test_completed_stage_records_valid_phase_end(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(lab, "ROOT", Path(folder)):
            args = argparse.Namespace(label="complete", discovery_timeout=1, interval=0.001)
            runner = lab.ControlCapture(args)
            snapshot = {"fresh_hosts": {runner.hosts[0]: [runner.hosts[0]]}}
            with patch.object(runner, "sample", return_value=snapshot):
                result = runner.stage("bootstrap", [0], 0)
            self.assertTrue(result["all_peer_membership_match"])
            events = [json.loads(line) for line in (runner.out / "phases.jsonl").read_text().splitlines()]
            self.assertEqual(events[-1]["action"], "end")
            self.assertEqual(events[-1]["phase"], "bootstrap")


if __name__ == "__main__":
    unittest.main()

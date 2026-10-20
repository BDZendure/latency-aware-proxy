import csv
import io
import unittest
import tempfile
import json
from pathlib import Path
from unittest.mock import patch, Mock
from types import SimpleNamespace

from runner import OUTCOMES, audit_records, docker_command, validate_summary, cleanup_processes, scenario_config, DockerProcess, prepare_output, summarize


class AccountingTests(unittest.TestCase):
    def summary(self):
        outcomes = dict.fromkeys(OUTCOMES, 0)
        outcomes.update(success=1, generator_miss=1)
        return {"offered": 2, "outcomes": outcomes,
                "success_latency_count": 1, "all_latency_count": 1,
                "warmup": {"offered": 0, "outcomes": dict.fromkeys(OUTCOMES, 0)}}

    def test_conservation(self):
        validate_summary(self.summary())
        broken = self.summary()
        broken["offered"] += 1
        with self.assertRaises(ValueError):
            validate_summary(broken)

    def test_streamed_records_match_summary(self):
        source = io.StringIO("id,phase,outcome,status,scheduled_ms,dispatch_ms,completion_ms,cost\n"
                             "0,measurement,success,200,0,0.1,2.1,light\n"
                             "1,measurement,generator_miss,0,1,,2,light\n")
        audit_records(source, self.summary())
        source.seek(0)
        broken = self.summary()
        broken["outcomes"]["success"] = 2
        with self.assertRaises(ValueError):
            audit_records(source, broken)

    def test_docker_roles_have_equal_explicit_limits(self):
        command = docker_command("bench-test", "proxy", "image", ["/app/proxy"], 2, "256m", "/tmp/run")
        self.assertIn("--cpus=2", command)
        self.assertIn("--memory=256m", command)
        self.assertIn("--memory-swap=256m", command)
        self.assertIn("--network=bench-test", command)

    def test_duplicate_record_ids_are_rejected(self):
        source = io.StringIO("id,phase,outcome,status,scheduled_ms,dispatch_ms,completion_ms,cost\n"
                             "0,measurement,success,200,0,0.1,2.1,light\n"
                             "0,measurement,generator_miss,0,1,,2,light\n")
        with self.assertRaises(ValueError):
            audit_records(source, self.summary())

    def test_cleanup_attempts_every_role_and_network(self):
        stopped = []
        class Role:
            def __init__(self, name):
                self.name = name
            def stop(self):
                stopped.append(self.name)
                if self.name == "generator":
                    raise RuntimeError("already gone")
        roles = [Role("backend"), Role("proxy"), Role("generator")]
        with patch("runner.command_output") as command:
            with self.assertRaises(RuntimeError):
                cleanup_processes(roles, "network")
            self.assertEqual(stopped, ["generator", "proxy", "backend"])
            command.assert_called_once_with(["docker", "network", "rm", "network"])

    def test_rate_scaling_does_not_change_backend_work(self):
        base, rate, heavy = scenario_config("mixed", 12, 1, 1)
        lower, low_rate, low_heavy = scenario_config("mixed", 12, 1, 1, rate_scale=0.5)
        self.assertEqual((base, heavy), (lower, low_heavy))
        self.assertEqual(rate / 2, low_rate)

    def test_docker_stats_failure_cannot_prevent_cleanup(self):
        process = object.__new__(DockerProcess)
        process.name, process.resource, process.observed = "role", None, {}
        process.sample = Mock(side_effect=TimeoutError("stats"))
        process.wait = Mock(side_effect=lambda _: setattr(process, "resource", {"exit_code": 0}))
        with patch("runner.command_output") as command:
            process.stop()
            calls = [call.args[0] for call in command.call_args_list]
            self.assertIn(["docker", "stop", "--time=6", "role"], calls)
            self.assertIn(["docker", "rm", "--force", "role"], calls)

    def test_docker_constructor_has_no_unowned_mutations(self):
        args = SimpleNamespace(image="fixed", cpus=2, memory="256m", docker_endpoint="unix:///socket", docker_api_version="1.52")
        with patch("runner.command_output") as command:
            process = DockerProcess("network", "proxy", ["/app/proxy"], "/tmp/run", args)
            command.assert_not_called()
            process.start()
            command.assert_called_once_with(process.command)

    def test_existing_archive_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            existing = root / "provenance.json"
            existing.write_text("original")
            with self.assertRaises(FileExistsError):
                prepare_output(root)
            self.assertEqual(existing.read_text(), "original")
            fresh = root / "fresh"
            prepare_output(fresh)
            self.assertEqual(list(fresh.iterdir()), [])

    def test_comparison_preserves_misses_across_replicates(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            for i, misses in enumerate((0, 0, 1)):
                trial = root / str(i)
                trial.mkdir()
                summary = self.summary()
                summary.update(outcomes=dict.fromkeys(OUTCOMES, 0), throughput=2-misses,
                               success_latency_count=2-misses, all_latency_count=2-misses,
                               success_latency_ms={"p50": 1, "p95": 2, "p99": 3})
                summary["outcomes"].update(success=2-misses, generator_miss=misses)
                (trial / "summary.json").write_text(json.dumps(summary))
                (trial / "metadata.json").write_text(json.dumps({"scenario": "steady", "policy": "round-robin"}))
                (trial / "resources.json").write_text(json.dumps({"proxy": {"cpu_seconds": .1, "peak_rss_bytes": 1024}}))
            summarize(root)
            lines = (root / "comparison.md").read_text().splitlines()
            header = next(line for line in lines if line.startswith("| Scenario"))
            row = next(line for line in lines if line.startswith("| steady"))
            cells = lambda line: [cell.strip() for cell in line.split("|")[1:-1]]
            values = dict(zip(cells(header), cells(row)))
            self.assertEqual(values["Gen missed"], "1")


if __name__ == "__main__":
    unittest.main()

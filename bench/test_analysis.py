import unittest
import csv
import gzip
import io
import json
import tempfile
from pathlib import Path

from analyze import analyze, phase_name, quantile
from runner import OUTCOMES, audit_records


class CohortTests(unittest.TestCase):
    def test_half_open_fault_and_recovery_windows(self):
        self.assertEqual(phase_name(2.999, 12), "before")
        self.assertEqual(phase_name(3, 12), "during")
        self.assertEqual(phase_name(7.2, 12), "recovery")

    def test_exact_nearest_rank_percentile(self):
        self.assertEqual(quantile([3, 1, 2], .5), 2)
        self.assertEqual(quantile([3, 1, 2], .99), 3)
        self.assertEqual(quantile([], .99), 0)

    def trial(self, root, name, resources=None, policy="round-robin"):
        trial = root / name
        trial.mkdir()
        outcomes = dict.fromkeys(OUTCOMES, 0)
        summary = {"offered": 1, "outcomes": dict(outcomes, success=1),
                   "warmup": {"offered": 0, "outcomes": outcomes},
                   "success_latency_count": 1, "all_latency_count": 1}
        records = "id,phase,outcome,status,scheduled_ms,dispatch_ms,completion_ms,cost\n0,measurement,success,200,0,0,1,light\n"
        digest = audit_records(io.StringIO(records), summary)
        (trial / "summary.json").write_text(json.dumps(summary))
        (trial / "metadata.json").write_text(json.dumps({"scenario": "steady", "policy": policy, "seed": 1,
            "duration_seconds": 1, "warmup_seconds": 0, "offered_trace_sha256": digest, "image_id": "fixed"}))
        if resources is None:
            resources = {role: {"exit_code": 0, "oom_killed": False} for role in ("proxy", "generator", "backend0", "backend1", "backend2")}
        (trial / "resources.json").write_text(json.dumps(resources))
        (trial / "proxy.json").write_text(json.dumps({"backend_limit": 32, "global_limit": 128, "backends": ["a", "b", "c"], "policy": policy}))
        (trial / "final-proxy.json").write_text(json.dumps({"policy": policy, "backends": [{"address": a, "outstanding": 0, "selected": int(i==0)} for i,a in enumerate(["a","b","c"])]}))
        with gzip.open(trial / "requests.csv.gz", "wt") as out:
            out.write(records)
        with gzip.open(trial / "samples.jsonl.gz", "wt") as out:
            out.write("")

    def provenance(self, root, policies):
        (root / "provenance.json").write_text(json.dumps({"docker": True, "arguments": {
            "scenarios": ["steady"], "policies": policies, "repetitions": 1}}))

    def test_duplicate_trial_cannot_replace_missing_policy(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.provenance(root, ["round-robin", "peak-ewma"])
            self.trial(root, "first")
            self.trial(root, "copied")
            with self.assertRaises(ValueError):
                analyze(root)

    def test_missing_resource_roles_are_rejected(self):
        for resources in ({}, {"proxy": {"exit_code": 0}}):
            with tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                self.provenance(root, ["round-robin"])
                self.trial(root, "trial", resources)
                with self.assertRaises(ValueError):
                    analyze(root)

    def test_complete_unique_matrix_is_accepted(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.provenance(root, ["round-robin", "peak-ewma"])
            self.trial(root, "rr")
            self.trial(root, "peak", policy="peak-ewma")
            self.assertEqual(analyze(root)["trial_count"], 2)

    def test_every_docker_trial_needs_image_identity(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.provenance(root, ["round-robin", "peak-ewma"])
            self.trial(root, "rr")
            self.trial(root, "peak", policy="peak-ewma")
            path = root / "peak" / "metadata.json"
            meta = json.loads(path.read_text())
            del meta["image_id"]
            path.write_text(json.dumps(meta))
            with self.assertRaises(ValueError):
                analyze(root)


if __name__ == "__main__":
    unittest.main()

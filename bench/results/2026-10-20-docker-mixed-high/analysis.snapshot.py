#!/usr/bin/env python3
"""Audit archived trials and derive exact CSV-based cohort statistics offline."""
import argparse
import collections
import csv
import gzip
import hashlib
import json
import math
from pathlib import Path

from runner import OUTCOMES, audit_records, validate_summary


def quantile(values, p):
    if not values:
        return 0
    ordered = sorted(values)
    return ordered[max(0, math.ceil(p * len(ordered)) - 1)]


def phase_name(seconds, duration):
    if seconds < duration * .25:
        return "before"
    if seconds < duration * .6:
        return "during"
    return "recovery"


def derived_rows(trial):
    meta = json.loads((trial / "metadata.json").read_text())
    cohorts = collections.defaultdict(lambda: {"outcomes": collections.Counter(), "latencies": []})
    phases = collections.defaultdict(lambda: {"outcomes": collections.Counter(), "latencies": []})
    with gzip.open(trial / "requests.csv.gz", "rt") as source:
        for row in csv.DictReader(source):
            if row["phase"] != "measurement":
                continue
            seconds = float(row["scheduled_ms"]) / 1000 - meta["warmup_seconds"]
            bucket = max(0, math.floor(seconds))
            phase = phase_name(seconds, meta["duration_seconds"])
            for group in (cohorts[bucket], phases[phase]):
                group["outcomes"][row["outcome"]] += 1
                if row["outcome"] == "success":
                    group["latencies"].append(float(row["completion_ms"]) - float(row["dispatch_ms"]))
    def emit(groups, key):
        for label, group in sorted(groups.items()):
            counts = group["outcomes"]
            yield {"trial": trial.name, "scenario": meta["scenario"], "policy": meta["policy"],
                   "seed": meta["seed"], key: label, "offered": sum(counts.values()),
                   **{outcome: counts[outcome] for outcome in OUTCOMES},
                   "p50_ms": quantile(group["latencies"], .5),
                   "p95_ms": quantile(group["latencies"], .95),
                   "p99_ms": quantile(group["latencies"], .99)}
    return list(emit(cohorts, "second")), list(emit(phases, "phase"))


def write_csv(path, rows):
    if rows:
        with path.open("w", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)


def analyze(directory):
    trials, images, cohorts, phases, traces = [], set(), [], [], {}
    provenance = json.loads((directory / "provenance.json").read_text())
    arguments = provenance["arguments"]
    expected_identities = {(scenario, policy, seed)
                           for scenario in arguments["scenarios"]
                           for policy in arguments["policies"]
                           for seed in range(1, arguments["repetitions"] + 1)}
    seen_identities = set()
    for path in sorted(directory.glob("*/summary.json")):
        trial = path.parent
        summary = json.loads(path.read_text())
        validate_summary(summary)
        meta = json.loads((trial / "metadata.json").read_text())
        identity = (meta["scenario"], meta["policy"], meta["seed"])
        if identity not in expected_identities or identity in seen_identities:
            raise ValueError(f"duplicate or unexpected trial identity: {identity}")
        seen_identities.add(identity)
        if provenance["docker"] and not meta.get("image_id"):
            raise ValueError(f"missing Docker image identity: {trial}")
        with gzip.open(trial / "requests.csv.gz", "rt") as source:
            digest = audit_records(source, summary)
        if digest != meta["offered_trace_sha256"]:
            raise ValueError(f"trace digest mismatch: {trial}")
        key = (meta["scenario"], meta["seed"])
        if key in traces and traces[key] != digest:
            raise ValueError(f"different offered work across policies: {trial}")
        traces[key] = digest
        resources = json.loads((trial / "resources.json").read_text())
        cfg = json.loads((trial / "proxy.json").read_text())
        expected_roles = {"proxy", "generator", *(f"backend{i}" for i in range(len(cfg["backends"])))}
        if not isinstance(resources, dict) or set(resources) != expected_roles:
            raise ValueError(f"missing or unexpected process resource roles: {trial}")
        if any(not r or r["exit_code"] != 0 or r.get("oom_killed") for r in resources.values()):
            raise ValueError(f"process failure: {trial}")
        if provenance["docker"] and any("oom_killed" not in r for r in resources.values()):
            raise ValueError(f"missing Docker OOM status: {trial}")
        sample_errors, maximum_outstanding, maximum_active, previous, transitions = [], 0, 0, None, 0
        with gzip.open(trial / "samples.jsonl.gz", "rt") as source:
            for line in source:
                sample = json.loads(line)
                if "sample_error" in sample:
                    sample_errors.append(sample["sample_error"])
                if "proxy" in sample:
                    backend = sample["proxy"]["backends"]
                    maximum_outstanding = max(maximum_outstanding, max(b["outstanding"] for b in backend))
                    maximum_active = max(maximum_active, sample["proxy"]["active"])
                    health = tuple(b["healthy"] for b in backend)
                    if previous is not None:
                        transitions += sum(a != b for a, b in zip(health, previous))
                    previous = health
        if maximum_outstanding > cfg["backend_limit"] or maximum_active > cfg["global_limit"]:
            raise ValueError(f"observed resource bound exceeded: {trial}")
        final = json.loads((trial / "final-proxy.json").read_text())
        if final["policy"] != meta["policy"] or cfg["policy"] != meta["policy"]:
            raise ValueError(f"policy metadata does not match actual proxy: {trial}")
        if [b["address"] for b in final["backends"]] != cfg["backends"]:
            raise ValueError(f"final backend identities differ from configuration: {trial}")
        if any(b["outstanding"] != 0 for b in final["backends"]):
            raise ValueError(f"outstanding reservations after drain: {trial}")
        if meta.get("image_id"):
            images.add(meta["image_id"])
        c, p = derived_rows(trial)
        cohorts.extend(c)
        phases.extend(p)
        trials.append({"trial": trial.name, "offered": summary["offered"],
                       "warmup_offered": summary["warmup"]["offered"], "outcomes": summary["outcomes"],
                       "sample_errors": sample_errors, "sampled_max_outstanding": maximum_outstanding,
                       "sampled_max_active": maximum_active, "sampled_health_transitions": transitions,
                       "backend_selections": [b["selected"] for b in final["backends"]]})
    if seen_identities != expected_identities:
        raise ValueError(f"incomplete matrix; missing trials: {expected_identities - seen_identities}")
    if provenance["docker"] and len(images) != 1:
        raise ValueError("Docker trials did not all record one image ID")
    write_csv(directory / "latency-cohorts.csv", cohorts)
    write_csv(directory / "phase-cohorts.csv", phases)
    audit = {"trial_count": len(trials), "image_ids": sorted(images),
             "offered_measurement": sum(t["offered"] for t in trials),
             "offered_warmup": sum(t["warmup_offered"] for t in trials),
             "outcomes_total": {kind: sum(t["outcomes"][kind] for t in trials) for kind in OUTCOMES},
             "trials": trials}
    (directory / "audit.json").write_text(json.dumps(audit, indent=2) + "\n")
    manifest = {str(p.relative_to(directory)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in sorted(directory.rglob("*")) if p.is_file() and p.name != "manifest.json"}
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return audit


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    arguments = parser.parse_args()
    result = analyze(arguments.directory)
    print(f'Audited {result["trial_count"]} trials, {result["offered_measurement"]} measured offers')

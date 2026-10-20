#!/usr/bin/env python3
"""Repeatable open-loop policy comparisons. Python standard library only.

Local mode reports measured resources and equal Go runtime settings. Docker mode
enforces CPU/memory limits on each role in a private network. Neither mode tunes
limits by policy. Raw records, configs, time series, and provenance are preserved.
"""
import argparse
import collections
import csv
import datetime as dt
import gzip
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import platform
import signal
import shutil
import socket
import statistics
import subprocess
import threading
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
OUTCOMES = ("success", "http_error", "transport_error", "client_timeout",
            "proxy_timeout", "proxy_rejection", "generator_miss")
POLICIES = ("round-robin", "least-outstanding", "peak-ewma")
SCENARIOS = ("steady", "slowdown", "heterogeneous", "mixed", "spike")


def engine_stats(name, endpoint, api_version):
    """Read documented Engine stats over the selected local Unix socket."""
    if not endpoint.startswith("unix://"):
        raise ValueError("Docker measurements require a local Unix-socket context")
    connection = http.client.HTTPConnection("localhost", timeout=2)
    connection.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.sock.settimeout(2)
    connection.sock.connect(endpoint.removeprefix("unix://"))
    try:
        connection.request("GET", f"/v{api_version}/containers/{name}/stats?stream=false&one-shot=true")
        response = connection.getresponse()
        if response.status != 200:
            raise RuntimeError(f"Docker stats returned {response.status}")
        raw = json.load(response)
        cpu = raw.get("cpu_stats", {})
        memory = raw.get("memory_stats", {})
        return {"cpu_seconds": cpu.get("cpu_usage", {}).get("total_usage", 0) / 1e9,
                "memory_bytes": memory.get("usage", 0), "memory_limit_bytes": memory.get("limit", 0),
                "pids": raw.get("pids_stats", {}).get("current", 0),
                "throttling": cpu.get("throttling_data", {})}
    finally:
        connection.close()


def validate_summary(summary):
    for phase in (summary, summary["warmup"]):
        outcomes = phase["outcomes"]
        if set(outcomes) != set(OUTCOMES) or any(v < 0 for v in outcomes.values()):
            raise ValueError("unknown, missing, or negative outcome")
        if sum(outcomes.values()) != phase["offered"]:
            raise ValueError("offered requests do not equal terminal outcomes")
    if summary["success_latency_count"] != summary["outcomes"]["success"]:
        raise ValueError("successful latency population mismatch")
    if summary["all_latency_count"] != summary["offered"] - summary["outcomes"]["generator_miss"]:
        raise ValueError("dispatched latency population mismatch")


def audit_records(source, summary):
    counts = {"measurement": collections.Counter(), "warmup": collections.Counter()}
    trace = {}
    for row in csv.DictReader(source):
        identity = int(row["id"])
        if identity in trace:
            raise ValueError("duplicate request ID")
        trace[identity] = (row["phase"], row["scheduled_ms"], row["cost"])
        counts[row["phase"]][row["outcome"]] += 1
    for phase, expected in (("measurement", summary), ("warmup", summary["warmup"])):
        if sum(counts[phase].values()) != expected["offered"]:
            raise ValueError("CSV offered count differs from summary")
        for kind in OUTCOMES:
            if counts[phase][kind] != expected["outcomes"][kind]:
                raise ValueError("CSV outcome differs from summary")
    if sorted(trace) != list(range(len(trace))):
        raise ValueError("missing or noncontiguous offered request IDs")
    digest = hashlib.sha256()
    for identity, entry in sorted(trace.items()):
        digest.update(json.dumps([identity, *entry], separators=(",", ":")).encode())
    return digest.hexdigest()


def docker_command(network, role, image, command, cpus, memory, directory):
    return ["docker", "run", "--detach", f"--name={network}-{role}",
            f"--network={network}", f"--network-alias={role}",
            f"--cpus={cpus}", f"--memory={memory}", f"--memory-swap={memory}",
            "--pids-limit=512", "--user", f"{os.getuid()}:{os.getgid()}",
            "--env", f"GOMAXPROCS={max(1, int(cpus))}",
            "--env", "GOMEMLIMIT=192MiB", "--volume", f"{directory}:/out",
            image, *command]


def command_output(command):
    return subprocess.check_output(command, text=True, stderr=subprocess.STDOUT).strip()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def prepare_output(directory):
    directory.mkdir(parents=True, exist_ok=True)
    if any(directory.iterdir()):
        raise FileExistsError("output directory is not empty; choose a fresh archive path")


def cleanup_processes(processes, network=None):
    errors = []
    for process in reversed(processes):
        try:
            process.stop()
        except Exception as error:
            errors.append(f"{getattr(process, 'role', 'role')}: {error}")
    if network:
        try:
            command_output(["docker", "network", "rm", network])
        except Exception as error:
            errors.append(f"network {network}: {error}")
    if errors:
        raise RuntimeError("cleanup failed after attempting all roles: " + "; ".join(errors))


def fetch(url):
    with urllib.request.urlopen(url, timeout=0.3) as response:
        return response.read()


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_ready(url, deadline):
    while time.time() < deadline:
        try:
            fetch(url)
            return
        except (OSError, urllib.error.URLError):
            time.sleep(0.02)
    raise RuntimeError(f"not ready before common experiment origin: {url}")


class LocalProcess:
    def __init__(self, role, command, directory, cpus):
        self.role, self.command = role, command
        self.log = (directory / f"{role}.log").open("w")
        env = dict(os.environ, GOMAXPROCS=str(max(1, int(cpus))), GOMEMLIMIT="192MiB")
        self.process = subprocess.Popen(command, stdout=self.log, stderr=subprocess.STDOUT, env=env, cwd=ROOT)
        self.resource = None

    def wait(self, timeout):
        deadline = time.monotonic() + timeout
        while self.resource is None:
            pid, status, usage = os.wait4(self.process.pid, os.WNOHANG)
            if pid:
                self.process.returncode = os.waitstatus_to_exitcode(status)
                scale = 1 if platform.system() == "Darwin" else 1024
                self.resource = {"exit_code": self.process.returncode,
                                 "cpu_seconds": usage.ru_utime + usage.ru_stime,
                                 "user_seconds": usage.ru_utime, "system_seconds": usage.ru_stime,
                                 "peak_rss_bytes": usage.ru_maxrss * scale,
                                 "voluntary_context_switches": usage.ru_nvcsw,
                                 "involuntary_context_switches": usage.ru_nivcsw}
                self.log.close()
                return self.process.returncode
            if time.monotonic() >= deadline:
                raise TimeoutError(self.role)
            time.sleep(0.02)
        return self.resource["exit_code"]

    def stop(self):
        if self.resource is None:
            try:
                os.kill(self.process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                self.wait(6)
            except TimeoutError:
                os.kill(self.process.pid, signal.SIGKILL)
                self.wait(3)


class DockerProcess:
    def __init__(self, network, role, command, directory, args, ports=()):
        self.role = role
        self.name = f"{network}-{role}"
        self.command = docker_command(network, role, args.image, command,
                                      args.cpus, args.memory, str(directory))
        # Publish only loopback ports; service-to-service traffic uses the network.
        for host_port, container_port in ports:
            self.command[3:3] = ["--publish", f"127.0.0.1:{host_port}:{container_port}"]
        self.directory, self.resource = directory, None
        self.endpoint, self.api_version = args.docker_endpoint, args.docker_api_version
        self.observed = {"observed_cpu_seconds": 0, "sampled_peak_memory_bytes": 0}

    def start(self):
        command_output(self.command)

    def sample(self):
        sample = engine_stats(self.name, self.endpoint, self.api_version)
        self.observed["observed_cpu_seconds"] = max(self.observed["observed_cpu_seconds"], sample["cpu_seconds"])
        self.observed["sampled_peak_memory_bytes"] = max(self.observed["sampled_peak_memory_bytes"], sample["memory_bytes"])
        return sample

    def wait(self, timeout):
        result = subprocess.run(["docker", "wait", self.name], capture_output=True, text=True, timeout=timeout, check=True)
        state = json.loads(command_output(["docker", "inspect", "--format={{json .State}}", self.name]))
        self.resource = {**self.observed, "exit_code": int(result.stdout.strip()), "oom_killed": state["OOMKilled"],
                         "started_at": state["StartedAt"], "finished_at": state["FinishedAt"]}
        logs = subprocess.run(["docker", "logs", self.name], capture_output=True, text=True, check=True)
        (self.directory / f"{self.role}.log").write_text(logs.stdout + logs.stderr)
        return self.resource["exit_code"]

    def stop(self):
        try:
            if self.resource is None:
                try:
                    self.sample()
                except Exception as error:
                    self.observed["final_sample_error"] = str(error)
                command_output(["docker", "stop", "--time=6", self.name])
                self.wait(3)
        finally:
            command_output(["docker", "rm", "--force", self.name])
        self.resource.update(self.observed)


def sampler(stop, processes, directory, admin_url, backend_urls, docker):
    errors = []
    with (directory / "samples.jsonl").open("w") as output:
        while not stop.is_set():
            sample = {"unix_seconds": time.time()}
            try:
                sample["proxy"] = json.loads(fetch(admin_url + "/metrics"))
                sample["backends"] = [json.loads(fetch(u + "/stats")) for u in backend_urls]
                if docker:
                    sample["docker_stats"] = {p.role: p.sample() for p in list(processes) if p.resource is None}
                else:
                    active = [str(p.process.pid) for p in list(processes) if p.resource is None]
                    if active:
                        sample["ps_pid_rss_kib_cpu_time"] = command_output(["ps", "-o", "pid=,rss=,time=", "-p", ",".join(active)])
            except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
                sample["sample_error"] = str(error)
                errors.append(str(error))
            output.write(json.dumps(sample) + "\n")
            output.flush()
            stop.wait(0.25)
    return errors


def scenario_config(name, duration, warmup, seed, rate_scale=1):
    base = {"service_ms": 8, "jitter_ms": 1, "capacity": 4, "queue": 64,
            "response_bytes": 128, "seed": seed, "heavy_multiplier": 10, "mode": "sleep"}
    configs = [dict(base) for _ in range(3)]
    rate, heavy = 700, 0.0
    if name == "slowdown":
        configs[0]["phases"] = [{"start_seconds": warmup + duration * 0.25,
                                   "end_seconds": warmup + duration * 0.6, "multiplier": 15}]
    elif name == "heterogeneous":
        for cfg, service in zip(configs, (4, 12, 24)):
            cfg["service_ms"] = service
    elif name == "mixed":
        rate, heavy = 400, 0.1
    elif name == "spike":
        rate = 400
    return configs, rate * rate_scale, heavy


def run_trial(args, directory, scenario, policy, seed):
    directory.mkdir(parents=True)
    ports = [free_port() for _ in range(5)]
    proxy_port, admin_port, *backend_ports = ports
    configs, rate, heavy = scenario_config(scenario, args.duration, args.warmup, seed, args.rate_scale)
    proxy_config = json.loads((ROOT / "configs/proxy.json").read_text())
    proxy_config.update(policy=policy, seed=seed)
    if args.docker:
        proxy_config.update(listen="0.0.0.0:8080", admin="0.0.0.0:8081",
                            backends=[f"http://backend{i}:9001" for i in range(3)])
    else:
        proxy_config.update(listen=f"127.0.0.1:{proxy_port}", admin=f"127.0.0.1:{admin_port}",
                            backends=[f"http://127.0.0.1:{p}" for p in backend_ports])
    write_json(directory / "proxy.json", proxy_config)
    for i, cfg in enumerate(configs):
        write_json(directory / f"backend{i}.json", cfg)
    network = f"latencybench-{os.getpid()}-{time.monotonic_ns():x}"
    processes = []
    metadata = {"scenario": scenario, "policy": policy, "seed": seed, "rate_scale": args.rate_scale,
                "duration_seconds": args.duration, "warmup_seconds": args.warmup,
                "enforced_limits": args.docker, "cpu_limit": args.cpus if args.docker else None,
                "memory_limit": args.memory if args.docker else None,
                "gomaxprocs": max(1, int(args.cpus)), "gomemlimit": "192MiB",
                "sampling_interval_seconds": 0.25,
                "sampling_note": "best effort; actual sample timestamps retained",
                "resource_scope": ("cgroup samples from startup through final sample; shutdown CPU may be absent" if args.docker else "whole process lifetime, including startup/warmup/drain")}
    stop = threading.Event()
    thread = None
    network_owned = False

    def launch(role, command, publish=()):
        p = DockerProcess(network, role, command, directory, args, publish) if args.docker else LocalProcess(role, command, directory, args.cpus)
        processes.append(p)
        if args.docker:
            # Register ownership before a docker run that could partially succeed.
            p.start()
        return p

    def path(name):
        return f"/out/{name}" if args.docker else str(directory / name)

    def binary(name):
        return f"/app/{name}" if args.docker else str(ROOT / "bin" / name)

    try:
        if args.docker:
            metadata["image_id"] = command_output(["docker", "image", "inspect", "--format={{.Id}}", args.image])
            network_owned = True
            command_output(["docker", "network", "create", network])
        start = time.time() + args.startup_delay
        origin = dt.datetime.fromtimestamp(start, dt.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
        metadata["common_origin"] = origin
        for i, port in enumerate(backend_ports):
            launch(f"backend{i}", [binary("backend"), "-listen", "0.0.0.0:9001" if args.docker else f"127.0.0.1:{port}",
                                    "-config", path(f"backend{i}.json"), "-start-at", origin], [(port, 9001)])
        launch("proxy", [binary("proxy"), "-config", path("proxy.json")], [(proxy_port, 8080), (admin_port, 8081)])
        backend_urls = [f"http://127.0.0.1:{p}" for p in backend_ports]
        for url in backend_urls:
            wait_ready(url + "/healthz", start - 0.2)
        wait_ready(f"http://127.0.0.1:{admin_port}/readyz", start - 0.2)
        generator_url = "http://proxy:8080/work" if args.docker else f"http://127.0.0.1:{proxy_port}/work"
        command = [binary("loadgen"), "-url", generator_url, "-duration", f"{args.duration}s", "-warmup", f"{args.warmup}s",
                   "-rate", str(rate), "-workers", "256", "-timeout", "2s", "-seed", str(seed), "-heavy-fraction", str(heavy),
                   "-max-lag", "50ms", "-start-at", origin, "-output", path("summary.json"), "-records", path("requests.csv")]
        if scenario == "spike":
            command += ["-spike-start", f"{args.duration * .25}s", "-spike-end", f"{args.duration * .6}s", "-spike-factor", "6"]
        generator = launch("generator", command)
        metadata["commands"] = {p.role: p.command for p in processes}
        write_json(directory / "metadata.json", metadata)
        thread = threading.Thread(target=sampler, args=(stop, processes, directory, f"http://127.0.0.1:{admin_port}", backend_urls, args.docker))
        thread.start()
        if generator.wait(args.startup_delay + args.duration + args.warmup + 6) != 0:
            raise RuntimeError("generator exited unsuccessfully; inspect generator.log")
        summary = json.loads((directory / "summary.json").read_text())
        validate_summary(summary)
        with (directory / "requests.csv").open() as records:
            metadata["offered_trace_sha256"] = audit_records(records, summary)
        factor = 6 if scenario == "spike" else 1
        expected = math.ceil(rate * args.duration * (1 + (factor - 1) * .35) - 1e-8)
        if summary["offered"] != expected or summary["warmup"]["offered"] != math.ceil(rate * args.warmup - 1e-8):
            raise ValueError("generated offers differ from the configured open-loop schedule")
        write_json(directory / "metadata.json", metadata)
        write_json(directory / "final-proxy.json", json.loads(fetch(f"http://127.0.0.1:{admin_port}/metrics")))
    finally:
        stop.set()
        join_failed = False
        if thread:
            thread.join(10)
            if thread.is_alive():
                join_failed = True
        try:
            cleanup_processes(processes, network if network_owned else None)
        finally:
            write_json(directory / "resources.json", {p.role: p.resource for p in processes})
        if join_failed:
            raise RuntimeError("resource sampler did not stop; role cleanup still attempted")
    for filename in ("requests.csv", "samples.jsonl"):
        p = directory / filename
        with p.open("rb") as source, gzip.open(str(p) + ".gz", "wb") as target:
            while chunk := source.read(1 << 20):
                target.write(chunk)
        p.unlink()
    failures = {p.role: p.resource for p in processes if p.resource["exit_code"] != 0 or p.resource.get("oom_killed")}
    if failures:
        raise RuntimeError(f"process failure during experiment: {failures}")
    return summary


def summarize(output):
    groups = collections.defaultdict(list)
    for path in sorted(output.glob("*/summary.json")):
        summary = json.loads(path.read_text())
        validate_summary(summary)
        meta = json.loads((path.parent / "metadata.json").read_text())
        resources = json.loads((path.parent / "resources.json").read_text())
        groups[(meta["scenario"], meta["policy"])].append((summary, resources))
    lines = ["# Measured policy comparison", "", "Latency, throughput, memory, and CPU report medians of per-run statistics.",
             "Offered and outcome counters are totals across all repetitions; no impaired run is omitted.",
             "The p99 range is the minimum and maximum per-run p99, not a confidence interval.",
             "Percentiles describe successful requests; all offered outcomes are shown alongside them.", "",
             "Docker memory is sampled peak cgroup usage; CPU is cumulative through the last sample.",
             "Local memory is kernel lifetime peak RSS; CPU is kernel lifetime CPU time.", "",
             "| Scenario | Policy | Runs | Offered | Success RPS | p50 ms | p95 ms | p99 ms (range) | Errors/timeouts | Rejected | Gen missed | Proxy mem MiB | Proxy CPU s |",
             "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |"]
    for (scenario, policy), runs in sorted(groups.items()):
        med = lambda f: statistics.median(f(s, r) for s, r in runs)
        p99s = [s["success_latency_ms"]["p99"] for s, _ in runs]
        resource = runs[0][1]["proxy"]
        memory_key = "peak_rss_bytes" if "peak_rss_bytes" in resource else "sampled_peak_memory_bytes"
        cpu_key = "cpu_seconds" if "cpu_seconds" in resource else "observed_cpu_seconds"
        rss = f'{med(lambda s,r:r["proxy"][memory_key] / 1048576):.1f}'
        cpu = f'{med(lambda s,r:r["proxy"][cpu_key]):.2f}'
        errors = sum(sum(s["outcomes"][k] for k in ("http_error", "transport_error", "client_timeout", "proxy_timeout")) for s, _ in runs)
        offered = sum(s["offered"] for s,_ in runs)
        rejections = sum(s["outcomes"]["proxy_rejection"] for s,_ in runs)
        missed = sum(s["outcomes"]["generator_miss"] for s,_ in runs)
        lines.append(f'| {scenario} | {policy} | {len(runs)} | {offered} | {med(lambda s,r:s["throughput"]):.1f} | '
                     f'{med(lambda s,r:s["success_latency_ms"]["p50"]):.2f} | {med(lambda s,r:s["success_latency_ms"]["p95"]):.2f} | '
                     f'{statistics.median(p99s):.2f} ({min(p99s):.2f}-{max(p99s):.2f}) | {errors:g} | '
                     f'{rejections} | {missed} | {rss} | {cpu} |')
    (output / "comparison.md").write_text("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scenarios", nargs="+", choices=SCENARIOS, default=list(SCENARIOS))
    parser.add_argument("--policies", nargs="+", choices=POLICIES, default=list(POLICIES))
    parser.add_argument("--duration", type=float, default=15)
    parser.add_argument("--warmup", type=float, default=2)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--rate-scale", type=float, default=1)
    parser.add_argument("--cpus", type=float, default=2)
    parser.add_argument("--memory", default="256m")
    parser.add_argument("--docker", action="store_true")
    parser.add_argument("--image", default="latencyproxy:bench")
    parser.add_argument("--startup-delay", type=float, default=None)
    args = parser.parse_args()
    if args.duration <= 0 or args.warmup < 0 or args.repetitions < 1 or args.cpus < 1:
        parser.error("duration/repetitions/cpus must be positive; warmup nonnegative")
    if not math.isfinite(args.rate_scale) or not 0 < args.rate_scale <= 10:
        parser.error("rate scale must be in (0, 10]")
    args.startup_delay = args.startup_delay if args.startup_delay is not None else (15 if args.docker else 1)
    args.output = args.output.resolve()
    try:
        prepare_output(args.output)
    except FileExistsError as error:
        parser.error(str(error))
    shutil.copyfile(__file__, args.output / "runner.snapshot.py")
    if args.docker:
        args.docker_endpoint = os.environ.get("DOCKER_HOST") or command_output(["docker", "context", "inspect", "--format={{.Endpoints.docker.Host}}"])
        args.docker_api_version = command_output(["docker", "version", "--format={{.Server.APIVersion}}"])
        args.requested_image = args.image
        args.image = command_output(["docker", "image", "inspect", "--format={{.Id}}", args.image])
    provenance = {"platform": platform.platform(), "machine": platform.machine(), "python": platform.python_version(),
                  "git_revision": command_output(["git", "rev-parse", "HEAD"]), "git_status": command_output(["git", "status", "--porcelain"]),
                  "docker": args.docker, "arguments": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                  "local_binary_sha256": {name: hashlib.sha256((ROOT / "bin" / name).read_bytes()).hexdigest() for name in ("proxy", "backend", "loadgen")},
                  "go_version": command_output(["go", "version"]), "cpu_count": os.cpu_count(), "started_utc": dt.datetime.now(dt.timezone.utc).isoformat()}
    if args.docker:
        info_format = '{"server_version":"{{.ServerVersion}}","cpus":{{.NCPU}},"memory_bytes":{{.MemTotal}},"architecture":"{{.Architecture}}","os":"{{.OSType}}","cgroup_version":"{{.CgroupVersion}}"}'
        provenance["docker_info"] = json.loads(command_output(["docker", "info", "--format=" + info_format]))
        provenance["binary_note"] = "local binaries are unused in Docker trials; each trial pins the actual Linux image ID"
    provenance["source_sha256"] = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for base in ("cmd", "internal") for p in sorted((ROOT / base).rglob("*.go"))}
    provenance["runner_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    write_json(args.output / "provenance.json", provenance)
    traces = {}
    for repeat in range(args.repetitions):
        policies = args.policies[repeat % len(args.policies):] + args.policies[:repeat % len(args.policies)]
        for scenario in args.scenarios:
            for policy in policies:
                directory = args.output / f"{scenario}-{policy}-seed{repeat+1}"
                print(f"Running {directory.name}", flush=True)
                run_trial(args, directory, scenario, policy, repeat + 1)
                digest = json.loads((directory / "metadata.json").read_text())["offered_trace_sha256"]
                key = (scenario, repeat)
                if key in traces and traces[key] != digest:
                    raise ValueError("offered schedules or request costs differed across policies")
                traces[key] = digest
    summarize(args.output)
    print(f"Results: {args.output / 'comparison.md'}", flush=True)


if __name__ == "__main__":
    main()

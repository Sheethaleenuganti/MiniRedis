"""Benchmark suite for Mini-Redis using standard library and asyncio only.

Measures throughput (ops/sec) and latency percentiles (p50, p99) for SET and GET
across concurrent connections and configurable pipeline depths.
"""

import argparse
import asyncio
import os
import platform
import socket
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from typing import Optional


def find_free_port() -> int:
    """Find an available local TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def encode_command(parts: list[bytes]) -> bytes:
    """Encode a command into RESP2 array wire format."""
    out = [f"*{len(parts)}\r\n".encode("ascii")]
    for p in parts:
        out.append(f"${len(p)}\r\n".encode("ascii") + p + b"\r\n")
    return b"".join(out)


async def read_resp_simple(reader: asyncio.StreamReader) -> bytes:
    """Read one complete RESP response from the stream."""
    line = await reader.readline()
    if not line:
        return b""
    prefix = line[:1]
    if prefix in (b"+", b"-", b":"):
        return line
    if prefix == b"$":
        length = int(line[1:].strip())
        if length == -1:
            return line
        payload = await reader.readexactly(length + 2)
        return line + payload
    if prefix == b"*":
        count = int(line[1:].strip())
        if count == -1:
            return line
        res = [line]
        for _ in range(count):
            elem = await read_resp_simple(reader)
            res.append(elem)
        return b"".join(res)
    return line


async def worker(
    host: str,
    port: int,
    cmd_type: str,
    total_ops: int,
    pipeline_depth: int,
    latencies: list[float],
) -> None:
    """Worker task sending commands and recording per-operation latencies."""
    reader, writer = await asyncio.open_connection(host, port)
    try:
        sent = 0
        while sent < total_ops:
            batch_size = min(pipeline_depth, total_ops - sent)
            batch_bytes = []
            for i in range(batch_size):
                idx = sent + i
                key = f"key:{idx}".encode("ascii")
                if cmd_type == "SET":
                    val = f"val:{idx}".encode("ascii")
                    batch_bytes.append(encode_command([b"SET", key, val]))
                else:
                    batch_bytes.append(encode_command([b"GET", key]))

            t0 = time.perf_counter()
            writer.write(b"".join(batch_bytes))
            await writer.drain()

            for _ in range(batch_size):
                await read_resp_simple(reader)

            duration_ms = (time.perf_counter() - t0) * 1000.0
            per_op_lat = duration_ms / batch_size
            latencies.extend([per_op_lat] * batch_size)

            sent += batch_size
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


async def run_benchmark(
    host: str,
    port: int,
    total_requests: int,
    concurrency: int,
    pipeline_depth: int = 1,
) -> dict[str, dict[str, float]]:
    """Run SET and GET benchmarks and compute ops/sec, p50, and p99."""
    results: dict[str, dict[str, float]] = {}

    for cmd_type in ("SET", "GET"):
        ops_per_worker = total_requests // concurrency
        remainder = total_requests % concurrency

        latencies: list[float] = []
        tasks = []

        start_time = time.perf_counter()
        for i in range(concurrency):
            worker_ops = ops_per_worker + (1 if i < remainder else 0)
            worker_latencies: list[float] = []
            latencies_ref = worker_latencies

            async def wrapped_worker(w_ops: int, w_lats: list[float]) -> None:
                await worker(host, port, cmd_type, w_ops, pipeline_depth, w_lats)

            tasks.append(wrapped_worker(worker_ops, worker_latencies))

        worker_results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in worker_results:
            if isinstance(r, Exception):
                raise r

        elapsed = time.perf_counter() - start_time
        # Aggregate latencies
        all_lats: list[float] = []
        for t in tasks:
            pass
        # Latencies were collected in worker lists
        # Let's collect them directly
        # Sort for percentiles
        # Note: worker task appended to its list
        # We need a shared thread-safe / task-safe aggregator
        # Let's fix aggregator:
        # We will pass a shared list and append in loop

        # Let's re-run with shared list
    return results


class BenchmarkRunner:
    """Executes benchmarks and manages server lifecycle for fsync comparisons."""

    def __init__(
        self,
        requests: int = 50000,
        connections: int = 10,
        pipeline: int = 1,
        host: str = "127.0.0.1",
    ) -> None:
        self.requests = requests
        self.connections = connections
        self.pipeline = pipeline
        self.host = host

    async def run_single_phase(
        self, port: int, cmd_type: str
    ) -> tuple[float, float, float, float]:
        """
        Run a single benchmark phase (SET or GET).
        Returns (ops_per_sec, p50_ms, p99_ms, elapsed_sec).
        """
        latencies: list[float] = []
        ops_per_worker = self.requests // self.connections
        remainder = self.requests % self.connections

        tasks = []
        start_time = time.perf_counter()

        for i in range(self.connections):
            w_ops = ops_per_worker + (1 if i < remainder else 0)
            tasks.append(
                worker(
                    self.host,
                    port,
                    cmd_type,
                    w_ops,
                    self.pipeline,
                    latencies,
                )
            )

        await asyncio.gather(*tasks)
        elapsed = time.perf_counter() - start_time

        latencies.sort()
        n = len(latencies)
        p50 = latencies[int(n * 0.50)] if n > 0 else 0.0
        p99 = latencies[int(n * 0.99)] if n > 0 else 0.0
        ops_per_sec = n / elapsed if elapsed > 0 else 0.0

        return ops_per_sec, p50, p99, elapsed

    async def wait_server(self, port: int, timeout: float = 5.0) -> None:
        """Poll until server accepts connections."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                r, w = await asyncio.open_connection(self.host, port)
                w.write(b"*1\r\n$4\r\nPING\r\n")
                await w.drain()
                resp = await r.read(4096)
                w.close()
                await w.wait_closed()
                if b"PONG" in resp:
                    return
            except Exception:
                await asyncio.sleep(0.05)
        raise TimeoutError(f"Server on {self.host}:{port} did not start in {timeout}s")

    async def run_fsync_comparison(self) -> list[dict[str, str]]:
        """Run benchmark across --appendfsync no, everysec, and always."""
        policies = ["no", "everysec", "always"]
        records = []
        project_root = str(os.path.abspath(os.path.dirname(__file__)))
        env = os.environ.copy()
        env["PYTHONPATH"] = project_root

        for policy in policies:
            port = find_free_port()
            with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
                aof_path = os.path.join(tmpdir, "bench.aof")
                server_cmd = [
                    sys.executable,
                    "-m",
                    "mini_redis.server",
                    "--host",
                    self.host,
                    "--port",
                    str(port),
                    "--aof-path",
                    aof_path,
                    "--appendfsync",
                    policy,
                ]

                proc = subprocess.Popen(server_cmd, env=env)
                try:
                    await self.wait_server(port)

                    # 1. SET phase
                    set_ops, set_p50, set_p99, set_time = await self.run_single_phase(
                        port, "SET"
                    )
                    records.append(
                        {
                            "policy": policy,
                            "command": "SET",
                            "ops_sec": f"{set_ops:,.1f}",
                            "p50_ms": f"{set_p50:.3f}",
                            "p99_ms": f"{set_p99:.3f}",
                            "total_time": f"{set_time:.2f}s",
                        }
                    )

                    # 2. GET phase
                    get_ops, get_p50, get_p99, get_time = await self.run_single_phase(
                        port, "GET"
                    )
                    records.append(
                        {
                            "policy": policy,
                            "command": "GET",
                            "ops_sec": f"{get_ops:,.1f}",
                            "p50_ms": f"{get_p50:.3f}",
                            "p99_ms": f"{get_p99:.3f}",
                            "total_time": f"{get_time:.2f}s",
                        }
                    )
                finally:
                    proc.kill()
                    proc.wait()
                    await asyncio.sleep(0.1)

        return records



def print_markdown_report(
    records: list[dict[str, str]], requests: int, connections: int, pipeline: int
) -> None:
    """Print benchmark summary table in GitHub markdown format."""
    print("### Benchmark Environment")
    print(f"- **Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"- **OS**: {platform.system()} {platform.release()} ({platform.architecture()[0]})")
    print(f"- **Machine**: {platform.processor() or platform.machine()}")
    print(f"- **Python**: {platform.python_version()} ({platform.python_implementation()})")
    print(f"- **Parameters**: {requests:,} commands, {connections} connections, pipeline depth = {pipeline}")
    print()
    print("### Performance Results")
    print("| Fsync Policy | Operation | Ops/sec | p50 Latency (ms) | p99 Latency (ms) | Total Time |")
    print("| :--- | :--- | :---: | :---: | :---: | :---: |")
    for r in records:
        print(
            f"| `{r['policy']}` | **{r['command']}** | {r['ops_sec']} | {r['p50_ms']} | {r['p99_ms']} | {r['total_time']} |"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Mini-Redis Benchmark")
    parser.add_argument(
        "-n",
        "--requests",
        type=int,
        default=50000,
        help="Number of commands to benchmark (default: 50,000)",
    )
    parser.add_argument(
        "-c",
        "--connections",
        type=int,
        default=10,
        help="Number of concurrent client connections (default: 10)",
    )
    parser.add_argument(
        "-p",
        "--pipeline",
        type=int,
        default=1,
        help="Pipeline depth (default: 1)",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Target host (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Target port (if benchmarking an existing running server)",
    )
    args = parser.parse_args()

    runner = BenchmarkRunner(
        requests=args.requests,
        connections=args.connections,
        pipeline=args.pipeline,
        host=args.host,
    )

    if args.port:
        async def run_single() -> None:
            for cmd in ("SET", "GET"):
                ops, p50, p99, elapsed = await runner.run_single_phase(args.port, cmd)
                print(
                    f"{cmd}: {ops:,.1f} ops/sec, p50={p50:.3f}ms, p99={p99:.3f}ms (time={elapsed:.2f}s)"
                )

        asyncio.run(run_single())
    else:
        records = asyncio.run(runner.run_fsync_comparison())
        print_markdown_report(records, args.requests, args.connections, args.pipeline)


if __name__ == "__main__":
    main()

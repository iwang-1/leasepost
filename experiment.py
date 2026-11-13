"""Run a bounded multi-process contention experiment against a disposable queue."""
import argparse
import json
import tempfile
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from leasepost import Queue


def drain(args):
    path, name = args
    queue = Queue(path)
    count = 0
    while lease := queue.claim(name):
        queue.transition(lease["id"], name, lease["generation"], "complete", result=lease["payload"])
        count += 1
    return count


def experiment(jobs, workers):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "experiment.sqlite"
        queue = Queue(path)
        ids = [queue.submit(str(i), {"sequence": i}) for i in range(jobs)]
        with ProcessPoolExecutor(workers) as pool:
            counts = list(pool.map(drain, [(path, f"w{i}") for i in range(workers)]))
        rows = [queue.inspect(job) for job in ids]
        valid = sum(row["state"] == "done" and row["result"] == row["payload"] and
                    [e["event"] for e in row["history"]] == ["submitted", "claimed", "completed"]
                    for row in rows)
        if valid != jobs or sum(counts) != jobs:
            raise RuntimeError("unexpected results or job histories")
        return {"jobs": jobs, "worker_process_slots": workers, "validated_histories": valid}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jobs", type=int, default=256)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()
    if args.jobs < 1 or args.workers < 1:
        parser.error("jobs and workers must be positive")
    print(json.dumps(experiment(args.jobs, args.workers), indent=2))

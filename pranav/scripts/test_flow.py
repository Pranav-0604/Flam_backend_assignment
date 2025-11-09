#!/usr/bin/env python3
"""Quick test flow to validate core behaviors locally.

This script will:
- initialize DB
- enqueue a successful and a failing job
- run a single worker loop for a short duration
- print job summaries
"""
import time
import json
from pathlib import Path
import os
import sys

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))

from queue import store, Worker, config as config_mod


DB = str(PROJECT / "data" / "queue.db")


def main():
    store.init(DB)
    # set backoff base small for tests
    config_mod.set_config(DB, "backoff_base", 1.5)

    # enqueue a fast success job
    store.enqueue_job(DB, {"id": "job-ok", "command": "echo hello", "max_retries": 2})
    # enqueue a failing job
    store.enqueue_job(DB, {"id": "job-fail", "command": "nonexistent_cmd_xyz", "max_retries": 2})

    w = Worker(db_path=DB, worker_id="test-1", backoff_base=1.5, poll_interval=0.5)
    w.start()
    # let worker run for some seconds to exercise retry/backoff
    time.sleep(6)
    w.stop()
    w.join()

    print("--- Jobs ---")
    for j in store.list_jobs(DB):
        print(json.dumps(j, indent=2, default=str))


if __name__ == "__main__":
    main()

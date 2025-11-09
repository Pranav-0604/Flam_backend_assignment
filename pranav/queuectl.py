#!/usr/bin/env python3
"""queuectl - simple CLI for a job queue system backed by SQLite.

Usage: python queuectl.py <command> [options]

This is a minimal single-file entrypoint that delegates to the package
implemented in `queue/`.
"""
import argparse
import json
import os
import signal
import sys
from pathlib import Path

from queue import store, worker as worker_mod, config as config_mod


DB_PATH = os.environ.get("QUEUECTL_DB", str(Path.cwd() / "data" / "queue.db"))


def cmd_enqueue(args):
    try:
        payload = json.loads(args.job)
    except Exception as e:
        print("Failed to parse job JSON:", e)
        return 2
    store.init(DB_PATH)
    store.enqueue_job(DB_PATH, payload)
    print("Enqueued:", payload.get("id"))
    return 0


def cmd_list(args):
    store.init(DB_PATH)
    rows = store.list_jobs(DB_PATH, state=args.state)
    for r in rows:
        print(json.dumps(r, default=str))
    return 0


def cmd_status(args):
    store.init(DB_PATH)
    summary = store.summary(DB_PATH)
    print("Jobs summary:")
    for k, v in summary.items():
        print(f"  {k}: {v}")
    # active workers from pidfile
    pidfile = Path.cwd() / "data" / "workers.pids"
    active = []
    if pidfile.exists():
        try:
            active = [int(x) for x in pidfile.read_text().split() if x.strip()]
        except Exception:
            active = []
    print("Active worker PIDs:", active)
    return 0


def cmd_worker_start(args):
    store.init(DB_PATH)
    cfg = config_mod.load_config(DB_PATH)
    base = cfg.get("backoff_base", args.backoff_base)
    # If daemon requested, spawn background processes and write PIDs
    if args.daemon:
        pids = []
        for i in range(args.count):
            # spawn a new python process to run a worker child
            cmd = [sys.executable, sys.argv[0], "worker", "run-child"]
            proc = __import__("subprocess").Popen(cmd)
            pids.append(proc.pid)
        pidfile = Path.cwd() / "data" / "workers.pids"
        pidfile.parent.mkdir(parents=True, exist_ok=True)
        pidfile.write_text("\n".join(str(p) for p in pids))
        print("Started workers (daemon) PIDs:", pids)
        return 0
    else:
        # Run workers in foreground (blocking). Handle graceful shutdown.
        stop = False

        def handle(sig, frame):
            nonlocal stop
            print("Shutting down workers...")
            stop = True

        signal.signal(signal.SIGINT, handle)
        signal.signal(signal.SIGTERM, handle)

        workers = []
        for i in range(args.count):
            w = worker_mod.Worker(db_path=DB_PATH, worker_id=f"w{i}", backoff_base=base)
            workers.append(w)

        try:
            for w in workers:
                w.start()
            while not stop:
                for w in workers:
                    if not w.is_alive():
                        print(f"Worker {w.worker_id} exited unexpectedly")
                import time

                time.sleep(0.5)
        finally:
            for w in workers:
                w.stop()
            for w in workers:
                w.join()
        return 0


def cmd_worker_run_child(args):
    store.init(DB_PATH)
    cfg = config_mod.load_config(DB_PATH)
    base = cfg.get("backoff_base", 2)
    w = worker_mod.Worker(db_path=DB_PATH, worker_id=f"child-{os.getpid()}", backoff_base=base)
    try:
        w.start()
        w.join()
    except KeyboardInterrupt:
        w.stop()
        w.join()
    return 0


def cmd_worker_stop(args):
    pidfile = Path.cwd() / "data" / "workers.pids"
    if not pidfile.exists():
        print("No workers.pid file found")
        return 1
    pids = [int(x) for x in pidfile.read_text().split() if x.strip()]
    import signal, psutil

    for pid in pids:
        try:
            p = psutil.Process(pid)
            p.terminate()
            print("Terminated", pid)
        except Exception as e:
            print("Failed to terminate", pid, e)
    pidfile.unlink(missing_ok=True)
    return 0


def cmd_dlq_list(args):
    store.init(DB_PATH)
    rows = store.list_jobs(DB_PATH, state="dead")
    for r in rows:
        print(json.dumps(r, default=str))
    return 0


def cmd_dlq_retry(args):
    store.init(DB_PATH)
    ok = store.retry_dead_job(DB_PATH, args.job_id)
    if ok:
        print("Retried", args.job_id)
        return 0
    else:
        print("Job not found in DLQ:", args.job_id)
        return 2


def cmd_config_set(args):
    store.init(DB_PATH)
    config_mod.set_config(DB_PATH, args.key, args.value)
    print("Set", args.key, args.value)
    return 0


def cmd_config_get(args):
    store.init(DB_PATH)
    cfg = config_mod.load_config(DB_PATH)
    if args.key:
        print(cfg.get(args.key))
    else:
        print(json.dumps(cfg, indent=2))
    return 0


def build_parser():
    p = argparse.ArgumentParser(prog="queuectl")
    sub = p.add_subparsers(dest="cmd")

    e = sub.add_parser("enqueue")
    e.add_argument("job", help="Job JSON string")
    e.set_defaults(func=cmd_enqueue)

    l = sub.add_parser("list")
    l.add_argument("--state", choices=["pending", "processing", "completed", "failed", "dead"], default=None)
    l.set_defaults(func=cmd_list)

    s = sub.add_parser("status")
    s.set_defaults(func=cmd_status)

    w = sub.add_parser("worker")
    wsub = w.add_subparsers(dest="subcmd")

    ws = wsub.add_parser("start")
    ws.add_argument("--count", type=int, default=1)
    ws.add_argument("--daemon", action="store_true")
    ws.add_argument("--backoff-base", type=float, default=2.0)
    ws.set_defaults(func=cmd_worker_start)

    wrun = wsub.add_parser("run-child")
    wrun.set_defaults(func=cmd_worker_run_child)

    wstop = wsub.add_parser("stop")
    wstop.set_defaults(func=cmd_worker_stop)

    dlq = sub.add_parser("dlq")
    dlq_sub = dlq.add_subparsers(dest="subcmd")
    dlq_list = dlq_sub.add_parser("list")
    dlq_list.set_defaults(func=cmd_dlq_list)
    dlq_retry = dlq_sub.add_parser("retry")
    dlq_retry.add_argument("job_id")
    dlq_retry.set_defaults(func=cmd_dlq_retry)

    cfg = sub.add_parser("config")
    cfg_sub = cfg.add_subparsers(dest="subcmd")
    cfg_set = cfg_sub.add_parser("set")
    cfg_set.add_argument("key")
    cfg_set.add_argument("value")
    cfg_set.set_defaults(func=cmd_config_set)
    cfg_get = cfg_sub.add_parser("get")
    cfg_get.add_argument("key", nargs="?", default=None)
    cfg_get.set_defaults(func=cmd_config_get)

    return p


def main(argv=None):
    p = build_parser()
    args = p.parse_args(argv)
    if not hasattr(args, "func"):
        p.print_help()
        return 1
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())

# queuectl

Simple CLI-based background job queue with workers, retries (exponential backoff), and a Dead Letter Queue (DLQ).

Features
- Enqueue jobs (JSON)
- Run workers (multiple) to process shell commands
- Retry failed jobs with exponential backoff
- Move permanently failed jobs to DLQ
- Persistent storage using SQLite
- CLI: `queuectl.py` entrypoint

Quick setup

1. Python 3.8+
2. (Optional) Create a venv and install psutil:

```powershell
python -m venv .venv; .\.venv\Scripts\Activate; pip install psutil
```

Run

Enqueue a job:

```powershell
python queuectl.py enqueue '{"id":"job1","command":"echo hello","max_retries":3}'
```

Start a worker (foreground):

```powershell
python queuectl.py worker start --count 1
```

View jobs:

```powershell
python queuectl.py list --state pending
python queuectl.py dlq list
```

Retry a job from DLQ:

```powershell
python queuectl.py dlq retry job1
```

Architecture
- SQLite (data/queue.db) stores jobs and config.
- Workers claim a single pending job atomically using a BEGIN IMMEDIATE transaction and update it to `processing`.
- On failure, attempts are incremented. If attempts > max_retries the job is moved to `dead` (DLQ). Otherwise it is rescheduled with next_run = now + backoff_base ** attempts.

Assumptions & trade-offs
- Workers are threads inside the main process by default; `--daemon` starts OS-level background processes.
- Claiming uses SQLite transactions for simplicity (may serialize writes under high concurrency).
- No authentication, limited monitoring. Intended as a simple, robust baseline.

Testing
- A quick test script is included: `scripts/test_flow.py`.

```powershell
python scripts/test_flow.py
```

This will enqueue a successful and a failing job and run a worker for a short time. Inspect `data/queue.db` with sqlite3 to see job states.

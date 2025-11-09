import time
import sqlite3
from typing import Optional, Dict, Any, List
from .db import get_conn, init_db
from datetime import datetime


def _now_iso():
    return datetime.utcnow().isoformat() + "Z"


def init(db_path: str):
    init_db(db_path)


def enqueue_job(db_path: str, job: Dict[str, Any]):
    conn = get_conn(db_path)
    cur = conn.cursor()
    now = _now_iso()
    jid = job.get("id")
    if not jid:
        raise ValueError("job must have id")
    command = job.get("command", "")
    state = job.get("state", "pending")
    attempts = int(job.get("attempts", 0))
    max_retries = int(job.get("max_retries", 3))
    cur.execute(
        "INSERT OR REPLACE INTO jobs(id, command, state, attempts, max_retries, created_at, updated_at) VALUES(?,?,?,?,?,?,?)",
        (jid, command, state, attempts, max_retries, now, now),
    )
    conn.commit()


def list_jobs(db_path: str, state: Optional[str] = None) -> List[Dict[str, Any]]:
    conn = get_conn(db_path)
    cur = conn.cursor()
    if state:
        cur.execute("SELECT * FROM jobs WHERE state=? ORDER BY created_at", (state,))
    else:
        cur.execute("SELECT * FROM jobs ORDER BY created_at")
    rows = cur.fetchall()
    out = []
    for r in rows:
        out.append({k: r[k] for k in r.keys()})
    return out


def get_job(db_path: str, job_id: str) -> Optional[Dict[str, Any]]:
    conn = get_conn(db_path)
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
    r = cur.fetchone()
    if not r:
        return None
    return {k: r[k] for k in r.keys()}


def summary(db_path: str):
    conn = get_conn(db_path)
    cur = conn.cursor()
    cur.execute("SELECT state, COUNT(*) as c FROM jobs GROUP BY state")
    rows = cur.fetchall()
    d = {"pending": 0, "processing": 0, "completed": 0, "failed": 0, "dead": 0}
    for r in rows:
        d[r[0]] = r[1]
    return d


def claim_job(db_path: str, now_ts: float = None) -> Optional[Dict[str, Any]]:
    """Atomically claim one pending job (with next_run <= now) and set it to processing.
    Returns the job row or None."""
    if now_ts is None:
        now_ts = time.time()
    conn = get_conn(db_path)
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        # find a pending job ready to run
        cur.execute(
            "SELECT id FROM jobs WHERE state='pending' AND (next_run IS NULL OR next_run <= ?) ORDER BY created_at LIMIT 1",
            (now_ts,),
        )
        r = cur.fetchone()
        if not r:
            conn.commit()
            return None
        jid = r[0]
        now = _now_iso()
        cur.execute(
            "UPDATE jobs SET state='processing', updated_at=? WHERE id=?",
            (now, jid),
        )
        conn.commit()
        return get_job(db_path, jid)
    except sqlite3.OperationalError:
        try:
            conn.rollback()
        except Exception:
            pass
        return None


def complete_job(db_path: str, job_id: str):
    conn = get_conn(db_path)
    cur = conn.cursor()
    now = _now_iso()
    cur.execute("UPDATE jobs SET state='completed', updated_at=? WHERE id=?", (now, job_id))
    conn.commit()


def fail_job(db_path: str, job_id: str, last_error: str, backoff_base: float = 2.0):
    """Increment attempts and reschedule or move to dead."""
    conn = get_conn(db_path)
    cur = conn.cursor()
    cur.execute("SELECT attempts, max_retries FROM jobs WHERE id=?", (job_id,))
    r = cur.fetchone()
    if not r:
        return
    attempts, max_retries = r[0], r[1]
    attempts = attempts + 1
    now = _now_iso()
    if attempts > max_retries:
        cur.execute(
            "UPDATE jobs SET state='dead', attempts=?, updated_at=?, last_error=? WHERE id=?",
            (attempts, now, last_error, job_id),
        )
    else:
        delay = (backoff_base ** attempts)
        next_run = time.time() + delay
        cur.execute(
            "UPDATE jobs SET state='pending', attempts=?, updated_at=?, next_run=?, last_error=? WHERE id=?",
            (attempts, now, next_run, last_error, job_id),
        )
    conn.commit()


def retry_dead_job(db_path: str, job_id: str) -> bool:
    conn = get_conn(db_path)
    cur = conn.cursor()
    cur.execute("SELECT id FROM jobs WHERE id=? AND state='dead'", (job_id,))
    if not cur.fetchone():
        return False
    now = _now_iso()
    cur.execute(
        "UPDATE jobs SET state='pending', attempts=0, updated_at=?, next_run=NULL, last_error=NULL WHERE id=?",
        (now, job_id),
    )
    conn.commit()
    return True

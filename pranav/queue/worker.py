import threading
import subprocess
import time
import traceback
from .store import claim_job, complete_job, fail_job


class Worker(threading.Thread):
    def __init__(self, db_path: str, worker_id: str, backoff_base: float = 2.0, poll_interval: float = 1.0):
        super().__init__()
        self.db_path = db_path
        self.worker_id = worker_id
        self.backoff_base = backoff_base
        self.poll_interval = poll_interval
        # avoid naming collision with Thread._stop
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def run(self):
        while not self._stop_event.is_set():
            job = claim_job(self.db_path)
            if not job:
                time.sleep(self.poll_interval)
                continue
            jid = job["id"]
            cmd = job["command"]
            try:
                # execute command in shell
                proc = subprocess.run(cmd, shell=True)
                rc = proc.returncode
            except Exception as e:
                rc = 1
                err = f"exception: {e}\n{traceback.format_exc()}"
            else:
                err = None

            if rc == 0:
                complete_job(self.db_path, jid)
            else:
                last_error = err or f"exit_code:{rc}"
                fail_job(self.db_path, jid, last_error, backoff_base=self.backoff_base)

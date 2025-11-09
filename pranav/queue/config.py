import json
from .db import get_conn


def load_config(db_path: str):
    conn = get_conn(db_path)
    cur = conn.cursor()
    cur.execute("SELECT key, value FROM config")
    rows = cur.fetchall()
    cfg = {}
    for k, v in rows:
        try:
            cfg[k] = json.loads(v)
        except Exception:
            cfg[k] = v
    # defaults
    if "max_retries" not in cfg:
        cfg["max_retries"] = 3
    if "backoff_base" not in cfg:
        cfg["backoff_base"] = 2
    return cfg


def set_config(db_path: str, key: str, value):
    conn = get_conn(db_path)
    cur = conn.cursor()
    v = json.dumps(value)
    cur.execute("INSERT OR REPLACE INTO config(key, value) VALUES(?,?)", (key, v))
    conn.commit()

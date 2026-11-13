"""A SQLite-backed, fenced work queue for processes sharing one local disk."""
import argparse
import json
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path


class Conflict(Exception):
    """The requested transition conflicts with persisted state."""


class Queue:
    def __init__(self, path, clock=time.time):
        self.path = Path(path)
        self.clock = clock
        with self.connection() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS jobs(
                    id INTEGER PRIMARY KEY, request_key TEXT NOT NULL UNIQUE, payload TEXT NOT NULL,
                    max_attempts INTEGER NOT NULL, state TEXT NOT NULL, ready REAL NOT NULL,
                    generation INTEGER NOT NULL DEFAULT 0, owner TEXT, expires REAL, result TEXT);
                CREATE INDEX IF NOT EXISTS ready_jobs ON jobs(state, ready, id);
                CREATE INDEX IF NOT EXISTS expired_leases ON jobs(state, expires);
                CREATE TABLE IF NOT EXISTS history(
                    seq INTEGER PRIMARY KEY, job INTEGER NOT NULL REFERENCES jobs(id),
                    generation INTEGER NOT NULL, event TEXT NOT NULL, at REAL NOT NULL);
                CREATE INDEX IF NOT EXISTS job_history ON history(job, seq);
            """)

    @contextmanager
    def connection(self):
        db = sqlite3.connect(self.path, isolation_level=None, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA synchronous=FULL")
            yield db
        finally:
            db.close()

    @contextmanager
    def transaction(self):
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db, self.clock()
                db.commit()
            except BaseException:
                db.rollback()
                raise

    @staticmethod
    def log(db, job, generation, event, now):
        db.execute("INSERT INTO history(job,generation,event,at) VALUES(?,?,?,?)",
                   (job, generation, event, now))

    def submit(self, key, payload, attempts=3):
        if not key or type(attempts) is not int or attempts < 1:
            raise ValueError("provide a nonempty request key and positive attempt limit")
        encoded = json.dumps(payload, sort_keys=True, allow_nan=False, separators=(",", ":"))
        with self.transaction() as (db, now):
            old = db.execute("SELECT * FROM jobs WHERE request_key=?", (key,)).fetchone()
            if old:
                if old["payload"] != encoded or old["max_attempts"] != attempts:
                    raise Conflict("request key already used with different input")
                return old["id"]
            job = db.execute("""INSERT INTO jobs(request_key,payload,max_attempts,state,ready)
                VALUES(?,?,?,'queued',?)""", (key, encoded, attempts, now)).lastrowid
            self.log(db, job, 0, "submitted", now)
            return job

    @staticmethod
    def duration(value):
        if type(value) is not int or not 1 <= value <= 86400:
            raise ValueError("lease duration must be 1 to 86400 seconds")

    def claim(self, owner, seconds=30):
        self.duration(seconds)
        if not owner:
            raise ValueError("worker name required")
        with self.transaction() as (db, now):
            expired = db.execute("""SELECT id,generation,max_attempts FROM jobs
                WHERE state='leased' AND expires<=? ORDER BY expires,id LIMIT 64""", (now,)).fetchall()
            for row in expired:
                terminal = row["generation"] >= row["max_attempts"]
                db.execute("UPDATE jobs SET state=?, ready=?, owner=NULL, expires=NULL WHERE id=?",
                           ("dead" if terminal else "queued", now, row["id"]))
                self.log(db, row["id"], row["generation"], "expired_dead" if terminal else "expired", now)
            row = db.execute("""SELECT * FROM jobs WHERE state='queued' AND ready<=?
                                ORDER BY ready,id LIMIT 1""", (now,)).fetchone()
            if row is None:
                return None
            generation = row["generation"] + 1
            db.execute("UPDATE jobs SET state='leased', generation=?, owner=?, expires=? WHERE id=?",
                       (generation, owner, now + seconds, row["id"]))
            self.log(db, row["id"], generation, "claimed", now)
            return dict(id=row["id"], payload=json.loads(row["payload"]), owner=owner,
                        generation=generation, expires=now + seconds)

    def transition(self, job, owner, generation, operation, *, seconds=30, result=None):
        if operation not in ("renew", "complete", "fail"):
            raise ValueError("unknown transition")
        if operation == "renew":
            self.duration(seconds)
        with self.transaction() as (db, now):
            row = db.execute("""SELECT * FROM jobs WHERE id=? AND state='leased' AND owner=?
                              AND generation=? AND expires>?""", (job, owner, generation, now)).fetchone()
            if row is None:
                raise Conflict("lease expired, replaced, or closed")
            if operation == "renew":
                db.execute("UPDATE jobs SET expires=? WHERE id=?", (max(row["expires"], now + seconds), job))
                event = "renewed"
            else:
                state = "done" if operation == "complete" else (
                    "dead" if generation >= row["max_attempts"] else "queued")
                delay = min(300, 2 ** min(generation - 1, 9)) if state == "queued" else 0
                value = json.dumps(result, allow_nan=False) if operation == "complete" else None
                db.execute("UPDATE jobs SET state=?,ready=?,owner=NULL,expires=NULL,result=? WHERE id=?",
                           (state, now + delay, value, job))
                event = {"done": "completed", "queued": "retry_scheduled", "dead": "failed_dead"}[state]
            self.log(db, job, generation, event, now)

    def inspect(self, job):
        with self.connection() as db:
            db.execute("BEGIN")
            row = db.execute("SELECT * FROM jobs WHERE id=?", (job,)).fetchone()
            if row is None:
                raise KeyError(job)
            data = dict(row)
            for field in ("payload", "result"):
                if data[field] is not None:
                    data[field] = json.loads(data[field])
            data["history"] = [dict(r) for r in db.execute(
                "SELECT generation,event,at FROM history WHERE job=? ORDER BY seq", (job,))]
            return data


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--db", default="queue.sqlite")
    sub = cli.add_subparsers(dest="cmd", required=True)
    submit = sub.add_parser("submit")
    submit.add_argument("key")
    submit.add_argument("payload", type=json.loads)
    submit.add_argument("--attempts", type=int, default=3)
    claim = sub.add_parser("claim")
    claim.add_argument("owner")
    claim.add_argument("--seconds", type=int, default=30)
    for operation in ("complete", "fail", "renew", "inspect"):
        cmd = sub.add_parser(operation)
        cmd.add_argument("job", type=int)
        if operation != "inspect":
            cmd.add_argument("owner")
            cmd.add_argument("generation", type=int)
        if operation == "renew":
            cmd.add_argument("--seconds", type=int, default=30)
        if operation == "complete":
            cmd.add_argument("--result", type=json.loads)
    args = cli.parse_args()
    try:
        q = Queue(args.db)
        if args.cmd == "submit":
            out = q.submit(args.key, args.payload, args.attempts)
        elif args.cmd == "claim":
            out = q.claim(args.owner, args.seconds)
        elif args.cmd == "inspect":
            out = q.inspect(args.job)
        else:
            q.transition(args.job, args.owner, args.generation, args.cmd,
                         seconds=getattr(args, "seconds", 30), result=getattr(args, "result", None))
            out = {"accepted": True}
        print(json.dumps(out, indent=2))
    except (Conflict, ValueError, KeyError, sqlite3.Error) as exc:
        cli.exit(2, f"leasepost: {exc}\n")


if __name__ == "__main__":
    main()

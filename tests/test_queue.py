import json
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from leasepost import Conflict, Queue


class QueueTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.db = Path(tmp.name) / "test.sqlite"
        self.now = 100
        self.queue = Queue(self.db, lambda: self.now)
        self.job = self.queue.submit("tile-7", {"tile": 7}, 2)

    def test_submit_deduplicates_across_restart(self):
        fresh = Queue(self.db)
        self.assertEqual(fresh.submit("tile-7", {"tile": 7}, 2), self.job)
        with self.assertRaises(Conflict):
            fresh.submit("tile-7", {"tile": 8}, 2)

    def test_competing_claims_and_fencing(self):
        with ThreadPoolExecutor(max_workers=12) as pool:
            leases = [lease for lease in pool.map(self.queue.claim, map(str, range(12))) if lease]
        self.assertEqual(len(leases), 1)
        owner = leases[0]["owner"]
        self.now += 30
        for op in ("complete", "fail", "renew"):
            with self.assertRaises(Conflict):
                self.queue.transition(self.job, owner, 1, op)
        self.assertEqual(self.queue.claim(owner)["generation"], 2)
        with self.assertRaises(Conflict):
            self.queue.transition(self.job, owner, 1, "complete")
        self.queue.transition(self.job, owner, 2, "complete", result={"ok": True})
        row = self.queue.inspect(self.job)
        self.assertEqual(row["result"], {"ok": True})
        self.assertEqual([e["event"] for e in row["history"]],
                         ["submitted", "claimed", "expired", "claimed", "completed"])

    def test_backoff_and_dead_letter(self):
        self.queue.claim("a")
        self.queue.transition(self.job, "a", 1, "fail")
        self.assertIsNone(self.queue.claim("b"))
        self.now += 1
        self.queue.claim("b")
        self.queue.transition(self.job, "b", 2, "fail")
        self.assertIsNone(self.queue.claim("c"))
        self.assertEqual(self.queue.inspect(self.job)["state"], "dead")

    def test_renewal_and_terminal_expiry(self):
        self.queue.claim("a", 5)
        self.now += 4
        self.queue.transition(self.job, "a", 1, "renew", seconds=20)
        self.now += 19
        self.assertIsNone(self.queue.claim("b"))
        self.now += 1
        self.queue.claim("b", 1)
        self.now += 1
        self.assertIsNone(self.queue.claim("c"))
        self.assertEqual(self.queue.inspect(self.job)["history"][-1]["event"], "expired_dead")

    def test_killed_worker_recovery(self):
        worker = subprocess.Popen([sys.executable, "-u", "-c",
            "import json,sys,time; from leasepost import Queue; "
            "print(json.dumps(Queue(sys.argv[1],lambda:100).claim('worker',5))); time.sleep(60)",
            str(self.db)], stdout=subprocess.PIPE, text=True)
        try:
            self.assertEqual(json.loads(worker.stdout.readline())["generation"], 1)
        finally:
            worker.kill()
            worker.wait(timeout=5)
            worker.stdout.close()
        self.now = 105
        self.assertEqual(self.queue.claim("replacement")["generation"], 2)

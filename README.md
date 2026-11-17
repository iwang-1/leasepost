# LeasePost

[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/) [![dependencies: standard library only](https://img.shields.io/badge/dependencies-stdlib_only-brightgreen)](#)

A durable background-job queue for worker processes sharing one local disk. The hard case is a paused worker: its lease expires, a replacement finishes the job, then the original worker wakes up and tries to publish an obsolete result. LeasePost checks a generation number and the lease deadline in the same transaction that stores the result.

## At a glance

- **Fenced leases.** A paused worker whose lease expired cannot overwrite the result of the worker that replaced it: generation and deadline are checked in the same transaction that stores the result.
- **Contention-tested.** **256** job histories validated across **8** concurrent worker processes; tests race **12** claims for one job and kill a real process right after its claim commits.
- **Exactly-once redrive.** **8** racing redrives of one dead job extend its attempt budget exactly once.
- Exponential retry, dead-letter state and an ordered audit history per job, all on SQLite WAL.

## Try a job

With Python 3.11+ and its built-in SQLite module, run these commands from a fresh checkout:

```sh
python3 leasepost.py submit tile-7 '{"tile":7}'
python3 leasepost.py claim renderer --seconds 60
python3 leasepost.py complete 1 renderer 1 --result '{"pixels":4096}'
python3 leasepost.py inspect 1
```

The final record includes the payload, result and ordered transition history. A repeated submit with identical inputs returns the original ID. Reusing its key with different payload or attempt settings raises a conflict. Run all commands against the same database. Use the global flag `--db PATH` to select another file.

## Execution contract

| Operation | Behavior |
| --- | --- |
| Claim | Select the earliest eligible ready time, breaking ties by job ID. Increment generation and assign owner and deadline. |
| Renew | A live owner can extend its current generation. |
| Complete | Persist the result and close the lease atomically. |
| Fail | Schedule exponential retry starting at one second, capped at five minutes. At the attempt limit, move to the dead state. |
| Recover | Each claim sweeps up to 64 expired leases. A retryable expired lease becomes eligible immediately. Continue polling after an empty claim. |

Each state transition and audit event share a SQLite transaction. Writers acquire the write lock before sampling the clock. WAL mode permits concurrent readers, and synchronous FULL is requested on every connection. Expiration uses host wall time and assumes a reasonably stable clock. The queue is designed for local storage and trusted workers. Delivery is at least once across lease expiry. External effects such as sending an email need their own idempotency mechanism. Generation fencing protects writes to this queue.

## Reproduce the evidence

```sh
python3 -m unittest discover -v
python3 experiment.py --jobs 256 --workers 8
```

The tests race twelve claim attempts for one job, exercise the exact expiry boundary with a controlled clock, reject a stale generation even when the worker name is reused, and kill a real child process after its claim commits. The separate contention experiment checks every result and its event sequence in a fresh database. The [recorded run](evidence/contention.json) validated 256 job histories with eight worker-process slots. This is a bounded correctness experiment. It does not measure throughput or cover machine power loss.

### Recover an exhausted job

Inspect the dead job, then run `python3 leasepost.py redrive JOB_ID FAILED_GENERATION --extra-attempts 1`. The queue atomically grants the extra attempt and records the specific failure generation. Repeating the command, even after the job succeeds, returns its recorded disposition. A conflicting attempt limit fails. A replacement lease always receives a higher generation. The contention regression races eight redrives of one dead job and checks that exactly one extends the attempt budget.

# Solution

## What was broken

Idempotency used a check-then-insert sequence against a non-unique index. Two
concurrent deliveries could both pass the check, and the event, call update,
and aggregate update were separate statements, so a retry after a partial
failure could either double-count or be incorrectly ignored. Ingestion now
claims `event_id` through a unique Postgres index and performs the claim, call
upsert, and stats increment in one transaction. A duplicate claim changes
nothing and still receives a successful response.

The in-memory cache wrote to a map without taking its mutex, causing races and
lost increments. Writes are now locked, and durable account totals are loaded
from Postgres when the service starts. Recording work used the HTTP request
context after the handler returned, so it was normally cancelled before the
database update; its error was also discarded. Recording jobs now use a
service-owned context, log failures, participate in graceful shutdown, and are
resumed from unprocessed Postgres rows after a restart.

## Deduplication choice

Postgres is the source of truth for every side effect, so a unique constraint
plus a transaction gives one atomic decision about both deduplication and the
resulting writes. A Redis `SETNX` key would add a second failure domain: an
expired or evicted key could admit a duplicate, while a key written before a
failed Postgres transaction could suppress a valid retry. An application lock
would not protect multiple instances. The migration removes pre-existing
duplicate event rows before adding the unique index; previously inflated
historical aggregates would still need a one-time reconciliation from calls.

## At 10,000 webhooks/second

I would keep the database uniqueness guarantee but make the request path a
small durable inbox write, then process calls, aggregates, and recordings with
partitioned workers through an outbox or durable broker. I would batch writes,
tune and observe the connection pool, partition high-volume tables, add
backpressure and queue-depth/error/latency metrics, and load-test failure and
retry scenarios. Recording workers would use leases with `SKIP LOCKED` (or
broker acknowledgements) so multiple instances cannot perform the same work,
and the per-process stats cache would become a shared or explicitly
invalidated read model.

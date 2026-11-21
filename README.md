# Ridgeline

A Go HTTP time-series database for signed millisecond measurements, correction
merges, fixed windows and counter rates. Data lives in a bbolt database file;
compressed blocks, deletion intervals and materialized aggregates share the
same atomic storage boundary.

## Run

Requires Go 1.25 or newer and Python 3 for the optional production walkthrough.
Dependencies are pinned in go.mod/go.sum. After the first dependency download,
the service and demonstrations require no external services.

```sh
go build -o build/ridgeline ./cmd/ridgeline
RIDGE_DB=var/ridgeline.db RIDGE_ADDR=127.0.0.1:8399 ./build/ridgeline
```

The address defaults to loopback. Run behind a trusted network boundary.
SIGTERM stops new HTTP requests, drains active requests for up to ten seconds,
then closes the database. A second process cannot open the same database file.

```sh
python3 examples/walkthrough.py --workflow all
python3 examples/walkthrough.py --output var/observations.json
```

The bounded walkthrough uses temporary volumes and an available ephemeral port.
Each named workflow runs the entire related stateful scenario so its required
preconditions and recovery remain reproducible. Formal tests are deferred.
`examples/input.json` contains the original measurements; a recorded production
session is in `examples/session.json`. These are synthetic, non-sensitive inputs.

## API

All routes start with `/ridge/v1`. JSON request fields are strict; bodies have
an 8 MiB limit. Failures return a stable code/detail object. Invalid requests
are 400, missing objects 404, state conflicts 409 and capacity rejection 422.
All methods below return 200 on success.

| Method | Route | Body / Result |
| --- | --- | --- |
| GET | `/health` | readiness |
| POST | `/spaces` | namespace policy, as in input.json |
| GET | `/spaces/{space}` | policy, watermark, floor, source sequence |
| PUT | `/spaces/{space}/mode` | `{"read_only":true}` |
| POST | `/spaces/{space}/points` | `{"now":10000,"points":[...]}` |
| POST | `/spaces/{space}/series` | `{"matchers":[]}` |
| POST | `/spaces/{space}/range` | time interval, matchers, optional limit |
| POST | `/spaces/{space}/aggregate` | interval, step, origin, function, group_by |
| POST | `/spaces/{space}/explain` | interval and matchers |
| POST | `/spaces/{space}/seal` | empty object, or `{"batch_points":N}` for a resumable job |
| GET | `/spaces/{space}/seal` | persistent seal-job status and progress |
| GET | `/spaces/{space}/blocks` | physical block inventory |
| POST | `/spaces/{space}/deletions` | interval and matchers |
| GET | `/spaces/{space}/deletions` | deletion sequence inventory |
| POST | `/spaces/{space}/retention` | `{"now":11000}` |
| POST | `/spaces/{space}/compact` | empty object |
| PUT | `/spaces/{space}/rollups/{name}` | aggregate query definition |
| GET | `/spaces/{space}/rollups/{name}` | stored result and stale flag |
| GET | `/image` | consistent binary image, SHA-256 and sequence headers |

Sample query:

```json
{"start":-5000,"end":10000,"step":1000,"origin":0,
 "group_by":["zone"],"function":"avg",
 "matchers":[{"label":"__name__","op":"=","value":"load_ratio"}]}
```

Matchers are ANDed and regexes are fully anchored. Empty/missing labels match
the same string, but series identity preserves their distinction. Raw results
are sorted by series ID, then timestamp, and expose truncation. Aggregate
queries always use all admitted source points and ignore raw-result limits.
Explain is an estimate of candidate blocks, not a guarantee of visible rows.

### Batched seal jobs

An empty-object `seal` call keeps the original one-shot, whole-head
transaction. Posting `{"batch_points":N}` instead runs seal as a persistent,
resumable **job**: the first call snapshots the current head as the job's
initial backlog and moves at most N points into blocks, and every later call
advances the job by one more batch. The job record lives in the database's
`sealjobs` bucket, so a process restart loses no progress; repeat calls simply
continue. Each batch is one bbolt transaction — a crash either commits the
whole batch or none of it, so no point is lost or duplicated.

Writes remain allowed between batches. Progress separates the points present
at job start (`initial_total`, `initial_sealed`, `initial_pending`) from data
added while the job runs (`added_sealed`, `added_pending`); same-timestamp
corrections, including to points already sealed, never inflate the new-work
count and are reported separately as `corrections_sealed`. A correction always
keeps its newer sequence, so an older sealed value can never override it;
queries keep merging head and blocks in one read transaction with unchanged
visibility semantics throughout. Compaction and retention refuse to run while
a seal job is active, and finish by leaving no dangling job.

```sh
python3 examples/walkthrough.py --workflow seal-job
```

The workflow interleaves batches with process restarts, a late correction and
a backfill, then asserts the block inventory and range results after the job
completes and after compaction.

## Storage And Recovery

`ridge-v1/spaces/<name>` contains config, catalog, head, blocks, payloads,
deletes, rollups and sealjobs buckets. Numeric point keys encode signed time in
lexicographic order. A namespace-level head counter tracks the live head key
count inside each transaction; the sealjobs bucket holds the active batched-seal
job record together with its added-key marker bucket and progress counters.
Blocks use format 1: a gzip stream containing an ordered JSON sample array,
with SHA-256 over the compressed payload.

Keep a downloaded image and its response hash together. Verify the bytes,
stop any process using the destination, then start a separate instance with
RIDGE_DB pointing to the verified image. Compare namespace floors, sequence,
raw points and materialization freshness before directing producers there.
The walkthrough demonstrates this sequence on an isolated restored volume.
An image has the source identity and includes every namespace.

The server currently holds backup bytes in memory, compacts a namespace in one
write transaction and scans catalog entries before selecting series. Query
source work is bounded by max_scan; maintenance shares that budget and may
reject an oversized operation without changing the database. These are explicit
single-node resource boundaries, not promises of unlimited ingestion.

See PROJECT_SPEC.md for exact arithmetic, late-data, counter and retention
semantics. There is no background retention job; clients explicitly advance it.

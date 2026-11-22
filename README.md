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
go test ./...
python3 examples/walkthrough.py --workflow all
python3 examples/walkthrough.py --output var/observations.json
```

The bounded walkthrough uses temporary volumes and an available ephemeral port.
Each named workflow runs the entire related stateful scenario so its required
preconditions and recovery remain reproducible. `--workflow drain` is included
in `all`; it demonstrates write/delete/retention/materialization rejection,
reads and physical maintenance during a drain, drain cancellation, drain
completion and restart inspection. The Go tests cover the same state machine.
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
| GET | `/spaces/{space}` | policy, watermark, floor, source sequence, lifecycle |
| GET | `/spaces/{space}/drain` | lifecycle state, phase, in-flight count, sequences |
| POST | `/spaces/{space}/drain` | stop accepting logical mutations (idempotent) |
| POST | `/spaces/{space}/drain/complete` | commit the maintainable state |
| DELETE | `/spaces/{space}/drain` | cancel the drain and accept writes again |
| PUT | `/spaces/{space}/mode` | `{"read_only":true}` or `{"state":"draining"}` |
| POST | `/spaces/{space}/points` | `{"now":10000,"points":[...]}` |
| POST | `/spaces/{space}/series` | `{"matchers":[]}` |
| POST | `/spaces/{space}/range` | time interval, matchers, optional limit |
| POST | `/spaces/{space}/aggregate` | interval, step, origin, function, group_by |
| POST | `/spaces/{space}/explain` | interval and matchers |
| POST | `/spaces/{space}/seal` | empty object |
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

## Namespace Drain

`writable -> draining -> read_only` separates "stop accepting" from "safe to
maintain". Reads and physical maintenance (seal, compact) work in every state.
Logical mutations (points, deletions, retention, materialization) are accepted
only while writable, but a request admitted just before a drain keeps an
in-flight slot and is allowed to commit.

```sh
curl -X POST localhost:8399/ridge/v1/spaces/spindle/drain
curl localhost:8399/ridge/v1/spaces/spindle/drain
curl -X POST localhost:8399/ridge/v1/spaces/spindle/drain/complete
curl -X DELETE localhost:8399/ridge/v1/spaces/spindle/drain
```

`phase` is `accepting`, `stopping` (drain started, logical requests still in
flight), `quiesced` (no in-flight logical work, not yet completed),
`completing`, or `maintainable`. Completing while `in_flight > 0` is a 409 with
the remaining count; the completion commit therefore cannot be followed by a
late logical mutation. `PUT /mode {"read_only":true}` is the one-call form: it
starts the drain and completes it immediately when nothing is in flight, and
otherwise fails with a 409 while leaving the namespace `draining`.

Every transition has its own database sequence (`started_sequence`,
`completed_sequence`, `canceled_sequence`), so a drain is auditable. After a
restart the durable state and sequences survive; `in_flight` is 0 (requests do
not survive a process) and `restarted` is true. The process never completes or
cancels a drain on its own — inspect `GET /spaces/{space}`, then complete or
cancel deliberately.

## Storage And Recovery

`ridge-v1/spaces/<name>` contains config, catalog, head, blocks, payloads,
deletes and rollups buckets. Numeric point keys encode signed time in
lexicographic order. Blocks use format 1: a gzip stream containing an ordered
JSON sample array, with SHA-256 over the compressed payload.

Keep a downloaded image and its response hash together. Verify the bytes,
stop any process using the destination, then start a separate instance with
RIDGE_DB pointing to the verified image. Compare namespace floors, sequence,
raw points and materialization freshness before directing producers there.
The walkthrough demonstrates this sequence on an isolated restored volume and
records the post-restart drain status, which reports `restarted` with an empty
in-flight count until an operator completes or cancels the drain. An image has
the source identity and includes every namespace.

The server currently holds backup bytes in memory, compacts a namespace in one
write transaction and scans catalog entries before selecting series. Query
source work is bounded by max_scan; maintenance shares that budget and may
reject an oversized operation without changing the database. These are explicit
single-node resource boundaries, not promises of unlimited ingestion.

See PROJECT_SPEC.md for exact arithmetic, late-data, counter and retention
semantics. There is no background retention job; clients explicitly advance it.

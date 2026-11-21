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
| POST | `/spaces/{space}/seal` | empty object |
| GET | `/spaces/{space}/blocks` | physical block inventory |
| POST | `/spaces/{space}/deletions` | interval and matchers |
| GET | `/spaces/{space}/deletions` | deletion sequence inventory |
| POST | `/spaces/{space}/retention` | `{"now":11000}` |
| POST | `/spaces/{space}/compact` | empty object |
| PUT | `/spaces/{space}/rollups/{name}` | rollup definition, optionally derived from another rollup |
| GET | `/spaces/{space}/rollups/{name}` | stored result, coverage, stale flag and read rule |
| GET | `/spaces/{space}/rollups` | all tiers with granularity, coverage and read rule |
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

## Rollup Tiers

A rollup materializes an aggregate result with its source sequence. Tiers can be
stacked: a definition with `"source":{"rollup":"minute"}` reduces the stored
reduction state of another rollup instead of re-reading raw points.

```json
{"start":0,"end":3600000,"step":3600000,"origin":0,"group_by":["zone"],
 "function":"avg","source":{"rollup":"minute"}}
```

Count, sum, avg, min and max are tier-reducible. Every reducible cell stores
`{"count","sum","min","max","value"}`; a parent average is `sum/count`, so it is
weighted by raw observations and never an average of child averages. `first`,
`last` and `rate` depend on raw point order or adjacency and cannot seed a
derived tier. A derived tier must keep the source origin and grouping and use a
step that is an integer multiple of the source step; anything else is rejected
rather than silently mixing spans.

Each cell and result carries a `coverage` object. `expected_parts` /
`present_parts` count source windows that tiled the cell, `complete` is false
for a clipped edge window, `exact` says the stored value equaled a full-window
aggregation at materialization, and `source_expired` is refreshed against the
current retention floor each time the rollup is read. `recomputable` is true
only while retained raw points still exist for the window.

`read_rule` summarizes the rule a reader should apply, from most to least
severe: `derived_source_expired`, `derived_stale`, `source_expired`, `stale`,
`partial`, `derived_exact`, `exact`. Staleness compares the namespace sequence,
not the global sequence, so materializing one tier does not make another stale.
Inspection always reports staleness; it never presents a stale result as
current, and a stored tier is never silently rewritten.

## Storage And Recovery

`ridge-v1/spaces/<name>` contains config, catalog, head, blocks, payloads,
deletes and rollups buckets. Numeric point keys encode signed time in
lexicographic order. Blocks use format 1: a gzip stream containing an ordered
JSON sample array, with SHA-256 over the compressed payload.

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

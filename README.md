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
| PUT | `/spaces/{space}/rollups/{name}` | aggregate query definition |
| GET | `/spaces/{space}/rollups/{name}` | stored result and stale flag |
| PUT | `/spaces/{space}/calibrations/{name}` | piecewise linear calibration view |
| GET | `/spaces/{space}/calibrations/{name}` | one view with revision and sequence |
| GET | `/spaces/{space}/calibrations` | calibration view inventory |
| POST | `/spaces/{space}/calibrated-range` | range read projected through a view |
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

## Calibration Views

A calibration view applies piecewise linear coefficients
`calibrated = factor * raw + offset` at read time. It never rewrites a sample:
ordinary `/range` and `/aggregate` calls are byte-for-byte unchanged unless the
request explicitly names a view. Views are stored per namespace in the
`calibrations` bucket and are revisioned; replacing a view bumps the namespace
sequence, which marks any rollup materialized against the old coefficients
stale.

```json
{"rules":[{"matchers":[{"label":"__name__","op":"=","value":"probe_celsius"}],
  "segments":[
    {"start":0,"end":2000,"factor":1.0,"offset":0.0},
    {"start":2000,"end":10000,"factor":1.1,"offset":2.0}]}]}
```

Interval rules, all deterministic:

- Every interval is half-open `[start, end)`. A boundary timestamp belongs to
  the later segment, so adjacent segments join seamlessly with no double
  coverage. Overlapping segments inside one rule, or two rule selectors that
  match the same series, are rejected with 400 at view definition time.
- Time with no covering segment is returned with `calibrated:false`, the raw
  value passed through and identity coefficients (`factor=1, offset=0`), rather
  than being dropped or guessed.
- Coefficient selection uses the observation timestamp only, never ingest or
  arrival order, so late data is calibrated with the segment effective at the
  time the measurement was taken.
- Each calibrated point carries `raw_value`, `factor`, `offset` and
  `calibrated`, so every reading traces back to its original observation and
  the exact coefficient used.

Select a view by adding `"calibration":"<name>"` to a `/range`-style query
(use `/calibrated-range` for point-level provenance), an `/aggregate` request,
or a rollup definition. An unknown view name is a 404, never a silent raw
fallback. Aggregates and rollups reduce the projected values; raw points stay
immutable, so correcting an old coefficient changes calibrated history and
refreshed rollups while the raw range stays identical.

## Storage And Recovery

`ridge-v1/spaces/<name>` contains config, catalog, head, blocks, payloads,
deletes, rollups and calibrations buckets. Numeric point keys encode signed
time in lexicographic order. Blocks use format 1: a gzip stream containing an
ordered JSON sample array, with SHA-256 over the compressed payload.

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

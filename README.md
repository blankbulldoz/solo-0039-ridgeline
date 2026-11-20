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
| POST | `/spaces/{space}/vector` | two selectors, join labels, arithmetic operator |
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

### Vector queries

A vector query computes an arithmetic expression from two selectors inside one
bbolt read transaction, so numerator and denominator always come from the same
snapshot; the response reports the sequence of that snapshot. `on` lists the
labels that associate series. Series are grouped by the value of those labels,
absent labels joining as the empty string, exactly like matchers. No timestamp
interpolation or nearest-neighbour carry-forward happens: each row pairs
samples at the identical timestamp.

```json
{"start":0,"end":10000,"on":["rack"],"op":"/","relationship":"many_to_one",
 "left":{"matchers":[{"label":"__name__","op":"=","value":"busy_total"}]},
 "right":{"matchers":[{"label":"__name__","op":"=","value":"capacity"}]}}
```

`op` is one of `/ * + -`. Rows are left-anchored: a left series with no right
match still emits rows, and `right_only_keys` counts right-only keys that
produce no rows. `relationship` declares the permitted cardinality and is
enforced per join key:

| `relationship` | Allowed | Rejected |
| --- | --- | --- |
| `one_to_one` (default) | at most one series on each side | either side has 2+ series on a key |
| `many_to_one` | one shared right value for many left series | 2+ right series on a key |
| `one_to_many` | one left series over many right series | 2+ left series on a key |

A violation is a `409 conflict`, never a silently picked or multiplied pair.
Shared capacity (`many_to_one`) re-uses one right series for each left series;
the row budget matches the namespace `max_scan` and excess rows are rejected.

Every result row carries `left`, `right` and `value` as nullable numbers plus a
`status`. Undefined arithmetic never becomes a normal-looking number:
| `status` | Meaning |
| --- | --- |
| `ok` | both samples at that timestamp; `value` is the result |
| `missing_left` | the left series has no sample at that timestamp |
| `missing_right` | the right series has no sample at that timestamp |
| `zero_denominator` | `/` with a right sample equal to zero |

For non-`ok` rows `value` is `null`; the present side keeps its number. Results
are sorted by join key, then timestamp. `stats` counts `rows`, each status,
`joined_keys` (with a counterpart), `left_only_keys`/`right_only_keys` and
`misaligned_keys` (keys emitting a missing-side row). The walkthrough's vector
workflow demonstrates a shared-capacity `many_to_one` join, a one-to-one pair,
the two cardinality refusals, all three undefined statuses, then removes the
conflicting series and recomputes every row as `ok`.

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

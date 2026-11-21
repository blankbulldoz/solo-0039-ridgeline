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
| POST | `/spaces/{space}/range` | time interval, matchers, optional limit and partial |
| POST | `/spaces/{space}/aggregate` | interval, step, origin, function, group_by, optional partial |
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

### Partial reads

A range or aggregate request may set `"partial":true`. The default stays
strict: one unreadable block, or a source set that exceeds `max_scan`, fails
the whole request. In a partial read every selected series is reported with a
status: `complete`, `failed` (stored bytes did not verify) or `not_executed`
(the scan budget or raw limit stopped before the series was read). A series is
never reported with only some of its points; incomplete series carry an empty
point list and an error detail, so consumers can keep computing groups that do
not depend on them.

Aggregates in a partial read reduce only complete series. Any grouping key that
lost a source series is withheld from `cells` and listed in `withheld` with its
status and contributing series IDs, rather than being summed over an incomplete
source. This applies even when other series in that same group read fine: a
group is all-or-nothing. A single oversized series is skipped without hiding
the remaining series; the budget still bounds total decoded points. `partial`
is rejected for materialization, deletion and explain, which must treat their
source as a complete set; a rollup is never published from a partial read.

Sample response for healthy `zone=east` and `zone=west` series queried
alongside an unreadable `zone=west` series:

```json
{"sequence":7,"floor":-4503599627370496,"partial":true,
 "series":[
  {"series":{"id":"28...","labels":{"__name__":"load_ratio","node":"ridge-a","zone":"east"}},
   "points":[{"time":0,"value":1,"sequence":2},{"time":1000,"value":2,"sequence":2}],
   "status":"complete"},
  {"series":{"id":"0f...","labels":{"__name__":"load_ratio","node":"ridge-b","zone":"west"}},
   "points":[], "status":"failed",
   "error":"corrupt: block 00000000000000000003-0f... for series 0f... is unreadable: block 00000000000000000003-0f... checksum mismatch"}],
 "stats":{"series":2,"blocks":1,"head":0,"decoded":2,"visible":2,"tombstones":0,"failed":1},
 "truncated":false}
```

The same request with `"partial":true` and `"group_by":["zone"],"function":"sum"`
returns the east cells and `"withheld":[{"labels":{"zone":"west"},"status":"failed","series_ids":["0f..."]}]`.
`python3 examples/walkthrough.py --workflow partial` seeds a healthy and a
deliberately damaged sealed series and shows the strict failure beside the
partial response.

See PROJECT_SPEC.md for exact arithmetic, late-data, counter and retention
semantics. There is no background retention job; clients explicitly advance it.

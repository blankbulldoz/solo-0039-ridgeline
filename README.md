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
| PUT | `/spaces/{space}/rules/{rule}` | new rule revision, source or derived |
| GET | `/spaces/{space}/rules` | current rules, geometry and dependents |
| GET | `/spaces/{space}/rules/{rule}` | current revision and direct dependents |
| GET | `/spaces/{space}/rules/{rule}/revisions` | every stored revision |
| DELETE | `/spaces/{space}/rules/{rule}` | retire an unreferenced rule |
| POST | `/spaces/{space}/rules/{rule}/runs` | execute the dependency chain |
| GET | `/spaces/{space}/rules/{rule}/runs` | recorded runs for that root |
| GET | `/spaces/{space}/runs` · `/runs/{run}` | run summaries · stored run provenance |
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

## Recording Rules

A recording rule names either a source definition — matchers, a reducer, a
window step and origin, and grouping labels — or a derived definition that
combines the cells of other rules. Rules are versioned: every `PUT` appends an
immutable revision and revisions are never rewritten.

```json
{"kind":"source","source":{"matchers":[{"label":"__name__","op":"=","value":"cpu"}],
 "function":"sum","step":1000,"origin":0,"group_by":["node"]}}
{"kind":"derived","inputs":["per_node_rules","total_rules"],"combine":"max"}
```

Executing a root rule (`POST /rules/{rule}/runs` with `start`, `end` and an
optional `max_points`) resolves the whole dependency closure before reading
data and then evaluates it inside one bbolt read transaction. Every node sees
one sequence and one set of visible samples, so a single execution cannot mix
data from different ingestion batches. A source rule referenced by several
derived rules is computed and decoded once; `order` lists the closure in
dependency order, and each node reports `references` and `shared`. Cycles,
missing references, mismatched window geometry and a decoded-point budget
overrun fail the whole run with a typed error instead of a partial result.

`record:true` stores the run with the revision, the frozen definition and the
output cells of every node. `GET /runs/{run}` compares the executed revisions
with the current ones and reports `data_stale` and `definition_stale`
separately, so an old result always names the definitions it actually used,
even after a rule is changed or retired. Derived rules must share the window
step and origin of their inputs and declare `combine` as `sum`, `min`, `max`,
`avg` or `count`; with an empty `group_by` the input cell labels are preserved.
Deleting a rule that is still referenced is a conflict, and a reintroduced
name continues its revision numbering.

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

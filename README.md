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
python3 examples/walkthrough.py --workflow relabel
python3 examples/relabel_migration.py
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
| POST | `/spaces/{space}/relabel/preview` | read-only migration plan for one renamed label |
| POST | `/spaces/{space}/relabel` | execute the plan, or record a refusal |
| GET | `/spaces/{space}/relabel/jobs` | migration attempt inventory |
| GET | `/spaces/{space}/relabel/jobs/{job}` | one attempt and whether it completed |
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

## Label Migration

A namespace can rename one label key across its stored history; the shipped
example moves `host` to `node`. New ingestion alone cannot do this, because
existing series identities, blocks, deletion intervals and rollups all persist
the old key. `POST /spaces/{space}/relabel/preview` resolves the whole namespace
and reports, without writing:

- `changes`: each source series and the identity it becomes;
- `merges`: targets reached by more than one source identity;
- `collisions`: timestamps where a merge would put two committed values on one
  identity, with every candidate value and commit sequence;
- `label_conflicts`: series that already carry the target key with a different
  value;
- `rollups`: stored definitions that reference the renamed key.

A series without the source key keeps its identity and samples unchanged; it
never gains an empty target label. A source value that is explicitly empty is a
present label and moves as empty. Two series that become one identity are a
merge. If a merged identity collides at a timestamp, the job is refused unless a
`value_conflict` policy is supplied:

| `value_conflict` | Behavior |
| --- | --- |
| `reject` (default) | refuse the whole job |
| `keep_first` | oldest commit sequence wins |
| `keep_last` | newest commit sequence wins |
| `deduplicate_identical` | accept only repeated equal values |

`label_conflict` handles a series whose target key already holds a different
value: `reject` (default), `keep_target`, or `prefer_source`.

Execution takes the namespace offline in two committed phases. The first records
a `running` job and a pause marker; from that commit, ingestion, sealing,
deletion, retention, compaction and rollup publication are rejected with 409.
The second rewrites the namespace and replaces the running record with the
completed one in the same bbolt transaction. A process that dies between the
phases leaves a pause marker and a running record; opening the database marks
that attempt `failed`, clears the pause, and admits writes again, and no data
was rewritten. A refused plan writes a terminal `failed` record and changes
nothing. Repeating a completed job returns its recorded result.

Deletion intervals and rollups are resolved rather than left dangling.
Samples hidden by a deletion at migration time stay hidden (they are dropped
from the rewritten output), and the interval is detached from series identities
that no longer exist, so it cannot later hide a merged series' unrelated
samples. Rollup definitions and stored cell labels are repointed at the new key;
a cell that already has a conflicting target value is left alone and reported.
The migration advances the namespace sequence, so every stored rollup result
reads as stale until it is rematerialized. Blocks are republished under the new
identities from visible samples, preserving commit sequences; retention floors
are unchanged.

`examples/relabel_migration.py` runs the absent-label, explicit-empty and
merge-collision scenarios against the real API, including a database image
written by the pre-migration release. `cmd/relabel-drill` reproduces the paused
state of an interrupted job for recovery drills; it must not run while a server
holds the file.

## Storage And Recovery

`ridge-v1/spaces/<name>` contains config, catalog, head, blocks, payloads,
deletes, rollups and jobs buckets. The jobs bucket is created on open when an
older image lacks it, so pre-migration images keep working. Numeric point keys
encode signed time in lexicographic order. Blocks use format 1: a gzip stream containing an ordered
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

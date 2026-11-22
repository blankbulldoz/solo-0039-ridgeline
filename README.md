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

### Series identifier storage

The HTTP identifier for a series is always the 64-character lowercase
hexadecimal SHA-256 digest of its canonical label set. Two physical layouts
exist and are tracked by a root-level `storage_version` marker:

| Version | Catalog keys | Head point keys | Marker |
| --- | --- | --- | --- |
| V1 | 64-byte hexadecimal string | 72 bytes (64 hex ID + 8 time) | absent |
| V2 | 32-byte raw digest | 40 bytes (32 raw ID + 8 time) | `storage_version = 2` |

V2 stores the raw 32-byte digest internally while every HTTP payload (series
IDs, block `series_id`, tombstone targets, rollup results) keeps using the
64-character hexadecimal form, so wire clients observe no identifier change.
Head keys use a fixed-width 32-byte prefix plus an 8-byte timestamp suffix;
the timestamp is never parsed as part of the series identifier.

### Versioned V1 -> V2 migration

Opening a V1 database runs an automatic, version-gated migration inside one
bbolt write transaction, before the service accepts traffic:

1. catalog keys are rewritten from 64-byte hex to 32-byte raw keys (the JSON
   `id` value stays hexadecimal);
2. head keys are rewritten from 72 to 40 bytes using the fixed-width split;
3. block metadata and payloads are verified to resolve through the new
   catalog; block IDs and payload keys stay hexadecimal;
4. tombstone targets are verified against the new catalog (their JSON stays
   hexadecimal);
5. rollup materializations need no rewrite and are parse-validated, keeping
   their source-sequence provenance;
6. the root `storage_version = 2` marker is written once.

The migration consumes no domain sequence, so logical data, rollup freshness
and provenance are unchanged. Empty series retain their catalog identity. A
database already carrying the V2 marker is opened without touching data, and a
file stamped with a newer version is rejected rather than partially read.

Before any migration write, the original file is copied to
`<database>.pre-v2-mirror` (an existing mirror is never overwritten). The
migration is atomic: if the transaction fails it rolls back and the live file
remains a consistent V1 database, so after a failed startup the original file
is still the correct file to keep running — restart the same binary to retry,
or stop and replace the live file with the mirror to roll back to V1. The
mirror is the authoritative fallback if the live file is ever in doubt; keep
it until the upgraded deployment is verified. Remove it manually only after
that verification, since its presence is what makes rollback possible.

`go run ./cmd/ridge-inspect inspect <db>` reports the marker and the physical
key-length distribution without opening the database through the service, and
its `tamper` subcommand supports migration failure drills on scratch copies.
`python3 examples/migration/migration_drill.py` runs the full end-to-end
procedure: it builds the frozen baseline binary, creates a V1 database with
missing/empty labels, delete-then-repost and an empty series, performs the
upgrade and restart, exercises a failed migration, and restores from the
mirror.

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

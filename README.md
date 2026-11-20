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
| POST | `/spaces/{space}/histograms` | cumulative histogram observations |
| POST | `/spaces/{space}/series` | `{"matchers":[]}` |
| POST | `/spaces/{space}/range` | time interval, matchers, optional limit |
| POST | `/spaces/{space}/aggregate` | interval, step, origin, function, group_by |
| POST | `/spaces/{space}/histogram-range` | corrected raw histogram snapshots |
| POST | `/spaces/{space}/histogram-query` | windowed quantile estimates and limitations |
| POST | `/spaces/{space}/explain` | interval and matchers |
| POST | `/spaces/{space}/seal` | empty object |
| GET | `/spaces/{space}/blocks` | physical block inventory |
| GET | `/spaces/{space}/histogram-blocks` | physical histogram block inventory |
| POST | `/spaces/{space}/deletions` | interval and matchers |
| GET | `/spaces/{space}/deletions` | deletion sequence inventory |
| POST | `/spaces/{space}/retention` | `{"now":11000}` |
| POST | `/spaces/{space}/compact` | empty object |
| PUT | `/spaces/{space}/rollups/{name}` | aggregate query definition |
| GET | `/spaces/{space}/rollups/{name}` | stored result and stale flag |
| GET | `/image` | consistent binary image, SHA-256 and sequence headers |

Scalar sample query:

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

## Cumulative Histograms

Latency quantiles cannot be reconstructed from a few averages, so producers
may submit cumulative histogram snapshots next to scalar samples. Each
observation carries strictly increasing finite bucket upper bounds,
cumulative counts over those buckets and the cumulative sum of observed
values:

```json
{"now":30000,"histograms":[
  {"labels":{"__name__":"latency_ms","node":"edge-1"},"time":30000,
   "bounds":[10,25,50,100],"counts":[10,40,80,100],"sum":3000.0}
]}
```

Within one observation counts must be non-decreasing and bounds strictly
increasing. Across observations, a smaller cumulative count or sum means the
emitting counter restarted: the reset snapshot becomes the post-reset
baseline and is counted wholesale, never treated as a negative delta. A
histogram query returns origin-aligned windows with the reset-aware increase,
bucket-interpolated quantile estimates and explicit limitation codes.

A bucket layout is identified by a fingerprint of its upper bounds. Multiple
instances are summed inside a window only when fingerprints match. When
different layouts meet, the cell reports `compatible:false`, keeps each
scheme's partial counts under `schemes` and returns no merged estimate with
the `incompatible_bucket_layouts` limitation rather than adding incompatible
buckets. Estimates always carry `bucket_resolution_estimate` (linear
interpolation inside finite-width buckets) and `window_baseline_excluded`
(the first snapshot per window is a baseline, not an extrapolation); resets
and edge-bucket capping add their own codes.

Histograms share the scalar path for corrections (newest sequence wins per
series-timestamp), deletions, retention floors, read-only policy and the
scan budget. Sealing moves head snapshots into format-2 gzip blocks using the
same checksum envelope; compaction rewrites them with corrections and
deletions resolved.

## Storage And Recovery

`ridge-v1/spaces/<name>` contains config, catalog, head, blocks, payloads,
deletes, rollups, hist_head, hist_blocks and hist_payloads buckets. Numeric
point keys encode signed time in lexicographic order. Scalar blocks use
format 1 and histogram blocks format 2: both are a gzip stream containing an
ordered JSON array, with SHA-256 over the compressed payload.

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

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
| POST | `/spaces/{space}/exemplars` | interval, matchers, optional bounded limit |
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

## Exemplars

An exemplar attaches an observation to a trace at one series-time so a
dashboard spike can jump into tracing. A point may carry an optional
`exemplar` object:

```json
{"labels":{"__name__":"pulse_total","node":"ridge-a"},"time":1500,"value":96,
 "exemplar":{"trace_id":"4bf92f3577b34da6a3ce929d0e0e4736",
             "labels":{"route":"/checkout","shard":"a"}}}
```

Exemplars do not participate in series identity, head points, block payloads
or any aggregation; they are stored in a separate bucket keyed by series,
time, commit sequence and in-batch offset. One series-time therefore holds
many exemplars, and a write without an exemplar changes nothing about
attachments. `trace_id` is required (up to 128 safe characters) and context
labels are bounded to eight entries of at most 256 characters; invalid
exemplars reject the whole atomic request.

`POST /spaces/{space}/exemplars` takes a half-open interval, the same label
matchers as other queries and an optional `limit` (default 100, hard cap
1000). Dense spikes cannot return without bound: when matches exceed the
limit, deterministically spaced representatives are returned with
`truncated:true`, and `stats.omitted` accounts for the rest. Every match is
still classified in `stats` (`visible`, `hidden_tombstone`, `below_floor`,
`detached`). Each returned exemplar reports `anchor_visible` and
`anchor_sequence`, so association validity is directly inspectable:

- a deletion hides exemplars committed at or before the tombstone sequence;
  a later correction at the same time admits new, valid exemplars without
  resurrecting old ones;
- same-time corrections keep the exemplar attached while its `value` and
  `anchor_sequence` track the newest visible point;
- retention physically reclaims exemplars below the floor during the
  retention compaction, and explicit compaction reclaims tombstoned or
  detached associations; maintenance receipts count kept and dropped records;
- sealing moves samples into blocks but leaves exemplars in place; anchors
  resolve through head and blocks alike, and backup images include them.

## Storage And Recovery

`ridge-v1/spaces/<name>` contains config, catalog, head, blocks, payloads,
deletes, rollups and exemplars buckets. Numeric point keys encode signed
time in lexicographic order; exemplar keys append commit sequence and offset
so one moment can carry many attachments. Blocks use format 1: a gzip stream
containing an ordered JSON sample array, with SHA-256 over the compressed
payload. Opening a database created before exemplars existed adds the new
bucket automatically.

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

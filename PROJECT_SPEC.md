# Ridgeline

Ridgeline is a single-node time-series database exposed as an HTTP service.
Infrastructure producers submit finite numeric measurements, signed Unix
millisecond timestamps and label sets. Query clients read corrected series,
grouped fixed windows, reset-aware counter rates and materialized rollups.

The database owns namespaces, series identities, samples, compressed blocks,
deletion intervals, retention floors, rollup definitions and backup images.
It is a storage and query engine, not a monitoring presentation application.

## Core Contract

All time intervals are half-open. Timestamps lie in [-2^52, 2^52] milliseconds.
Labels are canonicalized without losing label presence. A namespace and label
set identify a series; timestamp identifies a sample within that series.
Every successful mutation has a monotonically increasing database sequence.
Repeated timestamps use the most recently committed value, including within
one request. A deletion hides samples committed at or before the deletion;
new measurements at those timestamps may be admitted again. Retention is
different: its monotonic floor prevents old measurements from returning.

Ingestion is atomic per request. Namespace policy bounds point count, total
series, age relative to the newest previously accepted timestamp and future
skew relative to an explicit caller-supplied observation time. Empty namespaces
have no late-data watermark. Limits are evaluated before publishing a mutation.
Read-only namespaces reject ingestion but permit reads and physical maintenance.

Raw queries merge mutable head points with immutable gzip blocks and then
resolve corrections, deletions and retention. A query runs in one bbolt read
transaction. Exact label, inequality and regular-expression matchers are
supported; absent labels behave as the empty string for matcher evaluation.
Limits reject excessive source work rather than silently dropping points.
Raw result limits are explicit and include a truncation indicator.

Window aggregation supports count, sum, min, max, average, first and last.
Buckets align to a caller-supplied origin, including before the Unix epoch.
Only populated windows appear. An average weights observations, not series.
Counter rate sums positive changes and treats a decrease as a reset to zero;
its denominator is elapsed time between the first and last observed point.
No extrapolation, interpolation or clock correction is implied.

Sealing moves all current head points into compressed per-series blocks in
one transaction. Compaction resolves overlapping blocks, corrections and
deletions into one block per selected series; empty series retain their identity.
Retention advances an irreversible floor and compacts the namespace physically.
Deleting measurements does not erase catalog identity or promise to shrink the
database file. bbolt reuses free pages; returning pages to the filesystem is
outside the current API.

Rollups store a query definition and its output at a database sequence.
Inspection reports whether the namespace has changed since materialization;
it never silently presents stale materialization as current. Backup returns
a consistent bbolt database image plus SHA-256 and sequence headers. Starting
the server with that image as RIDGE_DB restores that database directly.

## Workflows

1. Create, inspect and toggle namespace read-only policy.
2. Ingest ordered, late and corrective measurements atomically.
3. Discover canonical series through label matchers.
4. Read corrected points across head and sealed blocks.
5. Aggregate groups into signed, origin-aligned time windows.
6. Calculate rates across counter resets.
7. Seal mutable measurements into verified compressed blocks.
8. Delete a range without hiding later corrections.
9. Advance retention and reject resurrection.
10. Materialize a rollup with source-sequence provenance.
11. Inspect materialization freshness after later writes.
12. Compact physical blocks while preserving query results.
13. Export a consistent image and restart from it.
14. Explain candidate series, physical blocks and query budgets.
15. Migrate legacy format 1 blocks to the current format.

Each API operation crosses wire, engine and at least one domain/storage
responsibility. Ingestion, range execution and maintenance cross several.
The engine coordinates transaction ownership; volume owns durable publication;
chunk owns encoding; select owns matching; window owns numerical semantics;
policy owns admission and retention decisions; model owns identities and types.

## Block Formats

Sealed samples live in per-series blocks carrying a format number and a
SHA-256 checksum of the encoded payload. Format 1 is a gzip stream of an
ordered JSON sample array. Format 2 is a compact binary encoding: an 8-byte
header (`RB`, version byte 2, a reserved flags byte that must be zero, a
little-endian uint32 sample count) followed by one record per sample —
zigzag-varint timestamp for the first sample then uvarint deltas, 8
little-endian bytes of IEEE-754 value bits, and a uvarint sequence then
zigzag-varint deltas. Timestamps, float64 bit patterns and observation
sequences round-trip exactly in both formats.

One database may hold both formats at once; reads dispatch on the block
record's format. Seals, compactions and retentions write the configured
format (RIDGE_BLOCK_FORMAT, default 2; 1 pins legacy writes). Migration
rewrites every older block to the configured format in one storage
transaction after verifying the re-encoded payload decodes to identical
samples; an interrupted migration commits nothing and loses no confirmed
observation. Blocks already current are skipped, an empty migration consumes
no sequence, and migration is forward-only — it never downgrades. Unknown
formats, checksum mismatches, truncation and trailing bytes fail loudly with
the block ID. Format 1 stays readable indefinitely; builds without format 2
support cannot read format 2 blocks, so rollback across the transition
requires pinned legacy writes and no migrated blocks.

## Boundaries

One process opens one database file. Storage transactions are provided by bbolt;
the service does not implement distributed consensus, replication, historical
revision queries, SQL, authentication or a background retention scheduler.
Large raw scans are bounded by the namespace scan budget. Maintenance currently
runs synchronously. Rollups are explicitly refreshed, not incrementally updated.
Block payload formats are described under Block Formats.

Use `go build ./...` and `python3 examples/walkthrough.py --workflow all` to
exercise real API operations, process restart and failure responses. Block
codec round-trip and corruption tests live in `internal/chunk`
(`go test ./internal/chunk/`); broader formal automated tests are deferred.
The walkthrough is a bounded production demo
using temporary database files and synthetic infrastructure measurements.

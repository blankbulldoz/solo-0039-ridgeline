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

Collection invalidation is explicit. A submitted point is either a finite
value or a staleness marker (`stale: true`); a marker carries no value and
is never the number zero. A marker and a value at the same series and
timestamp are alternative versions of one sample: the most recently
committed version wins regardless of kind, and within one request the
later point wins. A late value at a marker's timestamp therefore corrects
the invalidation, and a late marker at a value's timestamp invalidates it
retroactively. A marker invalidates only its own timestamp as a stored
sample; as a query-time signal it closes the current counter segment, so
nothing is computed across it until collection resumes. Markers advance
the namespace watermark, are sealed into blocks, are hidden by deletions
and are removed by the retention floor exactly like values.

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
Raw result limits are explicit and include a truncation indicator. Markers
appear in raw results with a null value and a true stale flag, so readers
see collection stop and resume; scan statistics count them separately.

Window aggregation supports count, sum, min, max, average, first and last.
Buckets align to a caller-supplied origin, including before the Unix epoch.
Only populated windows appear. An average weights observations, not series.
Markers are not observations: they populate no window and contribute no
value to any function. Counter rate sums positive changes and treats a
decrease as a reset to zero; a marker closes the current segment, so no
change is summed across it and the invalidation gap contributes neither
increase nor elapsed time. Each segment's denominator is elapsed time
between its own first and last observed point; segment increases and
segment elapsed times are summed separately. No extrapolation,
interpolation or clock correction is implied.

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
15. Retire a series with an explicit marker, correct it with late writes
    and resume collection.

Each API operation crosses wire, engine and at least one domain/storage
responsibility. Ingestion, range execution and maintenance cross several.
The engine coordinates transaction ownership; volume owns durable publication;
chunk owns encoding; select owns matching; window owns numerical semantics;
policy owns admission and retention decisions; model owns identities and types.

## Boundaries

One process opens one database file. Storage transactions are provided by bbolt;
the service does not implement distributed consensus, replication, historical
revision queries, SQL, authentication or a background retention scheduler.
Large raw scans are bounded by the namespace scan budget. Maintenance currently
runs synchronously. Rollups are explicitly refreshed, not incrementally updated.
Block format 1 is JSON encoded under gzip with a checksum of the encoded payload.

Use `go build ./...` and `python3 examples/walkthrough.py --workflow all` to
exercise real API operations, process restart and failure responses. Formal
automated tests are deferred. The walkthrough is a bounded production demo
using temporary database files and synthetic infrastructure measurements.

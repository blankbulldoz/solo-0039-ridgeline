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
Namespaces move through a three-state lifecycle: writable, draining and
read_only (maintainable). Read-only namespaces reject logical mutations but
permit reads and physical maintenance.

Draining is the observable intermediate state between accepting writes and
being maintenance-ready. Starting a drain immediately stops admitting logical
mutations (points, deletions, retention, materialization) while already
admitted requests keep an in-flight slot and are allowed to commit. Queries and
physical maintenance (seal, compaction) remain available. Completion is a
separate durable commit that refuses while any admitted logical request is
still in flight, so the completion sequence is a hard boundary: no logical
mutation can commit after it. Cancelling a drain returns the namespace to
writable. A completed drain can be reopened explicitly. Every transition has
its own sequence; a restart preserves the durable state plus its drain
sequences, reports `restarted` with an empty in-flight count, and never
completes or cancels a drain on its own.

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
outside the current API. Seal and compaction are physical maintenance: they
preserve sample sequence and logical values, so they are allowed in every
lifecycle state, including draining and read-only. Materialization, deletion
and retention are logical changes: they are rejected once acceptance stops and
can never be committed silently after a drain is declared complete.

Rollups store a query definition and its output at a database sequence.
Inspection reports whether the namespace has changed since materialization;
it never silently presents stale materialization as current. Backup returns
a consistent bbolt database image plus SHA-256 and sequence headers. Starting
the server with that image as RIDGE_DB restores that database directly.

## Workflows

1. Create, inspect and toggle namespace read-only policy.
2. Drain a namespace: stop acceptance, observe in-flight work, complete or cancel.
3. Ingest ordered, late and corrective measurements atomically.
4. Discover canonical series through label matchers.
5. Read corrected points across head and sealed blocks.
6. Aggregate groups into signed, origin-aligned time windows.
7. Calculate rates across counter resets.
8. Seal mutable measurements into verified compressed blocks.
9. Delete a range without hiding later corrections.
10. Advance retention and reject resurrection.
11. Materialize a rollup with source-sequence provenance.
12. Inspect materialization freshness after later writes.
13. Compact physical blocks while preserving query results.
14. Export a consistent image and restart from it.
15. Explain candidate series, physical blocks and query budgets.

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
Drain in-flight counts are process-local; a restart reports them as zero and
marks the namespace as restarted rather than inferring completion. Draining does
not wait for queries: reads stay available and continue to see committed data.

Use `go build ./...` and `python3 examples/walkthrough.py --workflow all` to
exercise real API operations, process restart and failure responses. The
walkthrough includes the staged drain and restart inspection. Formal automated
tests are deferred in the product contract; the repository includes focused Go
tests for the lifecycle state machine (`go test ./...`). The walkthrough is a
bounded production demo using temporary database files and synthetic
infrastructure measurements.

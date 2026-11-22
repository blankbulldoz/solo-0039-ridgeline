// Package visibility provides an executable, independent model of which
// samples a Ridgeline range query is allowed to expose.
//
// The model is a testing oracle. It deliberately avoids every production
// package: it keeps committed samples by identity and timestamp, applies
// sequence-aware deletion tombstones and the monotonic retention floor, and
// treats sealing and compaction as physically invisible. A test replays the
// same mutation sequence through the engine's public API and through this
// model; any disagreement in acceptance, failure category or visible samples
// is a bug in one of them.
//
// Because the model never calls the production scan path, an expected value
// cannot be derived from the implementation under test. The replay recorder
// maps identities through the engine only so the two snapshots can be keyed
// the same way; point values and visibility decisions always come from the
// model.
//
// Two properties of the contract are modeled explicitly because they are easy
// to conflate:
//
//   - A deletion hides samples committed at or before it. A later commit at a
//     hidden timestamp is admitted and visible ("delete allows a new value").
//   - Retention raises a floor forever. Admission below the floor is refused
//     and previously stored samples below it stay invisible ("floor forbids
//     the old value from entering at all").
//
// Failing sequences can be saved, and delta debugging shrinks them to a small
// reproducer while replaying from a clean engine and model; see Divergence,
// MarshalSequence and Shrink.
package visibility

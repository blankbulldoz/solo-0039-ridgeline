"""Assert the handoff's evidence claims against a relational database.

Exit status is non-zero if any PROVABLE check fails. Checks that depend on
deleted run-attribution evidence are reported as UNPROVABLE instead of failing,
which is the whole point of the redaction exercise.
"""

import sqlite3
import sys
from pathlib import Path

PROVABLE = "PROVABLE"
UNPROVABLE = "UNPROVABLE"
FAILED = "FAILED"


def check(conn, sql, params=()):
    row = conn.execute(sql, params).fetchone()
    return row[0]


def main():
    db_path = Path(sys.argv[1])
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    results = []

    def record(claim, status, detail):
        results.append((claim, status, detail))

    # 1. Image integrity: every image has a stored hash and sequence.
    n = check(conn, "SELECT count(*) FROM image")
    record("four images registered", PROVABLE if n == 4 else FAILED, f"count={n}")

    # 2. The fork: identical source_sequence, different stored rollup values.
    rows = conn.execute(
        "SELECT * FROM same_sequence_different_values ORDER BY rollup_name"
    ).fetchall()
    expected = {"pulse-avg", "pulse-sum", "pulse-caliber"}
    ok = {r["rollup_name"] for r in rows} == expected
    pairs = {(r["image_a"], r["image_b"]) for r in rows}
    record("fork pair shares source_sequence with different rollup values",
           PROVABLE if ok and pairs == {("branch-a.db", "branch-b.db")} else FAILED,
           f"rows={len(rows)}")

    # 3. Equal sequence alone never authorizes merging provenance.
    rejected = check(
        conn,
        "SELECT count(*) FROM rollup_pair_verdict "
        "WHERE image_a='branch-a.db' AND image_b='branch-b.db' "
        "AND verdict='REJECT_COMMON_SOURCE'")
    record("same-source_sequence pair is rejected as common source",
           PROVABLE if rejected == 3 else FAILED, f"rejected_pairs={rejected}")

    # 4. Query caliber mismatch must force rejection even when the same
    #    rollup name and source_sequence are presented.
    q_diff = check(
        conn,
        "SELECT count(*) FROM rollup_pair_verdict "
        "WHERE rollup_name='pulse-caliber' AND image_a='branch-a.db' "
        "AND image_b='branch-b.db' AND query_differs=1 "
        "AND verdict='REJECT_COMMON_SOURCE'")
    record("query-caliber divergence rejects common-source merge",
           PROVABLE if q_diff == 1 else FAILED, f"matching_pairs={q_diff}")

    # 5. Physical-only control: logical equality plus physical difference.
    row = conn.execute(
        "SELECT * FROM image_pair_verdict "
        "WHERE image_a='base-seed.db' AND image_b='branch-c.db'").fetchone()
    record("control branch keeps identical observations",
           PROVABLE if row and row["observations_equal"] == 1 else FAILED,
           dict(row) if row else "missing pair")
    record("control branch changes only physical layout",
           PROVABLE if row and row["layout_differs"] == 1
           and row["verdict"] == "SAME_OBSERVATIONS_LAYOUT_ONLY" else FAILED,
           dict(row) if row else "missing pair")

    # 6. Layout must not be misread as an observation change for any pair
    #    whose logical fingerprints agree.
    bad = check(
        conn,
        "SELECT count(*) FROM image_pair_verdict "
        "WHERE observations_equal=1 AND verdict='OBSERVATIONS_DIVERGE'")
    record("no equal-observation pair is classified as divergent",
           PROVABLE if bad == 0 else FAILED, f"misclassifications={bad}")

    # 7. Block-level proof that layout really moved in the control branch.
    moved = check(
        conn,
        "SELECT count(*) FROM physical_fingerprint a "
        "JOIN physical_fingerprint b USING (physical_sha256) "
        "WHERE a.image_name='base-seed.db' AND b.image_name='branch-c.db'")
    record("seed and control physical fingerprints differ",
           PROVABLE if moved == 0 else FAILED,
           "shared_physical_hash_rows=%d" % moved)

    # 8. Rollup freshness at export time.
    stale = check(conn, "SELECT count(*) FROM rollup_fact WHERE stale_flag=1")
    record("every materialized rollup was fresh at export",
           PROVABLE if stale == 0 else FAILED, f"stale_rollups={stale}")

    # 9. Run ownership: only provable while run_attribution rows survive.
    owned = check(
        conn,
        "SELECT count(*) FROM run_attribution WHERE image_name='branch-a.db'")
    if owned == 1:
        team = check(
            conn,
            "SELECT analyst_team FROM run_attribution WHERE image_name='branch-a.db'")
        record("branch-a run ownership (team and parent image) is attributable",
               PROVABLE, f"analyst_team={team}")
    else:
        record("branch-a run ownership (team and parent image) is attributable",
               UNPROVABLE, "run_attribution row removed; team and branch name "
                           "cannot be established from remaining tables")

    parent = check(
        conn, "SELECT restored_from FROM image WHERE image_name='branch-a.db'")
    if parent:
        record("branch-a structural parent link is available",
               PROVABLE, f"restored_from={parent}")
    else:
        record("branch-a structural parent link is available",
               UNPROVABLE, "restored_from is NULL; common-backup ancestry "
                           "cannot be shown from this database")

    width = max(len(c) for c, _, _ in results)
    for claim, status, detail in results:
        print(f"[{status:9s}] {claim.ljust(width)}  :: {detail}")
    failed = [r for r in results if r[1] == FAILED]
    print(f"\n{len(results)} checks, {len(failed)} failed, "
          f"{sum(1 for r in results if r[1] == UNPROVABLE)} unprovable")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()

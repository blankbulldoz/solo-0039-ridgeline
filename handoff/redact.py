"""Produce the redacted delivery where one product loses run ownership.

The redacted copy keeps every byte-level, logical, physical and rollup fact
but deletes branch-a's run_attribution row. A second artifact additionally
nulls the structural parent pointer, demonstrating how much weaker the
provenance story becomes when ownership and lineage are both missing.
"""

import csv
import shutil
import sqlite3
from pathlib import Path

REDACTED_IMAGE = "branch-a.db"


def export_csv(conn, csv_dir):
    csv_dir.mkdir(parents=True, exist_ok=True)
    for old in csv_dir.glob("*.csv"):
        old.unlink()
    names = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table','view') "
        "AND name NOT LIKE 'sqlite_%' ORDER BY name")]
    for name in names:
        rows = conn.execute(f"SELECT * FROM {name}").fetchall()
        cols = [d[0] for d in conn.execute(
            f"SELECT * FROM {name} LIMIT 1").description]
        with (csv_dir / f"{name}.csv").open("w", newline="",
                                            encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(cols)
            writer.writerows(rows)


def build(src_db, out_db, also_clear_parent):
    if out_db.exists():
        out_db.unlink()
    shutil.copyfile(src_db, out_db)
    conn = sqlite3.connect(out_db)
    conn.execute("PRAGMA foreign_keys = ON")
    deleted = conn.execute(
        "DELETE FROM run_attribution WHERE image_name = ?",
        (REDACTED_IMAGE,)).rowcount
    if deleted != 1:
        raise RuntimeError("expected exactly one run_attribution row to delete")
    if also_clear_parent:
        conn.execute(
            "UPDATE image SET restored_from = NULL WHERE image_name = ?",
            (REDACTED_IMAGE,))
    conn.commit()
    remaining = conn.execute(
        "SELECT count(*) FROM run_attribution WHERE image_name = ?",
        (REDACTED_IMAGE,)).fetchone()[0]
    conn.close()
    return remaining


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dist", required=True)
    args = parser.parse_args()
    rel = Path(args.dist) / "relational"
    src = rel / "ridgeline_lineage.sqlite"

    out1 = rel / "ridgeline_lineage_redacted.sqlite"
    remaining = build(src, out1, also_clear_parent=False)
    conn = sqlite3.connect(out1)
    export_csv(conn, rel / "csv_redacted")
    conn.close()

    out2 = rel / "ridgeline_lineage_redacted_no_parent.sqlite"
    build(src, out2, also_clear_parent=True)

    print({"redacted_db": str(out1), "no_parent_db": str(out2),
           "remaining_attribution_rows": remaining})


if __name__ == "__main__":
    main()

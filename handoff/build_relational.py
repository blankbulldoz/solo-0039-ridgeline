"""Turn evidence.json into importable relational data.

Outputs under the delivery directory:
  relational/ridgeline_lineage.sqlite   normalized tables plus decision views
  relational/csv/*.csv                  one CSV per table for non-SQLite tools

The schema separates four evidence layers deliberately:
  image/run provenance  -> where bytes came from and which run produced them
  logical samples       -> observable (series, timestamp, value) facts
  physical blocks       -> storage-layout facts (block id, checksum, bytes)
  rollup facts          -> stored report values and their source_sequence
"""

import csv
import hashlib
import json
from pathlib import Path
import sqlite3

RUN_ID = "run-2026-11-22-ridgeline-fork"
BASE_IMAGE = "base-seed.db"
BRANCHES = ["branch-a", "branch-b", "branch-c"]


def connect(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def create_schema(conn):
    conn.executescript(
        """
        CREATE TABLE image (
            image_name      TEXT PRIMARY KEY,
            image_sha256    TEXT NOT NULL,
            byte_size       INTEGER NOT NULL,
            backup_sequence INTEGER NOT NULL,
            restored_from   TEXT REFERENCES image(image_name),
            fork_kind       TEXT NOT NULL CHECK (
                fork_kind IN ('shared_backup','observation_fork','physical_only'))
        );

        -- The run-attribution layer. Removing rows here is exactly the
        -- "take away the run ownership record" exercise: image bytes and
        -- every measured fact stay, lineage claims lose their evidence.
        CREATE TABLE run_attribution (
            run_id          TEXT NOT NULL,
            image_name      TEXT NOT NULL REFERENCES image(image_name),
            branch_name     TEXT NOT NULL,
            analyst_team    TEXT NOT NULL,
            produced_from   TEXT NOT NULL REFERENCES image(image_name),
            correction_time INTEGER,
            correction_value REAL,
            PRIMARY KEY (run_id, image_name)
        );

        CREATE TABLE series_fact (
            image_name      TEXT NOT NULL REFERENCES image(image_name),
            series_id       TEXT NOT NULL,
            labels_json     TEXT NOT NULL,
            PRIMARY KEY (image_name, series_id)
        );

        CREATE TABLE sample_fact (
            image_name      TEXT NOT NULL REFERENCES image(image_name),
            series_id       TEXT NOT NULL,
            sample_time     INTEGER NOT NULL,
            value           REAL NOT NULL,
            commit_sequence INTEGER NOT NULL,
            PRIMARY KEY (image_name, series_id, sample_time)
        );

        CREATE TABLE block_fact (
            image_name      TEXT NOT NULL REFERENCES image(image_name),
            block_id        TEXT NOT NULL,
            series_id       TEXT NOT NULL,
            block_start     INTEGER NOT NULL,
            block_end       INTEGER NOT NULL,
            point_count     INTEGER NOT NULL,
            payload_bytes   INTEGER NOT NULL,
            checksum_sha256 TEXT NOT NULL,
            format_version  INTEGER NOT NULL,
            PRIMARY KEY (image_name, block_id)
        );

        CREATE TABLE rollup_fact (
            image_name       TEXT NOT NULL REFERENCES image(image_name),
            rollup_name      TEXT NOT NULL,
            query_json       TEXT NOT NULL,
            source_sequence  INTEGER NOT NULL,
            stale_flag       INTEGER NOT NULL,
            PRIMARY KEY (image_name, rollup_name)
        );

        CREATE TABLE rollup_cell (
            image_name       TEXT NOT NULL,
            rollup_name      TEXT NOT NULL,
            group_labels_json TEXT NOT NULL,
            window_start     INTEGER NOT NULL,
            window_end       INTEGER NOT NULL,
            cell_value       REAL NOT NULL,
            point_count      INTEGER NOT NULL,
            PRIMARY KEY (image_name, rollup_name, group_labels_json, window_start),
            FOREIGN KEY (image_name, rollup_name)
                REFERENCES rollup_fact(image_name, rollup_name)
        );

        -- Fingerprints are computed by the builder over canonicalized rows:
        -- logical hashes ignore block layout, physical hashes ignore values
        -- that survive unchanged, and rollup hashes pin every stored cell.
        CREATE TABLE logical_fingerprint (
            image_name  TEXT PRIMARY KEY REFERENCES image(image_name),
            logical_sha256 TEXT NOT NULL,
            sample_rows INTEGER NOT NULL
        );

        CREATE TABLE physical_fingerprint (
            image_name  TEXT PRIMARY KEY REFERENCES image(image_name),
            physical_sha256 TEXT NOT NULL,
            block_rows  INTEGER NOT NULL
        );

        CREATE TABLE rollup_value_fingerprint (
            image_name  TEXT NOT NULL,
            rollup_name TEXT NOT NULL,
            values_sha256 TEXT NOT NULL,
            cell_rows   INTEGER NOT NULL,
            PRIMARY KEY (image_name, rollup_name)
        );
        """
    )


def populate(conn, evidence):
    images = {row["image"]: row for row in evidence["images"]}
    conn.execute(
        "INSERT INTO image VALUES (?,?,?,?,?,?)",
        (BASE_IMAGE, images[BASE_IMAGE]["sha256"], images[BASE_IMAGE]["bytes"],
         images[BASE_IMAGE]["sequence"], None, "shared_backup"))

    teams = {"branch-a": "analyst-team-alpha",
             "branch-b": "analyst-team-bravo",
             "branch-c": "analyst-team-control"}
    for branch in BRANCHES:
        meta = evidence["branches"][branch]
        img = meta["image"]
        conn.execute(
            "INSERT INTO image VALUES (?,?,?,?,?,?)",
            (img["image"], img["sha256"], img["bytes"], img["sequence"],
             BASE_IMAGE, meta["meta"]["kind"]))
        corr = meta["meta"]["correction"]
        conn.execute(
            "INSERT INTO run_attribution VALUES (?,?,?,?,?,?,?)",
            (RUN_ID, img["image"], branch, teams[branch], BASE_IMAGE,
             500 if corr is not None else None, corr))

        seen_series = set()
        for sp in meta["range"]["series"]:
            sid = sp["series"]["id"]
            if sid not in seen_series:
                conn.execute("INSERT INTO series_fact VALUES (?,?,?)",
                             (img["image"], sid,
                              json.dumps(sp["series"]["labels"],
                                         sort_keys=True, ensure_ascii=False)))
                seen_series.add(sid)
            for point in sp["points"]:
                conn.execute("INSERT INTO sample_fact VALUES (?,?,?,?,?)",
                             (img["image"], sid, point["time"], point["value"],
                              point["sequence"]))
        for block in meta["blocks"]:
            conn.execute("INSERT INTO block_fact VALUES (?,?,?,?,?,?,?,?,?)",
                         (img["image"], block["id"], block["series_id"],
                          block["start"], block["end"], block["count"],
                          block["bytes"], block["checksum"], block["format"]))
        for name, view in meta["rollups"].items():
            conn.execute("INSERT INTO rollup_fact VALUES (?,?,?,?,?)",
                         (img["image"], name,
                          json.dumps(view["query"], sort_keys=True,
                                     ensure_ascii=False),
                          view["source_sequence"], int(view["stale"])))
            for cell in view["result"]["cells"]:
                conn.execute("INSERT INTO rollup_cell VALUES (?,?,?,?,?,?,?)",
                             (img["image"], name,
                              json.dumps(cell["labels"], sort_keys=True,
                                         ensure_ascii=False),
                              cell["start"], cell["end"], cell["value"],
                              cell["count"]))

    # Seed logical/block/rollup facts: the seed has no rollups yet, only
    # samples and physical blocks.
    seed_img = images[BASE_IMAGE]["image"]
    for sp in evidence["seed"]["range"]["series"]:
        sid = sp["series"]["id"]
        conn.execute("INSERT OR IGNORE INTO series_fact VALUES (?,?,?)",
                     (seed_img, sid,
                      json.dumps(sp["series"]["labels"], sort_keys=True,
                                 ensure_ascii=False)))
        for point in sp["points"]:
            conn.execute("INSERT OR IGNORE INTO sample_fact VALUES (?,?,?,?,?)",
                         (seed_img, sid, point["time"], point["value"],
                          point["sequence"]))
    for block in evidence["seed"]["blocks"]:
        conn.execute("INSERT INTO block_fact VALUES (?,?,?,?,?,?,?,?,?)",
                     (seed_img, block["id"], block["series_id"], block["start"],
                      block["end"], block["count"], block["bytes"],
                      block["checksum"], block["format"]))


def compute_fingerprints(conn):
    rows = conn.execute(
        "SELECT image_name, series_id, sample_time, printf('%.17g', value) "
        "FROM sample_fact ORDER BY image_name, series_id, sample_time"
    ).fetchall()
    buckets = {}
    for image_name, sid, ts, val in rows:
        buckets.setdefault(image_name, []).append(f"{sid}:{ts}:{val}")
    for image_name, parts in buckets.items():
        digest = hashlib.sha256("|".join(parts).encode()).hexdigest()
        conn.execute("INSERT INTO logical_fingerprint VALUES (?,?,?)",
                     (image_name, digest, len(parts)))

    rows = conn.execute(
        "SELECT image_name, block_id, checksum_sha256, payload_bytes "
        "FROM block_fact ORDER BY image_name, block_id"
    ).fetchall()
    buckets = {}
    for image_name, bid, checksum, size in rows:
        buckets.setdefault(image_name, []).append(f"{bid}:{checksum}:{size}")
    for image_name, parts in buckets.items():
        digest = hashlib.sha256("|".join(parts).encode()).hexdigest()
        conn.execute("INSERT INTO physical_fingerprint VALUES (?,?,?)",
                     (image_name, digest, len(parts)))

    rows = conn.execute(
        "SELECT image_name, rollup_name, group_labels_json, window_start, "
        "printf('%.17g', cell_value), point_count FROM rollup_cell "
        "ORDER BY image_name, rollup_name, group_labels_json, window_start"
    ).fetchall()
    buckets = {}
    for image_name, rollup, labels, start, val, count in rows:
        buckets.setdefault((image_name, rollup), []).append(
            f"{labels}:{start}:{val}:{count}")
    for (image_name, rollup), parts in buckets.items():
        digest = hashlib.sha256("|".join(parts).encode()).hexdigest()
        conn.execute("INSERT INTO rollup_value_fingerprint VALUES (?,?,?,?)",
                     (image_name, rollup, digest, len(parts)))


def create_views(conn):
    #   * query text or stored values differ  -> reject common source
    #   * only physical fingerprints differ   -> same observations, layout only
    #   * identical source_sequence alone proves nothing across images
    conn.execute(
        """
        CREATE VIEW rollup_pair_verdict AS
        WITH pair AS (
            SELECT a.image_name AS image_a, b.image_name AS image_b,
                   a.rollup_name,
                   a.source_sequence AS seq_a, b.source_sequence AS seq_b,
                   a.query_json AS query_a, b.query_json AS query_b,
                   va.values_sha256 AS val_a, vb.values_sha256 AS val_b,
                   la.logical_sha256 AS log_a, lb.logical_sha256 AS log_b,
                   pa.physical_sha256 AS phys_a, pb.physical_sha256 AS phys_b
            FROM rollup_fact a
            JOIN rollup_fact b
              ON a.rollup_name = b.rollup_name AND a.image_name < b.image_name
            JOIN rollup_value_fingerprint va
              ON va.image_name = a.image_name AND va.rollup_name = a.rollup_name
            JOIN rollup_value_fingerprint vb
              ON vb.image_name = b.image_name AND vb.rollup_name = b.rollup_name
            JOIN logical_fingerprint la ON la.image_name = a.image_name
            JOIN logical_fingerprint lb ON lb.image_name = b.image_name
            JOIN physical_fingerprint pa ON pa.image_name = a.image_name
            JOIN physical_fingerprint pb ON pb.image_name = b.image_name
        )
        SELECT image_a, image_b, rollup_name, seq_a, seq_b,
               CASE WHEN query_a <> query_b THEN 1 ELSE 0 END AS query_differs,
               CASE WHEN val_a <> val_b THEN 1 ELSE 0 END AS values_differ,
               CASE WHEN log_a = log_b THEN 1 ELSE 0 END AS observations_equal,
               CASE WHEN phys_a <> phys_b THEN 1 ELSE 0 END AS layout_differs,
               CASE
                 WHEN query_a <> query_b OR val_a <> val_b
                   THEN 'REJECT_COMMON_SOURCE'
                 WHEN log_a = log_b AND phys_a <> phys_b
                   THEN 'SAME_OBSERVATIONS_LAYOUT_ONLY'
                 WHEN log_a = log_b AND phys_a = phys_b
                   THEN 'IDENTICAL_EVIDENCE'
                 ELSE 'INDETERMINATE'
               END AS verdict,
               CASE WHEN seq_a = seq_b THEN 1 ELSE 0 END AS same_source_sequence
        FROM pair
        """
    )
    # Image-level counterpart: compares whole images, including the seed that
    # has no rollups yet. This is where the physical-only control branch is
    # expected to read SAME_OBSERVATIONS_LAYOUT_ONLY.
    conn.execute(
        """
        CREATE VIEW image_pair_verdict AS
        SELECT a.image_name AS image_a, b.image_name AS image_b,
               CASE WHEN la.logical_sha256 = lb.logical_sha256 THEN 1
                    ELSE 0 END AS observations_equal,
               CASE WHEN pa.physical_sha256 <> pb.physical_sha256 THEN 1
                    ELSE 0 END AS layout_differs,
               CASE WHEN a.image_sha256 <> b.image_sha256 THEN 1
                    ELSE 0 END AS image_bytes_differ,
               CASE
                 WHEN la.logical_sha256 = lb.logical_sha256
                      AND pa.physical_sha256 <> pb.physical_sha256
                   THEN 'SAME_OBSERVATIONS_LAYOUT_ONLY'
                 WHEN la.logical_sha256 = lb.logical_sha256
                      AND pa.physical_sha256 = pb.physical_sha256
                   THEN 'IDENTICAL_EVIDENCE'
                 ELSE 'OBSERVATIONS_DIVERGE'
               END AS verdict
        FROM image a
        JOIN image b ON a.image_name < b.image_name
        JOIN logical_fingerprint la ON la.image_name = a.image_name
        JOIN logical_fingerprint lb ON lb.image_name = b.image_name
        JOIN physical_fingerprint pa ON pa.image_name = a.image_name
        JOIN physical_fingerprint pb ON pb.image_name = b.image_name
        """
    )

    # Convenience view for the headline finding of the fork.
    conn.execute(
        """
        CREATE VIEW same_sequence_different_values AS
        SELECT v.image_a, v.image_b, v.rollup_name, v.seq_a, v.seq_b,
               f1.values_sha256 AS val_a, f2.values_sha256 AS val_b
        FROM rollup_pair_verdict v
        JOIN rollup_value_fingerprint f1
          ON f1.image_name = v.image_a AND f1.rollup_name = v.rollup_name
        JOIN rollup_value_fingerprint f2
          ON f2.image_name = v.image_b AND f2.rollup_name = v.rollup_name
        WHERE v.same_source_sequence = 1 AND v.values_differ = 1
        """
    )


def export_csv(conn, csv_dir):
    csv_dir.mkdir(parents=True, exist_ok=True)
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table','view') "
        "AND name NOT LIKE 'sqlite_%' ORDER BY type, name")]
    for name in tables:
        rows = conn.execute(f"SELECT * FROM {name}").fetchall()
        cols = [d[0] for d in conn.execute(f"SELECT * FROM {name} LIMIT 1").description]
        with (csv_dir / f"{name}.csv").open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(cols)
            writer.writerows(rows)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--dist", required=True)
    args = parser.parse_args()
    evidence = json.loads(Path(args.evidence).read_text())
    rel_dir = Path(args.dist) / "relational"
    csv_dir = rel_dir / "csv"
    rel_dir.mkdir(parents=True, exist_ok=True)
    db_path = rel_dir / "ridgeline_lineage.sqlite"
    if db_path.exists():
        db_path.unlink()
    conn = connect(db_path)
    create_schema(conn)
    populate(conn, evidence)
    compute_fingerprints(conn)
    create_views(conn)
    conn.commit()
    export_csv(conn, csv_dir)
    conn.close()
    print(json.dumps({"sqlite": str(db_path),
                      "csv_tables": sorted(p.name for p in csv_dir.glob('*.csv'))},
                     indent=2))


if __name__ == "__main__":
    main()

"""Produce the synthetic fixture with DuckDB, independently of Armada's Go writer."""

import hashlib
import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent / "workload"
MINUTE = 60_000_000
ZERO = 1_735_689_600_000_000

conn = duckdb.connect()
conn.execute("""
    CREATE TABLE offered (
        logical_job_id VARCHAR, submitted_at_us BIGINT, queue VARCHAR,
        job_set VARCHAR, queue_priority BIGINT, priority_class VARCHAR,
        useful_work_us BIGINT, startup_us BIGINT, checkpoint_interval_us BIGINT,
        requirements_json VARCHAR, gang_id VARCHAR, gang_cardinality BIGINT,
        gang_node_uniformity_label VARCHAR, work_estimation_method VARCHAR
    )
""")

def req(**resources):
    return json.dumps({"resourceRequirements": {"requests": resources}},
                      sort_keys=True, separators=(",", ":"))

jobs = [
    ("cpu", -90, "low", 180, 45, None, req(cpu="1", memory="9007199254740993")),
    ("gpu", -90, "low", 180, 45, 60, req(**{"nvidia.com/gpu": "1"})),
    ("preemptor", 30, "high", 10, 0, None, req(cpu="1", **{"nvidia.com/gpu": "1"})),
]
partitions = []
for index, (job, arrival, priority, useful, startup, checkpoint, requirements) in enumerate(jobs):
    submitted = ZERO + arrival * MINUTE
    conn.execute("DELETE FROM offered")
    conn.execute("INSERT INTO offered VALUES (?, ?, 'offered', 'synthetic', 0, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL)",
                 [job, submitted, priority, useful * MINUTE, startup * MINUTE,
                  None if checkpoint is None else checkpoint * MINUTE, requirements])
    name = f"part-{index}.parquet"
    path = ROOT / name
    conn.execute("COPY offered TO ? (FORMAT PARQUET, COMPRESSION SNAPPY)", [str(path)])
    partitions.append({"path": name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                       "rows": 1, "min_submitted_at_us": submitted, "max_submitted_at_us": submitted})
(ROOT / "manifest.json").write_text(json.dumps({
    "schema_version": 1,
    "execution_model": "fixed-useful-work-v1",
    "dataset_id": "synthetic-mixed-v1",
    "converter_version": f"duckdb-{duckdb.__version__}",
    "work_estimation_method": "synthetic-known-work",
    "quality_counts": {"estimated": 0, "excluded": 0, "known": 3},
    "partitions": partitions,
}, indent=2) + "\n")

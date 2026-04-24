"""
MetadataStore — tracks every pipeline run in DuckDB.

Schema:
  pipeline_runs(
      run_id          INTEGER PRIMARY KEY,
      pipeline_name   TEXT,
      started_at      TEXT,
      finished_at     TEXT,
      status          TEXT,   -- running | success | failed
      rows_extracted  INTEGER,
      rows_loaded     INTEGER,
      elapsed_seconds REAL,
      error_message   TEXT,
  )
"""

from __future__ import annotations

import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          INTEGER PRIMARY KEY,
    pipeline_name   TEXT    NOT NULL,
    started_at      TEXT    NOT NULL,
    finished_at     TEXT,
    status          TEXT    DEFAULT 'running',
    rows_extracted  INTEGER,
    rows_loaded     INTEGER,
    elapsed_seconds REAL,
    error_message   TEXT
)
"""


class MetadataStore:
    def __init__(self, db_path: str = "metadata.duckdb"):
        import duckdb
        self.db_path = db_path
        self._conn = duckdb.connect(db_path)
        self._conn.execute(_CREATE_TABLE)

    def start_run(self, pipeline_name: str) -> int:
        started_at = datetime.utcnow().isoformat()
        self._conn.execute(
            "INSERT INTO pipeline_runs (pipeline_name, started_at) VALUES (?, ?)",
            [pipeline_name, started_at],
        )
        run_id = self._conn.execute(
            "SELECT MAX(run_id) FROM pipeline_runs WHERE pipeline_name = ?",
            [pipeline_name],
        ).fetchone()[0]
        return run_id

    def finish_run(self, run_id: int, stats: dict) -> None:
        self._conn.execute(
            """
            UPDATE pipeline_runs SET
                finished_at     = ?,
                status          = ?,
                rows_extracted  = ?,
                rows_loaded     = ?,
                elapsed_seconds = ?,
                error_message   = ?
            WHERE run_id = ?
            """,
            [
                datetime.utcnow().isoformat(),
                stats.get("status"),
                stats.get("rows_extracted"),
                stats.get("rows_loaded"),
                stats.get("elapsed_seconds"),
                stats.get("error"),
                run_id,
            ],
        )

    def get_history(self, pipeline_name: str | None = None, limit: int = 20) -> list[dict]:
        if pipeline_name:
            rows = self._conn.execute(
                "SELECT * FROM pipeline_runs WHERE pipeline_name = ? ORDER BY run_id DESC LIMIT ?",
                [pipeline_name, limit],
            ).fetchdf()
        else:
            rows = self._conn.execute(
                "SELECT * FROM pipeline_runs ORDER BY run_id DESC LIMIT ?",
                [limit],
            ).fetchdf()
        return rows.to_dict(orient="records")

    def print_history(self, pipeline_name: str | None = None) -> None:
        rows = self.get_history(pipeline_name)
        if not rows:
            print("No runs found.")
            return
        header = f"{'Run':<6} {'Pipeline':<30} {'Status':<10} {'Rows':<8} {'Time(s)':<9} {'Started'}"
        print(header)
        print("-" * len(header))
        for r in rows:
            print(
                f"{r['run_id']:<6} {str(r['pipeline_name']):<30} "
                f"{str(r['status']):<10} {str(r.get('rows_loaded', '-')):<8} "
                f"{str(r.get('elapsed_seconds', '-')):<9} {r['started_at']}"
            )

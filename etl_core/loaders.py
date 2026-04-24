"""
Loaders — pluggable data destination adapters.

Supported destinations:
  - duckdb    : DuckDB table (default, recommended for Data Lake)
  - sqlite    : SQLite table
  - csv       : local CSV file
  - json_file : local JSON file
  - postgres  : PostgreSQL table (requires psycopg2)

Adding a new loader:
  1. Subclass BaseLoader
  2. Implement load(data) → None
  3. Register it in LoaderFactory._registry
"""

from __future__ import annotations

import csv
import json
import logging
import sqlite3
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
#  Base                                                                 #
# ------------------------------------------------------------------ #

class BaseLoader(ABC):
    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def load(self, data: list[dict[str, Any]]) -> None:
        ...

    def _resolve_write_mode(self) -> str:
        """Return 'append' or 'replace' based on config."""
        return self.config.get("write_mode", "append")


# ------------------------------------------------------------------ #
#  Implementations                                                      #
# ------------------------------------------------------------------ #

class DuckDBLoader(BaseLoader):
    """
    Load data into a DuckDB table.
    Automatically creates the table if it doesn't exist.

    Config:
      database   : path to .duckdb file (default: data_lake.duckdb)
      table      : destination table name (required)
      schema     : optional schema/namespace
      write_mode : append | replace (default: append)
    """

    def load(self, data: list[dict]) -> None:
        import duckdb
        import pandas as pd

        if not data:
            logger.warning("DuckDBLoader: no data to load.")
            return

        db_path = self.config.get("database", "data_lake.duckdb")
        table = self.config["table"]
        schema = self.config.get("schema", "main")
        write_mode = self._resolve_write_mode()

        df = pd.DataFrame(data)
        full_table = f"{schema}.{table}"

        conn = duckdb.connect(db_path)
        try:
            if write_mode == "replace":
                conn.execute(f"DROP TABLE IF EXISTS {full_table}")
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {full_table} AS SELECT * FROM df LIMIT 0"
            )
            conn.execute(f"INSERT INTO {full_table} SELECT * FROM df")
            count = conn.execute(f"SELECT COUNT(*) FROM {full_table}").fetchone()[0]
            logger.info(f"DuckDB: {len(data)} rows → {full_table} (total: {count})")
        finally:
            conn.close()


class SQLiteLoader(BaseLoader):
    """Load data into a SQLite table."""

    def load(self, data: list[dict]) -> None:
        if not data:
            return

        db_path = self.config["database"]
        table = self.config["table"]
        write_mode = self._resolve_write_mode()

        columns = list(data[0].keys())
        placeholders = ", ".join(["?"] * len(columns))
        col_defs = ", ".join([f'"{c}" TEXT' for c in columns])
        rows = [tuple(row.get(c) for c in columns) for row in data]

        conn = sqlite3.connect(db_path)
        try:
            if write_mode == "replace":
                conn.execute(f"DROP TABLE IF EXISTS {table}")
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table} ({col_defs})")
            conn.executemany(
                f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
                rows,
            )
            conn.commit()
            logger.info(f"SQLite: {len(data)} rows → {table}")
        finally:
            conn.close()


class CSVLoader(BaseLoader):
    """Write data to a CSV file."""

    def load(self, data: list[dict]) -> None:
        if not data:
            return

        path = Path(self.config["path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        write_mode = self._resolve_write_mode()

        file_mode = "w" if write_mode == "replace" else "a"
        write_header = write_mode == "replace" or not path.exists()

        with open(path, file_mode, newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(data[0].keys()))
            if write_header:
                writer.writeheader()
            writer.writerows(data)

        logger.info(f"CSV: {len(data)} rows → {path}")


class JSONFileLoader(BaseLoader):
    """Write data to a JSON file."""

    def load(self, data: list[dict]) -> None:
        path = Path(self.config["path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        write_mode = self._resolve_write_mode()

        if write_mode == "append" and path.exists():
            with open(path, encoding="utf-8") as f:
                existing = json.load(f)
            data = existing + data

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)

        logger.info(f"JSON: {len(data)} rows → {path}")


class PostgresLoader(BaseLoader):
    """Load data into a PostgreSQL table using COPY (fast bulk insert)."""

    def load(self, data: list[dict]) -> None:
        if not data:
            return
        try:
            import psycopg2
            import psycopg2.extras
        except ImportError:
            raise ImportError("Install psycopg2: pip install psycopg2-binary")

        conn_str = self.config["connection_string"]
        table = self.config["table"]
        write_mode = self._resolve_write_mode()
        columns = list(data[0].keys())

        conn = psycopg2.connect(conn_str)
        try:
            with conn.cursor() as cur:
                if write_mode == "replace":
                    cur.execute(f"TRUNCATE TABLE {table}")
                psycopg2.extras.execute_batch(
                    cur,
                    f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(['%s']*len(columns))})",
                    [tuple(row.get(c) for c in columns) for row in data],
                )
            conn.commit()
            logger.info(f"Postgres: {len(data)} rows → {table}")
        finally:
            conn.close()


# ------------------------------------------------------------------ #
#  Factory                                                              #
# ------------------------------------------------------------------ #

class LoaderFactory:
    _registry: dict[str, type[BaseLoader]] = {
        "duckdb": DuckDBLoader,
        "sqlite": SQLiteLoader,
        "csv": CSVLoader,
        "json_file": JSONFileLoader,
        "postgres": PostgresLoader,
    }

    @classmethod
    def create(cls, dest_config: dict) -> BaseLoader:
        dest_type = dest_config.get("type")
        if dest_type not in cls._registry:
            raise ValueError(
                f"Unknown destination type '{dest_type}'. "
                f"Available: {list(cls._registry.keys())}"
            )
        return cls._registry[dest_type](dest_config)

    @classmethod
    def register(cls, name: str, loader_cls: type[BaseLoader]) -> None:
        """Register a custom loader at runtime."""
        cls._registry[name] = loader_cls

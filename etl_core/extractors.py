"""
Extractors — pluggable data source adapters.

Supported sources:
  - csv       : local CSV file
  - json_file : local JSON file
  - rest_api  : HTTP REST endpoint (GET/POST)
  - sqlite    : SQLite database
  - duckdb    : DuckDB database
  - postgres  : PostgreSQL (requires psycopg2)

Adding a new source:
  1. Subclass BaseExtractor
  2. Implement extract() → list[dict]
  3. Register it in ExtractorFactory._registry
"""

from __future__ import annotations

import csv
import json
import logging
import sqlite3
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
#  Base                                                                 #
# ------------------------------------------------------------------ #

class BaseExtractor(ABC):
    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def extract(self) -> list[dict[str, Any]]:
        ...


# ------------------------------------------------------------------ #
#  Implementations                                                      #
# ------------------------------------------------------------------ #

class CSVExtractor(BaseExtractor):
    """Read a local CSV file."""

    def extract(self) -> list[dict]:
        path = Path(self.config["path"])
        delimiter = self.config.get("delimiter", ",")
        encoding = self.config.get("encoding", "utf-8")
        with open(path, newline="", encoding=encoding) as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            return [row for row in reader]


class JSONFileExtractor(BaseExtractor):
    """Read a local JSON file (list of objects)."""

    def extract(self) -> list[dict]:
        path = Path(self.config["path"])
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        # Support {"data": [...]} wrapper
        root_key = self.config.get("root_key", "data")
        return data[root_key]


class RESTAPIExtractor(BaseExtractor):
    """
    Fetch data from a REST API.

    Config keys:
      url         : endpoint URL (required)
      method      : GET | POST (default GET)
      headers     : dict of extra headers
      params      : query-string params (GET) or JSON body (POST)
      root_key    : dotted path into the response to reach the list
      pagination  : { type: "offset", page_param: "page", limit_param: "limit",
                       limit: 100, max_pages: 10 }
    """

    def extract(self) -> list[dict]:
        url = self.config["url"]
        method = self.config.get("method", "GET").upper()
        headers = self.config.get("headers", {})
        params = self.config.get("params", {})
        root_key = self.config.get("root_key")
        pagination = self.config.get("pagination")

        all_rows: list[dict] = []

        with httpx.Client(timeout=30) as client:
            if not pagination:
                response = self._request(client, method, url, headers, params)
                all_rows = self._extract_rows(response, root_key)
            else:
                all_rows = self._paginate(client, method, url, headers, params, root_key, pagination)

        logger.info(f"REST API extracted {len(all_rows)} records from {url}")
        return all_rows

    def _request(self, client, method, url, headers, params) -> Any:
        if method == "GET":
            r = client.get(url, headers=headers, params=params)
        else:
            r = client.post(url, headers=headers, json=params)
        r.raise_for_status()
        return r.json()

    def _extract_rows(self, data: Any, root_key: str | None) -> list[dict]:
        if root_key:
            for key in root_key.split("."):
                data = data[key]
        if isinstance(data, list):
            return data
        return [data]

    def _paginate(self, client, method, url, headers, params, root_key, pag_cfg) -> list[dict]:
        all_rows: list[dict] = []
        page = pag_cfg.get("start_page", 1)
        max_pages = pag_cfg.get("max_pages", 100)
        page_param = pag_cfg.get("page_param", "page")
        limit = pag_cfg.get("limit", 100)
        limit_param = pag_cfg.get("limit_param", "limit")

        for _ in range(max_pages):
            p = {**params, page_param: page, limit_param: limit}
            response = self._request(client, method, url, headers, p)
            rows = self._extract_rows(response, root_key)
            if not rows:
                break
            all_rows.extend(rows)
            if len(rows) < limit:
                break
            page += 1

        return all_rows


class SQLiteExtractor(BaseExtractor):
    """Run a SQL query against a SQLite database."""

    def extract(self) -> list[dict]:
        db_path = self.config["database"]
        query = self.config["query"]
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.execute(query)
            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()


class DuckDBExtractor(BaseExtractor):
    """Run a SQL query against a DuckDB database."""

    def extract(self) -> list[dict]:
        import duckdb
        db_path = self.config.get("database", ":memory:")
        query = self.config["query"]
        conn = duckdb.connect(db_path)
        try:
            result = conn.execute(query).fetchdf()
            return result.to_dict(orient="records")
        finally:
            conn.close()


class PostgresExtractor(BaseExtractor):
    """Run a SQL query against PostgreSQL."""

    def extract(self) -> list[dict]:
        try:
            import psycopg2
            import psycopg2.extras
        except ImportError:
            raise ImportError("Install psycopg2: pip install psycopg2-binary")

        conn_str = self.config["connection_string"]
        query = self.config["query"]
        conn = psycopg2.connect(conn_str)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query)
                return [dict(row) for row in cur.fetchall()]
        finally:
            conn.close()


# ------------------------------------------------------------------ #
#  Factory                                                              #
# ------------------------------------------------------------------ #

class ExtractorFactory:
    _registry: dict[str, type[BaseExtractor]] = {
        "csv": CSVExtractor,
        "json_file": JSONFileExtractor,
        "rest_api": RESTAPIExtractor,
        "sqlite": SQLiteExtractor,
        "duckdb": DuckDBExtractor,
        "postgres": PostgresExtractor,
    }

    @classmethod
    def create(cls, source_config: dict) -> BaseExtractor:
        source_type = source_config.get("type")
        if source_type not in cls._registry:
            raise ValueError(
                f"Unknown source type '{source_type}'. "
                f"Available: {list(cls._registry.keys())}"
            )
        return cls._registry[source_type](source_config)

    @classmethod
    def register(cls, name: str, extractor_cls: type[BaseExtractor]) -> None:
        """Register a custom extractor at runtime."""
        cls._registry[name] = extractor_cls

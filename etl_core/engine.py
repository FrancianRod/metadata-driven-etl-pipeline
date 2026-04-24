"""
ETL Engine - Core orchestration module.
Reads a pipeline config and executes extract → transform → load automatically.
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from .extractors import ExtractorFactory
from .transformers import TransformerPipeline
from .loaders import LoaderFactory
from .metadata import MetadataStore

logger = logging.getLogger(__name__)


class ETLEngine:
    """
    Zero-code ETL orchestrator.

    Usage:
        engine = ETLEngine("configs/my_pipeline.yaml")
        engine.run()
    """

    def __init__(self, config_path: str, metadata_db: str = "metadata.duckdb"):
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.metadata = MetadataStore(metadata_db)

    # ------------------------------------------------------------------ #
    #  Config loading                                                       #
    # ------------------------------------------------------------------ #

    def _load_config(self) -> dict:
        suffix = self.config_path.suffix.lower()
        with open(self.config_path, "r") as f:
            if suffix in (".yaml", ".yml"):
                config = yaml.safe_load(f)
            elif suffix == ".json":
                config = json.load(f)
            else:
                raise ValueError(f"Unsupported config format: {suffix}")
        self._validate_config(config)
        return config

    def _validate_config(self, config: dict) -> None:
        required_keys = {"pipeline_name", "source", "destination"}
        missing = required_keys - set(config.keys())
        if missing:
            raise ValueError(f"Config missing required keys: {missing}")

    # ------------------------------------------------------------------ #
    #  Pipeline execution                                                   #
    # ------------------------------------------------------------------ #

    def run(self) -> dict[str, Any]:
        """Execute the full ETL pipeline and return a run summary."""
        pipeline_name = self.config["pipeline_name"]
        run_id = self.metadata.start_run(pipeline_name)
        start = time.time()

        logger.info(f"[{pipeline_name}] Starting run {run_id}")

        stats: dict[str, Any] = {
            "run_id": run_id,
            "pipeline": pipeline_name,
            "started_at": datetime.utcnow().isoformat(),
            "status": "running",
        }

        try:
            # 1 — EXTRACT
            logger.info(f"[{pipeline_name}] Extracting from source...")
            extractor = ExtractorFactory.create(self.config["source"])
            raw_data = extractor.extract()
            stats["rows_extracted"] = len(raw_data)
            logger.info(f"[{pipeline_name}] Extracted {len(raw_data)} rows.")

            # 2 — TRANSFORM
            transformations = self.config.get("transformations", [])
            if transformations:
                logger.info(f"[{pipeline_name}] Applying {len(transformations)} transformation(s)...")
                pipeline = TransformerPipeline(transformations)
                transformed_data = pipeline.apply(raw_data)
            else:
                transformed_data = raw_data
            stats["rows_after_transform"] = len(transformed_data)

            # 3 — LOAD
            logger.info(f"[{pipeline_name}] Loading into destination...")
            loader = LoaderFactory.create(self.config["destination"])
            loader.load(transformed_data)
            stats["rows_loaded"] = len(transformed_data)

            elapsed = round(time.time() - start, 2)
            stats.update({"status": "success", "elapsed_seconds": elapsed})
            self.metadata.finish_run(run_id, stats)
            logger.info(f"[{pipeline_name}] ✓ Done in {elapsed}s — {len(transformed_data)} rows loaded.")

        except Exception as exc:
            elapsed = round(time.time() - start, 2)
            stats.update({"status": "failed", "error": str(exc), "elapsed_seconds": elapsed})
            self.metadata.finish_run(run_id, stats)
            logger.error(f"[{pipeline_name}] ✗ Failed after {elapsed}s: {exc}")
            raise

        return stats

#!/usr/bin/env python3
"""
etl-run — command-line interface for the Zero-Code ETL Framework.

Usage:
    python run_pipeline.py configs/my_pipeline.yaml
    python run_pipeline.py configs/my_pipeline.yaml --metadata metadata.duckdb
    python run_pipeline.py --history
    python run_pipeline.py --history --pipeline sales_pipeline
"""

import argparse
import logging
import sys

from etl_core import ETLEngine, MetadataStore


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Zero-Code ETL Framework — run pipelines from config files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py configs/users_api.yaml
  python run_pipeline.py configs/sales_db.json --log-level DEBUG
  python run_pipeline.py --history
  python run_pipeline.py --history --pipeline sales_pipeline
        """,
    )
    parser.add_argument(
        "config",
        nargs="?",
        help="Path to pipeline config file (.yaml or .json)",
    )
    parser.add_argument(
        "--metadata",
        default="metadata.duckdb",
        help="Path to metadata DuckDB file (default: metadata.duckdb)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO)",
    )
    parser.add_argument(
        "--history",
        action="store_true",
        help="Print pipeline run history and exit",
    )
    parser.add_argument(
        "--pipeline",
        help="Filter history by pipeline name (used with --history)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    if args.history:
        store = MetadataStore(args.metadata)
        store.print_history(args.pipeline)
        return

    if not args.config:
        print("Error: provide a config file path or use --history.")
        print("Run with --help for usage.")
        sys.exit(1)

    try:
        engine = ETLEngine(args.config, metadata_db=args.metadata)
        stats = engine.run()
        print(f"\n✓ Pipeline '{stats['pipeline']}' completed successfully.")
        print(f"  Rows extracted : {stats.get('rows_extracted', 'N/A')}")
        print(f"  Rows loaded    : {stats.get('rows_loaded', 'N/A')}")
        print(f"  Elapsed        : {stats.get('elapsed_seconds', 'N/A')}s")
    except FileNotFoundError as e:
        print(f"Config file not found: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

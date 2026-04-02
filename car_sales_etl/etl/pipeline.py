# etl/pipeline.py
# ─────────────────────────────────────────────────────────────
# ETL PIPELINE ORCHESTRATOR
#
# Entry point for the full Extract → Transform → Load pipeline.
# Run this file directly:
#
#   python etl/pipeline.py
#
# Optional flags:
#   --extract-only     Run only the Extract step
#   --transform-only   Run Extract + Transform, skip Load
#   --skip-load        Same as --transform-only
# ─────────────────────────────────────────────────────────────

import argparse
import logging
import sys
import time
from pathlib import Path
from datetime import datetime

# Ensure project root is on the path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from etl.extract   import extract
from etl.transform import transform
from etl.load      import load
from config        import LOG_LEVEL

# ── Logging setup ──────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            Path(__file__).parent.parent / "pipeline.log",
            mode="a",
            encoding="utf-8",
        ),
    ],
)

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Car Sales ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python etl/pipeline.py                  # Full pipeline (E + T + L)
  python etl/pipeline.py --extract-only   # Stop after extraction
  python etl/pipeline.py --skip-load      # Transform only, no DB write
        """,
    )
    parser.add_argument(
        "--extract-only", action="store_true",
        help="Run only the Extract step (no Transform or Load)"
    )
    parser.add_argument(
        "--skip-load", "--transform-only", action="store_true",
        help="Run Extract + Transform but skip the Load step"
    )
    return parser.parse_args()


def run_pipeline(extract_only: bool = False, skip_load: bool = False) -> dict:
    """
    Execute the full ETL pipeline (or a partial run based on flags).

    Returns
    -------
    dict : Summary metrics from each stage that was run.
    """
    pipeline_start = time.time()
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    logger.info("╔══════════════════════════════════════════════════════╗")
    logger.info(f"  Car Sales ETL Pipeline  |  Run ID: {run_id}")
    logger.info("╚══════════════════════════════════════════════════════╝")

    summary = {"run_id": run_id, "status": "success"}

    # ── EXTRACT ────────────────────────────────────────────────
    t0 = time.time()
    try:
        raw_df = extract()
        summary["extract_rows"]    = len(raw_df)
        summary["extract_time_s"]  = round(time.time() - t0, 2)
    except Exception as e:
        logger.error(f"EXTRACT failed: {e}")
        summary["status"] = "failed_at_extract"
        raise

    if extract_only:
        logger.info("--extract-only flag set. Stopping after Extract.")
        _log_summary(summary, time.time() - pipeline_start)
        return summary

    # ── TRANSFORM ──────────────────────────────────────────────
    t0 = time.time()
    try:
        clean_df = transform(raw_df)
        summary["transform_rows"]   = len(clean_df)
        summary["dropped_rows"]     = summary["extract_rows"] - len(clean_df)
        summary["transform_time_s"] = round(time.time() - t0, 2)
    except Exception as e:
        logger.error(f"TRANSFORM failed: {e}")
        summary["status"] = "failed_at_transform"
        raise

    if skip_load:
        logger.info("--skip-load flag set. Stopping after Transform.")
        _log_summary(summary, time.time() - pipeline_start)
        return summary

    # ── LOAD ───────────────────────────────────────────────────
    t0 = time.time()
    try:
        load(clean_df)
        summary["load_rows"]   = len(clean_df)
        summary["load_time_s"] = round(time.time() - t0, 2)
    except ConnectionError as e:
        logger.error(f"LOAD failed — DB connection issue: {e}")
        summary["status"] = "failed_at_load"
        raise
    except Exception as e:
        logger.error(f"LOAD failed: {e}")
        summary["status"] = "failed_at_load"
        raise

    # ── Done ───────────────────────────────────────────────────
    summary["total_time_s"] = round(time.time() - pipeline_start, 2)
    _log_summary(summary, summary["total_time_s"])
    return summary


def _log_summary(summary: dict, total_elapsed: float) -> None:
    logger.info("╔══════════════════════════════════════════════════════╗")
    logger.info("  PIPELINE SUMMARY")
    logger.info(f"  Run ID:           {summary.get('run_id')}")
    logger.info(f"  Status:           {summary.get('status', 'unknown').upper()}")
    if "extract_rows" in summary:
        logger.info(f"  Extracted rows:   {summary['extract_rows']:,}  ({summary.get('extract_time_s', '?')}s)")
    if "transform_rows" in summary:
        logger.info(f"  Transformed rows: {summary['transform_rows']:,}  ({summary.get('transform_time_s', '?')}s)")
        logger.info(f"  Rows dropped:     {summary.get('dropped_rows', '?'):,}")
    if "load_rows" in summary:
        logger.info(f"  Loaded rows:      {summary['load_rows']:,}  ({summary.get('load_time_s', '?')}s)")
    logger.info(f"  Total time:       {total_elapsed:.1f}s")
    logger.info("╚══════════════════════════════════════════════════════╝")


if __name__ == "__main__":
    args = parse_args()
    try:
        run_pipeline(
            extract_only=args.extract_only,
            skip_load=args.skip_load,
        )
    except Exception:
        sys.exit(1)

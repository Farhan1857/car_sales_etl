# etl/extract.py
# ─────────────────────────────────────────────────────────────
# EXTRACT — Step 1 of the ETL Pipeline
#
# Responsibilities:
#   • Load the raw CSV from disk
#   • Validate that all expected columns are present
#   • Report basic source statistics
#   • Return a raw DataFrame for the Transform step
# ─────────────────────────────────────────────────────────────

import logging
import pandas as pd
from pathlib import Path

import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config import RAW_CSV, EXPECTED_COLUMNS

logger = logging.getLogger(__name__)


def extract() -> pd.DataFrame:
    """
    Load and perform a light schema validation on the raw CSV.

    Returns
    -------
    pd.DataFrame
        Raw, unmodified dataframe from the source CSV.

    Raises
    ------
    FileNotFoundError
        If the source CSV does not exist at the configured path.
    ValueError
        If required columns are missing from the source file.
    """
    logger.info("=" * 60)
    logger.info("EXTRACT — starting")
    logger.info(f"Source: {RAW_CSV}")

    # ── 1. File existence check ────────────────────────────────
    if not RAW_CSV.exists():
        raise FileNotFoundError(
            f"Source file not found: {RAW_CSV}\n"
            "Place the raw CSV at data/raw/car_sales.csv before running."
        )

    # ── 2. Load CSV ────────────────────────────────────────────
    logger.info("Loading CSV into memory...")
    df = pd.read_csv(RAW_CSV, low_memory=False)
    logger.info(f"Loaded {len(df):,} rows × {len(df.columns)} columns")

    # ── 3. Normalize column names ──────────────────────────────
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # ── 4. Schema validation ───────────────────────────────────
    missing_cols = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing_cols:
        raise ValueError(
            f"Schema mismatch — missing columns: {missing_cols}\n"
            f"Found: {list(df.columns)}"
        )
    logger.info("Schema validation passed — all expected columns present.")

    # ── 5. Source statistics ───────────────────────────────────
    logger.info(f"  Date range:       {df['saledate'].iloc[0]}  →  {df['saledate'].iloc[-1]}")
    logger.info(f"  Unique makes:     {df['make'].nunique()}")
    logger.info(f"  Unique sellers:   {df['seller'].nunique()}")
    logger.info(f"  States covered:   {df['state'].nunique()}")
    logger.info(f"  Null sellingprice:{df['sellingprice'].isna().sum():,}")

    logger.info("EXTRACT — complete")
    return df


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="%(levelname)s | %(message)s")
    df = extract()
    print(df.head())
    print(df.dtypes)

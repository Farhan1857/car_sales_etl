# etl/transform.py
# ─────────────────────────────────────────────────────────────
# TRANSFORM — Step 2 of the ETL Pipeline
#
# Responsibilities:
#   • Handle nulls and fix data types
#   • Parse and normalize the saledate timestamp
#   • Clean string fields (strip, title-case, etc.)
#   • Filter out invalid / outlier records
#   • Engineer derived columns for business analysis
#   • Return a clean DataFrame ready for DB insertion
# ─────────────────────────────────────────────────────────────

import logging
import pandas as pd
import numpy as np
from pathlib import Path

import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config import (
    CLEANED_CSV,
    MIN_SELLING_PRICE, MAX_SELLING_PRICE,
    MIN_ODOMETER, MAX_ODOMETER,
    VIN_LENGTH,
)

logger = logging.getLogger(__name__)


# ── Helper utilities ───────────────────────────────────────────

def _drop_report(label: str, before: int, after: int) -> None:
    dropped = before - after
    pct = (dropped / before) * 100 if before else 0
    logger.info(f"  [{label}] dropped {dropped:,} rows ({pct:.2f}%)")


# ── Main transform function ────────────────────────────────────

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and enrich the raw car sales DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Raw dataframe from the Extract step.

    Returns
    -------
    pd.DataFrame
        Cleaned, typed, and feature-enriched dataframe.
    """
    logger.info("=" * 60)
    logger.info("TRANSFORM — starting")
    initial_count = len(df)
    logger.info(f"Input rows: {initial_count:,}")

    # ── Step 1: Drop records missing critical fields ───────────
    logger.info("Step 1 — Dropping records with null critical fields...")
    critical_cols = ["vin", "make", "model", "sellingprice", "mmr", "saledate"]
    df = df.dropna(subset=critical_cols)
    _drop_report("null critical fields", initial_count, len(df))

    # ── Step 2: VIN validation ─────────────────────────────────
    logger.info("Step 2 — Validating VINs...")
    before = len(df)
    df["vin"] = df["vin"].astype(str).str.strip().str.upper()
    df = df[df["vin"].str.len() == VIN_LENGTH]
    _drop_report("invalid VIN length", before, len(df))

    # ── Step 3: Type casting ───────────────────────────────────
    logger.info("Step 3 — Casting column types...")
    df["year"]         = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["condition"]    = pd.to_numeric(df["condition"], errors="coerce")
    df["odometer"]     = pd.to_numeric(df["odometer"], errors="coerce").astype("Int64")
    df["mmr"]          = pd.to_numeric(df["mmr"], errors="coerce")
    df["sellingprice"] = pd.to_numeric(df["sellingprice"], errors="coerce")

    # ── Step 4: Parse saledate ─────────────────────────────────
    logger.info("Step 4 — Parsing saledate timestamps...")
    # Format example: "Tue Dec 16 2014 12:30:00 GMT-0800 (PST)"
    df["saledate"] = df["saledate"].str.replace(r"\s*\(.*?\)", "", regex=True).str.strip()
    df["saledate"] = pd.to_datetime(df["saledate"], format="%a %b %d %Y %H:%M:%S GMT%z",
                                    errors="coerce", utc=True)
    df["saledate"] = df["saledate"].dt.tz_convert(None)  # store as naive UTC

    before = len(df)
    df = df.dropna(subset=["saledate", "year", "odometer", "sellingprice", "mmr"])
    _drop_report("unparseable dates/numerics", before, len(df))

    # ── Step 5: String normalization ───────────────────────────
    logger.info("Step 5 — Normalizing string fields...")
    str_cols = ["make", "model", "trim", "body", "transmission",
                "state", "color", "interior", "seller"]
    for col in str_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.title()
                .replace("Nan", np.nan)
            )

    # ── Step 6: Outlier filtering ──────────────────────────────
    logger.info("Step 6 — Filtering price and odometer outliers...")
    before = len(df)
    df = df[
        df["sellingprice"].between(MIN_SELLING_PRICE, MAX_SELLING_PRICE) &
        df["odometer"].between(MIN_ODOMETER, MAX_ODOMETER) &
        df["year"].between(1990, 2026)
    ]
    _drop_report("outliers (price/odometer/year)", before, len(df))

    # ── Step 7: Feature engineering ───────────────────────────
    logger.info("Step 7 — Engineering derived features...")

    # Price margin over Manheim Market Report
    df["price_margin"]     = df["sellingprice"] - df["mmr"]
    df["margin_pct"]       = ((df["price_margin"] / df["mmr"]) * 100).round(2)

    # Time-based features (for Tableau time-series charts)
    df["sale_year"]        = df["saledate"].dt.year
    df["sale_month"]       = df["saledate"].dt.month
    df["sale_month_name"]  = df["saledate"].dt.strftime("%b")
    df["sale_day_of_week"] = df["saledate"].dt.day_name()
    df["sale_quarter"]     = df["saledate"].dt.quarter

    # Vehicle age at time of sale
    df["vehicle_age"]      = df["sale_year"] - df["year"].astype(int)
    df["vehicle_age"]      = df["vehicle_age"].clip(lower=0)

    # Broad price tier (for segmentation)
    df["price_tier"] = pd.cut(
        df["sellingprice"],
        bins=[0, 10_000, 20_000, 35_000, 60_000, 250_000],
        labels=["Budget", "Economy", "Mid-Range", "Premium", "Luxury"],
    )

    # Above/below market flag
    df["above_market"] = (df["price_margin"] > 0).astype(int)

    logger.info(f"  Derived columns added: price_margin, margin_pct, sale_year, "
                f"sale_month, sale_quarter, vehicle_age, price_tier, above_market")

    # ── Step 8: Final dedup on VIN + saledate ─────────────────
    logger.info("Step 8 — Removing duplicate VIN+saledate records...")
    before = len(df)
    df = df.drop_duplicates(subset=["vin", "saledate"], keep="first")
    _drop_report("duplicate VIN+saledate", before, len(df))

    # ── Step 9: Reset index & column ordering ─────────────────
    df = df.reset_index(drop=True)

    final_count = len(df)
    total_dropped = initial_count - final_count
    logger.info(f"TRANSFORM — complete")
    logger.info(f"  Final rows:    {final_count:,}")
    logger.info(f"  Total dropped: {total_dropped:,} ({(total_dropped/initial_count)*100:.1f}%)")

    # ── Step 10: Save processed CSV ───────────────────────────
    CLEANED_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(CLEANED_CSV, index=False)
    logger.info(f"  Processed CSV saved → {CLEANED_CSV}")

    return df


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="%(levelname)s | %(message)s")
    from extract import extract
    raw_df = extract()
    clean_df = transform(raw_df)
    print(clean_df.dtypes)
    print(clean_df[["make", "sellingprice", "mmr", "price_margin", "price_tier"]].head(10))

# etl/load.py
# ─────────────────────────────────────────────────────────────
# LOAD — Step 3 of the ETL Pipeline
#
# Responsibilities:
#   • Connect to MySQL via SQLAlchemy
#   • Load the cleaned DataFrame in chunks (memory-efficient)
#   • Populate dimension tables (dim_make, dim_seller)
#   • Log row counts and timing per chunk
# ─────────────────────────────────────────────────────────────

import logging
import time
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path

import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config import DB_URL, CHUNK_SIZE

logger = logging.getLogger(__name__)


def get_engine():
    """Create and return a SQLAlchemy engine with connection pooling."""
    engine = create_engine(
        DB_URL,
        pool_pre_ping=True,       # Detect stale connections
        pool_size=5,
        max_overflow=10,
        echo=False,
    )
    return engine


def _verify_connection(engine) -> bool:
    """Quick connectivity check before attempting bulk insert."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()
        logger.info("  DB connection verified.")
        return True
    except Exception as e:
        logger.error(f"  DB connection failed: {e}")
        return False


def load_fact_table(df: pd.DataFrame, engine) -> int:
    # Rename condition to match DB schema column name
    df = df.rename(columns={"condition": "condition_score"})
    
    fact_cols = [
        "vin", "year", "make", "model", "trim", "body", "transmission",
        "state", "condition_score", "odometer", "color", "interior", "seller",
        "mmr", "sellingprice", "saledate",
        "price_margin", "margin_pct", "sale_year", "sale_month",
        "sale_month_name", "sale_day_of_week", "sale_quarter",
        "vehicle_age", "price_tier", "above_market",
    ]
    df_load = df[[c for c in fact_cols if c in df.columns]].copy()

    # Convert pandas nullable Int64 to standard Python int for MySQL
    for col in ["year", "odometer", "vehicle_age"]:
        if col in df_load.columns:
            df_load[col] = df_load[col].astype(object).where(df_load[col].notna(), None)

    # Convert Categorical (price_tier) to string
    if "price_tier" in df_load.columns:
        df_load["price_tier"] = df_load["price_tier"].astype(str)

    total_rows = len(df_load)
    inserted = 0
    chunks = (total_rows // CHUNK_SIZE) + 1

    logger.info(f"  Inserting {total_rows:,} rows in {chunks} chunk(s) of {CHUNK_SIZE:,}...")

    for i, start in enumerate(range(0, total_rows, CHUNK_SIZE)):
        chunk = df_load.iloc[start : start + CHUNK_SIZE]
        chunk.to_sql(
            name="car_sales",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",          # Batch VALUES inserts (faster than row-by-row)
            chunksize=200,
        )
        inserted += len(chunk)
        progress = (inserted / total_rows) * 100
        logger.info(f"    Chunk {i+1}/{chunks} — {inserted:,}/{total_rows:,} rows ({progress:.1f}%)")

    return inserted


def load_dim_make(df: pd.DataFrame, engine) -> None:
    """
    Upsert distinct makes + models into the dim_make dimension table.
    This powers the Tableau 'Make/Model' filter hierarchy.
    """
    dim = (
        df[["make", "model"]]
        .drop_duplicates()
        .dropna()
        .sort_values(["make", "model"])
        .reset_index(drop=True)
    )
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM dim_make"))
        conn.commit()
    dim.to_sql("dim_make", con=engine, if_exists="append", index=False)
    logger.info(f"  dim_make loaded: {len(dim):,} make/model combinations")


def load_dim_seller(df: pd.DataFrame, engine) -> None:
    """
    Upsert distinct seller names into the dim_seller dimension table.
    """
    dim = (
        df[["seller"]]
        .drop_duplicates()
        .dropna()
        .sort_values("seller")
        .reset_index(drop=True)
    )
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM dim_seller"))
        conn.commit()
    dim.to_sql("dim_seller", con=engine, if_exists="append", index=False)
    logger.info(f"  dim_seller loaded: {len(dim):,} unique sellers")


def load(df: pd.DataFrame) -> None:
    """
    Main load entry point. Connects to MySQL and populates all tables.

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned dataframe from the Transform step.
    """
    logger.info("=" * 60)
    logger.info("LOAD — starting")

    engine = get_engine()

    if not _verify_connection(engine):
        raise ConnectionError(
            "Cannot connect to MySQL. Check DB_CONFIG in config.py "
            "and ensure the database server is running."
        )

    start_time = time.time()

    # Load dimension tables first (referenced by fact queries)
    logger.info("Loading dimension tables...")
    load_dim_make(df, engine)
    load_dim_seller(df, engine)

    # Load fact table
    logger.info("Loading fact table: car_sales...")
    inserted = load_fact_table(df, engine)

    elapsed = time.time() - start_time
    logger.info(f"LOAD — complete")
    logger.info(f"  Rows loaded:  {inserted:,}")
    logger.info(f"  Elapsed time: {elapsed:.1f}s")

    engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="%(levelname)s | %(message)s")
    from extract import extract
    from transform import transform

    raw_df  = extract()
    clean_df = transform(raw_df)
    load(clean_df)

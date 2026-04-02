# dashboard/tableau_export.py
# ─────────────────────────────────────────────────────────────
# Exports MySQL views to CSV files that Tableau can connect to
# as file-based data sources (useful when Tableau Desktop is not
# connected directly to the database).
#
# Usage:
#   python dashboard/tableau_export.py
#
# Output: data/processed/tableau_*.csv
# ─────────────────────────────────────────────────────────────

import logging
import sys
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config import DB_URL, DATA_PROCESSED_DIR

logging.basicConfig(level="INFO", format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


# Views to export and their corresponding output filenames
EXPORT_TARGETS = {
    "vw_kpi_summary":        "tableau_kpi_summary.csv",
    "vw_monthly_trend":      "tableau_monthly_trend.csv",
    "vw_make_performance":   "tableau_make_performance.csv",
    "vw_seller_leaderboard": "tableau_seller_leaderboard.csv",
    "vw_body_segment":       "tableau_body_segment.csv",
}

# Full fact table subset (for scatter plots / detailed analysis)
DETAIL_QUERY = """
    SELECT
        make, model, body, sale_year, sale_month, sale_quarter,
        state, condition_score, odometer, vehicle_age,
        sellingprice, mmr, price_margin, margin_pct,
        price_tier, above_market, seller
    FROM car_sales
"""


def export_views(engine) -> None:
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    for view_name, filename in EXPORT_TARGETS.items():
        logger.info(f"Exporting {view_name}...")
        df = pd.read_sql(f"SELECT * FROM {view_name}", con=engine)
        out_path = DATA_PROCESSED_DIR / filename
        df.to_csv(out_path, index=False)
        logger.info(f"  → {out_path} ({len(df):,} rows)")


def export_detail(engine) -> None:
    logger.info("Exporting detail table (sampled for Tableau)...")
    df = pd.read_sql(DETAIL_QUERY, con=engine)

    # Sample for large datasets to keep Tableau responsive
    if len(df) > 100_000:
        df = df.sample(n=100_000, random_state=42)
        logger.info(f"  Sampled to 100,000 rows (from {len(df):,})")

    out_path = DATA_PROCESSED_DIR / "tableau_sales_detail.csv"
    df.to_csv(out_path, index=False)
    logger.info(f"  → {out_path} ({len(df):,} rows)")


if __name__ == "__main__":
    engine = create_engine(DB_URL)
    logger.info("Starting Tableau CSV export...")
    export_views(engine)
    export_detail(engine)
    logger.info("Export complete. Load the CSV files in Tableau Desktop.")
    engine.dispose()

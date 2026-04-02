# config.py
# ─────────────────────────────────────────────────────────────
# Central configuration for the Car Sales ETL Pipeline.
# Update DB_CONFIG with your MySQL credentials before running.
# ─────────────────────────────────────────────────────────────

import os
from pathlib import Path

# ── Project Paths ──────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
DATA_RAW_DIR = BASE_DIR / "data" / "raw"
DATA_PROCESSED_DIR = BASE_DIR / "data" / "processed"

RAW_CSV = DATA_RAW_DIR / "car_sales.csv"
CLEANED_CSV = DATA_PROCESSED_DIR / "car_sales_cleaned.csv"

# ── MySQL Connection ───────────────────────────────────────────
# Override any of these via environment variables for security.
DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "your_password_here"),
    "database": os.getenv("DB_NAME", "car_sales_db"),
}

# SQLAlchemy connection string
DB_URL = (
    "mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    .format(**DB_CONFIG)
)

# ── Pipeline Settings ─────────────────────────────────────────
CHUNK_SIZE = 10_000          # Rows per DB insert chunk
LOG_LEVEL = "INFO"

# Outlier thresholds
MIN_SELLING_PRICE = 500      # Drop sales below $500 (data errors)
MAX_SELLING_PRICE = 250_000  # Drop sales above $250K (exotic outliers)
MIN_ODOMETER = 0
MAX_ODOMETER = 500_000

# VIN length validation
VIN_LENGTH = 17

# Expected raw columns (used in schema validation during Extract)
EXPECTED_COLUMNS = [
    "year", "make", "model", "trim", "body", "transmission",
    "vin", "state", "condition", "odometer", "color",
    "interior", "seller", "mmr", "sellingprice", "saledate",
]

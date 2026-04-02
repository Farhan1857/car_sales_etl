# analysis/eda.py
# ─────────────────────────────────────────────────────────────
# Exploratory Data Analysis — Car Dealership Sales
#
# This script mirrors the analysis performed in eda.ipynb.
# Run it standalone or use `jupytext` to convert to .ipynb.
#
#   python analysis/eda.py
#
# Outputs figures to analysis/figures/
# ─────────────────────────────────────────────────────────────

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from pathlib import Path

import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config import CLEANED_CSV

# ── Setup ──────────────────────────────────────────────────────
FIGURES_DIR = Path(__file__).parent / "figures"
FIGURES_DIR.mkdir(exist_ok=True)

sns.set_theme(style="whitegrid", palette="muted")
plt.rcParams.update({"figure.dpi": 120, "axes.titlesize": 13})


def load_data() -> pd.DataFrame:
    """Load the cleaned CSV produced by the Transform step."""
    if not CLEANED_CSV.exists():
        raise FileNotFoundError(
            f"Cleaned CSV not found at {CLEANED_CSV}. "
            "Run the pipeline first: python etl/pipeline.py --skip-load"
        )
    df = pd.read_csv(CLEANED_CSV, parse_dates=["saledate"], low_memory=False)
    print(f"Loaded {len(df):,} records")
    print(df.dtypes)
    return df


# ── Figure 1: Price Distribution ──────────────────────────────
def plot_price_distribution(df: pd.DataFrame) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    axes[0].hist(df["sellingprice"], bins=80, color="#4C72B0", edgecolor="white")
    axes[0].set_title("Selling Price Distribution")
    axes[0].set_xlabel("Selling Price ($)")
    axes[0].set_ylabel("Count")
    axes[0].xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))

    axes[1].hist(df["price_margin"], bins=80, color="#DD8452", edgecolor="white")
    axes[1].axvline(0, color="red", linewidth=1.5, linestyle="--", label="MMR = Selling Price")
    axes[1].set_title("Price Margin over MMR")
    axes[1].set_xlabel("Margin ($)")
    axes[1].set_ylabel("Count")
    axes[1].legend()

    fig.suptitle("Selling Price & Margin Distributions", fontsize=15, fontweight="bold")
    plt.tight_layout()
    fig.savefig(FIGURES_DIR / "01_price_distribution.png")
    plt.close()
    print("Saved: 01_price_distribution.png")


# ── Figure 2: Top Makes by Volume ─────────────────────────────
def plot_top_makes(df: pd.DataFrame) -> None:
    top_makes = (
        df.groupby("make")["sellingprice"]
        .agg(["count", "mean"])
        .rename(columns={"count": "units", "mean": "avg_price"})
        .nlargest(15, "units")
        .reset_index()
    )

    fig, ax = plt.subplots(figsize=(12, 6))
    bars = ax.barh(top_makes["make"][::-1], top_makes["units"][::-1], color="#4C72B0")
    ax.set_xlabel("Units Sold")
    ax.set_title("Top 15 Makes by Sales Volume", fontweight="bold")
    ax.bar_label(bars, fmt="{:,.0f}", padding=4)
    plt.tight_layout()
    fig.savefig(FIGURES_DIR / "02_top_makes_volume.png")
    plt.close()
    print("Saved: 02_top_makes_volume.png")


# ── Figure 3: Monthly Sales Trend ─────────────────────────────
def plot_monthly_trend(df: pd.DataFrame) -> None:
    monthly = (
        df.groupby(["sale_year", "sale_month"])
        .agg(units=("sellingprice", "count"), revenue=("sellingprice", "sum"))
        .reset_index()
        .assign(year_month=lambda d: pd.to_datetime(
            d["sale_year"].astype(str) + "-" + d["sale_month"].astype(str).str.zfill(2)
        ))
        .sort_values("year_month")
    )

    fig, ax1 = plt.subplots(figsize=(14, 5))
    color_units = "#4C72B0"
    color_rev   = "#DD8452"

    ax1.plot(monthly["year_month"], monthly["units"], color=color_units,
             marker="o", linewidth=2, label="Units Sold")
    ax1.set_ylabel("Units Sold", color=color_units)
    ax1.tick_params(axis="y", labelcolor=color_units)

    ax2 = ax1.twinx()
    ax2.plot(monthly["year_month"], monthly["revenue"] / 1e6, color=color_rev,
             marker="s", linewidth=2, linestyle="--", label="Revenue ($M)")
    ax2.set_ylabel("Revenue ($M)", color=color_rev)
    ax2.tick_params(axis="y", labelcolor=color_rev)
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:.1f}M"))

    ax1.set_title("Monthly Sales Volume & Revenue", fontweight="bold")
    ax1.set_xlabel("Month")
    fig.legend(loc="upper left", bbox_to_anchor=(0.1, 0.9))
    fig.autofmt_xdate()
    plt.tight_layout()
    fig.savefig(FIGURES_DIR / "03_monthly_trend.png")
    plt.close()
    print("Saved: 03_monthly_trend.png")


# ── Figure 4: Avg Margin by Make (Top 10) ─────────────────────
def plot_margin_by_make(df: pd.DataFrame) -> None:
    margin = (
        df[df["make"].notna()]
        .groupby("make")
        .agg(avg_margin=("price_margin", "mean"), units=("sellingprice", "count"))
        .query("units >= 100")
        .nlargest(10, "avg_margin")
        .reset_index()
    )

    colors = ["#2ecc71" if m > 0 else "#e74c3c" for m in margin["avg_margin"]]
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(margin["make"], margin["avg_margin"], color=colors, edgecolor="white")
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("Top 10 Makes by Avg Price Margin over MMR", fontweight="bold")
    ax.set_xlabel("Make")
    ax.set_ylabel("Avg Margin ($)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    fig.savefig(FIGURES_DIR / "04_margin_by_make.png")
    plt.close()
    print("Saved: 04_margin_by_make.png")


# ── Figure 5: Body Style Breakdown ────────────────────────────
def plot_body_breakdown(df: pd.DataFrame) -> None:
    body = (
        df[df["body"].notna()]
        .groupby("body")
        .agg(units=("sellingprice", "count"), avg_price=("sellingprice", "mean"))
        .nlargest(8, "units")
        .reset_index()
    )

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    axes[0].pie(body["units"], labels=body["body"], autopct="%1.1f%%",
                startangle=90, colors=sns.color_palette("muted", len(body)))
    axes[0].set_title("Volume by Body Style", fontweight="bold")

    axes[1].bar(body["body"], body["avg_price"], color=sns.color_palette("muted", len(body)))
    axes[1].set_title("Avg Selling Price by Body Style", fontweight="bold")
    axes[1].set_ylabel("Avg Price ($)")
    axes[1].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    plt.xticks(rotation=30, ha="right")

    plt.tight_layout()
    fig.savefig(FIGURES_DIR / "05_body_breakdown.png")
    plt.close()
    print("Saved: 05_body_breakdown.png")


# ── Figure 6: Condition Score vs. Price ───────────────────────
def plot_condition_vs_price(df: pd.DataFrame) -> None:
    cond = (
        df[df["condition_score"].notna()]
        .assign(cond_bucket=lambda d: d["condition_score"].round(0))
        .groupby("cond_bucket")
        .agg(avg_price=("sellingprice", "mean"), avg_margin=("price_margin", "mean"),
             units=("sellingprice", "count"))
        .reset_index()
    )

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(cond["cond_bucket"], cond["avg_price"], marker="o", linewidth=2,
            color="#4C72B0", label="Avg Selling Price")
    ax.set_xlabel("Condition Score (1–5)")
    ax.set_ylabel("Avg Selling Price ($)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.set_title("Vehicle Condition Score vs. Average Selling Price", fontweight="bold")
    ax.legend()
    plt.tight_layout()
    fig.savefig(FIGURES_DIR / "06_condition_vs_price.png")
    plt.close()
    print("Saved: 06_condition_vs_price.png")


# ── Main ───────────────────────────────────────────────────────
if __name__ == "__main__":
    df = load_data()

    print("\n── Descriptive Statistics ──")
    print(df[["sellingprice", "mmr", "price_margin", "margin_pct", "odometer"]].describe().round(2))

    print("\nGenerating figures...")
    plot_price_distribution(df)
    plot_top_makes(df)
    plot_monthly_trend(df)
    plot_margin_by_make(df)
    plot_body_breakdown(df)
    plot_condition_vs_price(df)

    print(f"\nAll figures saved to: {FIGURES_DIR}/")

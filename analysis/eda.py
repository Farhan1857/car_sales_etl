# analysis/eda.py
# ─────────────────────────────────────────────────────────────
# Exploratory Data Analysis — Car Dealership Sales
#
#   python analysis/eda.py
#
# Outputs figures to analysis/figures/
# Questions addressed:
#   Q1. Which make has the highest and lowest avg selling price?
#   Q2. Does color influence price?
#   Q3. What is the relation between condition and body type?
#   Q4. Does body type affect price?
#   Q5. Does make affect condition score?
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
    if not CLEANED_CSV.exists():
        raise FileNotFoundError(
            f"Cleaned CSV not found at {CLEANED_CSV}. "
            "Run the pipeline first: python etl/pipeline.py --skip-load"
        )
    df = pd.read_csv(CLEANED_CSV, parse_dates=["saledate"], low_memory=False)
    print(f"Loaded {len(df):,} records")
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


# ── Figure 2: Monthly Sales Trend ─────────────────────────────
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
    fig.savefig(FIGURES_DIR / "02_monthly_trend.png")
    plt.close()
    print("Saved: 02_monthly_trend.png")


# ── Figure 3: Q1 — Highest & Lowest Avg Price by Make ─────────
def plot_make_price_highlow(df: pd.DataFrame) -> None:
    make_price = (
        df[df["make"].notna()]
        .groupby("make")
        .agg(avg_price=("sellingprice", "mean"), units=("sellingprice", "count"))
        .query("units >= 100")
        .reset_index()
        .sort_values("avg_price")
    )

    top10    = make_price.nlargest(10, "avg_price")
    bottom10 = make_price.nsmallest(10, "avg_price")
    combined = pd.concat([bottom10, top10]).reset_index(drop=True)
    colors   = ["#e74c3c"] * 10 + ["#2ecc71"] * 10

    fig, ax = plt.subplots(figsize=(12, 8))
    bars = ax.barh(combined["make"], combined["avg_price"], color=colors, edgecolor="white")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.set_xlabel("Avg Selling Price ($)")
    ax.set_title(
        "Q1: Highest vs Lowest Avg Selling Price by Make\n"
        "(Top 10 = green  |  Bottom 10 = red  |  min. 100 units sold)",
        fontweight="bold"
    )
    ax.axvline(make_price["avg_price"].mean(), color="black", linewidth=1,
               linestyle="--", label=f"Overall Avg: ${make_price['avg_price'].mean():,.0f}")
    ax.legend()
    ax.bar_label(bars, fmt="$%.0f", padding=4, fontsize=8)
    plt.tight_layout()
    fig.savefig(FIGURES_DIR / "03_make_price_highlow.png")
    plt.close()
    print("Saved: 03_make_price_highlow.png")


# ── Figure 4: Q2 — Does Color Influence Price? ────────────────
def plot_color_vs_price(df: pd.DataFrame) -> None:
    df = df.copy()
    df["color"] = df["color"].str.strip().str.title()

    color_price = (
        df[df["color"].notna() & ~df["color"].isin(["—", "Nan", "nan"])]
        .groupby("color")
        .agg(avg_price=("sellingprice", "mean"), units=("sellingprice", "count"))
        .query("units >= 500")
        .reset_index()
        .sort_values("avg_price", ascending=False)
    )

    overall_avg = df["sellingprice"].mean()
    color_price["diff_from_avg"] = color_price["avg_price"] - overall_avg
    bar_colors = ["#2ecc71" if d >= 0 else "#e74c3c" for d in color_price["diff_from_avg"]]

    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    axes[0].barh(
        color_price["color"][::-1],
        color_price["avg_price"][::-1],
        color=bar_colors[::-1],
        edgecolor="white"
    )
    axes[0].axvline(overall_avg, color="black", linewidth=1.2, linestyle="--",
                    label=f"Overall Avg: ${overall_avg:,.0f}")
    axes[0].xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    axes[0].set_title("Avg Selling Price by Color\n(green = above avg, red = below avg)", fontweight="bold")
    axes[0].set_xlabel("Avg Selling Price ($)")
    axes[0].legend(fontsize=9)

    axes[1].barh(
        color_price["color"][::-1],
        color_price["units"][::-1],
        color="#4C72B0",
        edgecolor="white"
    )
    axes[1].xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    axes[1].set_title("Units Sold by Color", fontweight="bold")
    axes[1].set_xlabel("Units Sold")

    fig.suptitle(
        "Q2: Does Vehicle Color Influence Price?  (Colors with ≥500 units sold)",
        fontsize=14, fontweight="bold"
    )
    plt.tight_layout()
    fig.savefig(FIGURES_DIR / "04_color_vs_price.png")
    plt.close()
    print("Saved: 04_color_vs_price.png")


# ── Figure 5: Q3 — Condition vs Body Type ─────────────────────
def plot_condition_by_body(df: pd.DataFrame) -> None:
    top_bodies = (
        df[df["body"].notna()]
        .groupby("body")["condition"]
        .count()
        .nlargest(8)
        .index.tolist()
    )

    plot_df = df[df["body"].isin(top_bodies)].copy()
    order = (
        plot_df.groupby("body")["condition"]
        .median()
        .sort_values(ascending=False)
        .index.tolist()
    )

    fig, ax = plt.subplots(figsize=(14, 6))
    sns.boxplot(
        data=plot_df,
        x="body",
        y="condition",
        order=order,
        palette="muted",
        ax=ax,
        flierprops={"marker": ".", "markersize": 2, "alpha": 0.3}
    )
    ax.set_title(
        "Q3: Condition Score Distribution by Body Type\n"
        "(Higher score = better condition  |  Box = IQR  |  Line = median)",
        fontweight="bold"
    )
    ax.set_xlabel("Body Type")
    ax.set_ylabel("Condition Score")
    plt.xticks(rotation=25, ha="right")
    plt.tight_layout()
    fig.savefig(FIGURES_DIR / "05_condition_by_body.png")
    plt.close()
    print("Saved: 05_condition_by_body.png")


# ── Figure 6: Q4 — Does Body Type Affect Price? ───────────────
def plot_body_vs_price(df: pd.DataFrame) -> None:
    top_bodies = (
        df[df["body"].notna()]
        .groupby("body")["sellingprice"]
        .count()
        .nlargest(8)
        .index.tolist()
    )

    body_stats = (
        df[df["body"].isin(top_bodies)]
        .groupby("body")["sellingprice"]
        .mean()
        .reset_index()
        .rename(columns={"sellingprice": "avg"})
        .sort_values("avg", ascending=False)
    )

    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    palette = sns.color_palette("muted", len(body_stats))
    bars = axes[0].bar(
        body_stats["body"], body_stats["avg"],
        color=palette, edgecolor="white"
    )
    axes[0].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    axes[0].set_title("Avg Selling Price by Body Type", fontweight="bold")
    axes[0].set_ylabel("Avg Selling Price ($)")
    axes[0].bar_label(bars, fmt="$%.0f", padding=4, fontsize=8)
    plt.setp(axes[0].get_xticklabels(), rotation=25, ha="right")

    plot_df = df[df["body"].isin(top_bodies)].copy()
    order = body_stats["body"].tolist()
    sns.violinplot(
        data=plot_df,
        x="body",
        y="sellingprice",
        order=order,
        palette="muted",
        ax=axes[1],
        inner="quartile",
        cut=0
    )
    axes[1].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    axes[1].set_title("Price Distribution by Body Type", fontweight="bold")
    axes[1].set_ylabel("Selling Price ($)")
    axes[1].set_xlabel("Body Type")
    plt.setp(axes[1].get_xticklabels(), rotation=25, ha="right")

    fig.suptitle("Q4: Does Body Type Affect Selling Price?", fontsize=14, fontweight="bold")
    plt.tight_layout()
    fig.savefig(FIGURES_DIR / "06_body_vs_price.png")
    plt.close()
    print("Saved: 06_body_vs_price.png")


# ── Figure 7: Q5 — Does Make Affect Condition Score? ──────────
def plot_make_vs_condition(df: pd.DataFrame) -> None:
    top_makes = (
        df[df["make"].notna() & df["condition"].notna()]
        .groupby("make")["condition"]
        .count()
        .nlargest(15)
        .index.tolist()
    )

    make_cond = (
        df[df["make"].isin(top_makes)]
        .groupby("make")["condition"]
        .agg(avg="mean", std="std")
        .reset_index()
        .sort_values("avg", ascending=False)
    )

    overall_avg = df["condition"].mean()
    bar_colors = ["#2ecc71" if v >= overall_avg else "#e74c3c" for v in make_cond["avg"]]

    fig, axes = plt.subplots(1, 2, figsize=(18, 7))

    axes[0].barh(
        make_cond["make"][::-1],
        make_cond["avg"][::-1],
        xerr=make_cond["std"][::-1],
        color=bar_colors[::-1],
        edgecolor="white",
        capsize=3,
        error_kw={"elinewidth": 1, "ecolor": "gray"}
    )
    axes[0].axvline(overall_avg, color="black", linewidth=1.2, linestyle="--",
                    label=f"Overall Avg: {overall_avg:.1f}")
    axes[0].set_title("Avg Condition Score by Make\n(error bars = std dev)", fontweight="bold")
    axes[0].set_xlabel("Avg Condition Score")
    axes[0].legend(fontsize=9)

    plot_df = df[df["make"].isin(top_makes) & df["condition"].notna()].copy()
    plot_df["cond_bucket"] = pd.cut(
        plot_df["condition"],
        bins=[0, 10, 20, 30, 40, 50],
        labels=["0-10", "10-20", "20-30", "30-40", "40-50"]
    )
    heat = (
        plot_df.groupby(["make", "cond_bucket"], observed=True)
        .size()
        .unstack(fill_value=0)
    )
    heat_pct = heat.div(heat.sum(axis=1), axis=0) * 100
    heat_pct = heat_pct.loc[make_cond["make"].tolist()]

    sns.heatmap(
        heat_pct,
        ax=axes[1],
        cmap="YlOrRd",
        annot=True,
        fmt=".0f",
        linewidths=0.5,
        cbar_kws={"label": "% of Make's Inventory"}
    )
    axes[1].set_title("Condition Score Distribution by Make (%)", fontweight="bold")
    axes[1].set_xlabel("Condition Score Range")
    axes[1].set_ylabel("Make")

    fig.suptitle("Q5: Does Vehicle Make Affect Condition Score?", fontsize=14, fontweight="bold")
    plt.tight_layout()
    fig.savefig(FIGURES_DIR / "07_make_vs_condition.png")
    plt.close()
    print("Saved: 07_make_vs_condition.png")


# ── Main ───────────────────────────────────────────────────────
if __name__ == "__main__":
    df = load_data()

    print("\n── Descriptive Statistics ──")
    print(df[["sellingprice", "mmr", "price_margin", "margin_pct", "odometer"]].describe().round(2))

    print("\nGenerating figures...")
    plot_price_distribution(df)
    plot_monthly_trend(df)
    plot_make_price_highlow(df)
    plot_color_vs_price(df)
    plot_condition_by_body(df)
    plot_body_vs_price(df)
    plot_make_vs_condition(df)

    print(f"\nAll figures saved to: {FIGURES_DIR}/")
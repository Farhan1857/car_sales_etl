# docs/tableau_dashboard_guide.md
# ─────────────────────────────────────────────────────────────
# Tableau Dashboard Setup Guide
# Car Dealership Sales Analysis
# ─────────────────────────────────────────────────────────────

## Overview

The Tableau workbook (`dashboard/Car_Sales_Analysis.twbx`) contains **3 dashboards**
powered by the MySQL views created in `sql/views.sql`.

---

## Data Source Setup

### Option A — Connect to MySQL directly (recommended)
1. Open Tableau Desktop → **Connect → MySQL**
2. Enter host, port, database credentials from `config.py`
3. Import views: `vw_kpi_summary`, `vw_monthly_trend`, `vw_make_performance`,
   `vw_seller_leaderboard`, `vw_body_segment`

### Option B — Use exported CSVs
1. Run: `python dashboard/tableau_export.py`
2. Open Tableau Desktop → **Connect → Text File**
3. Load each `data/processed/tableau_*.csv`

---

## Dashboard 1: Executive Summary

**Purpose:** High-level KPIs for dealership leadership.

| Sheet | Chart Type | Data Source | Key Metric |
|---|---|---|---|
| Total Revenue | KPI Card | vw_kpi_summary | `total_revenue` |
| Avg Margin % | KPI Card | vw_kpi_summary | `avg_margin_pct` |
| Units Sold | KPI Card | vw_kpi_summary | `total_units_sold` |
| % Above Market | KPI Card | vw_kpi_summary | computed |
| Revenue by Make | Horizontal Bar | vw_make_performance | `total_revenue` |
| Volume by Body Style | Donut | vw_body_segment | `units_sold` |

**Filters:** Year, Make (multi-select)

---

## Dashboard 2: Sales Trend Analysis

**Purpose:** Track performance over time for sales team reporting.

| Sheet | Chart Type | Data Source | Key Metric |
|---|---|---|---|
| Monthly Revenue | Dual-axis line | vw_monthly_trend | `total_revenue`, `units_sold` |
| Avg Price vs. MMR | Line chart | vw_monthly_trend | `avg_price`, `avg_mmr` |
| Margin Trend | Area chart | vw_monthly_trend | `avg_margin` |
| Day-of-Week Heatmap | Heatmap | car_sales (detail) | `units_sold` |

**Filters:** Year, Quarter, Body Style

---

## Dashboard 3: Market Intelligence

**Purpose:** Pricing strategy and seller benchmarking.

| Sheet | Chart Type | Data Source | Key Metric |
|---|---|---|---|
| Make Margin Ranking | Ranked bar | vw_make_performance | `avg_margin_pct` |
| Seller Leaderboard | Table | vw_seller_leaderboard | revenue, margin, % above market |
| Condition vs. Price | Scatter | car_sales (detail) | `condition_score` vs `sellingprice` |
| Price Tier Split | Stacked bar | car_sales (detail) | by make + price_tier |

**Filters:** Make, Seller, Price Tier

---

## Calculated Fields (Tableau)

Add these in Tableau if not already in the data source:

```
// Margin flag
[sellingprice] > [mmr]

// Formatted margin %
STR(ROUND([margin_pct], 1)) + "%"

// Above/Below Market Label
IF [above_market] = 1 THEN "Above Market" ELSE "Below Market" END
```

---

## Color Scheme

| Meaning | Color |
|---|---|
| Primary / Revenue | `#4C72B0` (Blue) |
| Margin / Positive | `#2ECC71` (Green) |
| Below Market | `#E74C3C` (Red) |
| Neutral / Volume | `#DD8452` (Orange) |

---

## Publishing (Optional)

To publish to Tableau Public:
1. File → Save to Tableau Public As...
2. Sign in with your Tableau Public account
3. Set permissions to Public

> ⚠️ Do not publish dashboards containing real PII (VINs, seller names)
> without reviewing your data sharing policy.

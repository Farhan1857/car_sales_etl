-- sql/views.sql
-- ─────────────────────────────────────────────────────────────
-- MySQL Views — Pre-aggregated data sources for Tableau
-- Connect Tableau directly to these views for best performance.
-- ─────────────────────────────────────────────────────────────

USE car_sales_db;

-- ── View 1: Monthly Revenue & Volume Trend ─────────────────────
CREATE OR REPLACE VIEW vw_monthly_trend AS
SELECT
    CONCAT(sale_year, '-', LPAD(sale_month, 2, '0'))  AS year_month,
    sale_year,
    sale_month,
    sale_month_name,
    COUNT(*)                                           AS units_sold,
    ROUND(SUM(sellingprice), 2)                        AS total_revenue,
    ROUND(AVG(sellingprice), 2)                        AS avg_price,
    ROUND(AVG(price_margin), 2)                        AS avg_margin,
    ROUND(AVG(margin_pct), 2)                          AS avg_margin_pct
FROM car_sales
GROUP BY sale_year, sale_month, sale_month_name;


-- ── View 2: Make-Level Performance ────────────────────────────
CREATE OR REPLACE VIEW vw_make_performance AS
SELECT
    make,
    COUNT(*)                        AS units_sold,
    ROUND(SUM(sellingprice), 2)     AS total_revenue,
    ROUND(AVG(sellingprice), 2)     AS avg_price,
    ROUND(AVG(mmr), 2)              AS avg_mmr,
    ROUND(AVG(price_margin), 2)     AS avg_margin,
    ROUND(AVG(margin_pct), 2)       AS avg_margin_pct,
    ROUND(SUM(CASE WHEN above_market = 1 THEN 1 ELSE 0 END) / COUNT(*) * 100, 1)
                                    AS pct_above_market
FROM car_sales
GROUP BY make;


-- ── View 3: Seller Leaderboard ─────────────────────────────────
CREATE OR REPLACE VIEW vw_seller_leaderboard AS
SELECT
    seller,
    COUNT(*)                        AS units_sold,
    ROUND(SUM(sellingprice), 2)     AS total_revenue,
    ROUND(AVG(sellingprice), 2)     AS avg_price,
    ROUND(AVG(price_margin), 2)     AS avg_margin,
    ROUND(AVG(margin_pct), 2)       AS avg_margin_pct
FROM car_sales
WHERE seller IS NOT NULL
GROUP BY seller
HAVING units_sold >= 10;


-- ── View 4: Body Style Segmentation ───────────────────────────
CREATE OR REPLACE VIEW vw_body_segment AS
SELECT
    body,
    COUNT(*)                        AS units_sold,
    ROUND(AVG(sellingprice), 2)     AS avg_price,
    ROUND(AVG(price_margin), 2)     AS avg_margin,
    ROUND(SUM(sellingprice), 2)     AS total_revenue,
    ROUND(AVG(odometer), 0)         AS avg_odometer
FROM car_sales
WHERE body IS NOT NULL
GROUP BY body;


-- ── View 5: KPI Summary (single-row dashboard card source) ─────
CREATE OR REPLACE VIEW vw_kpi_summary AS
SELECT
    COUNT(*)                        AS total_units_sold,
    ROUND(SUM(sellingprice), 2)     AS total_revenue,
    ROUND(AVG(sellingprice), 2)     AS avg_selling_price,
    ROUND(AVG(mmr), 2)              AS avg_mmr,
    ROUND(AVG(price_margin), 2)     AS avg_margin,
    ROUND(AVG(margin_pct), 2)       AS avg_margin_pct,
    COUNT(DISTINCT make)            AS unique_makes,
    COUNT(DISTINCT seller)          AS unique_sellers,
    MIN(saledate)                   AS earliest_sale,
    MAX(saledate)                   AS latest_sale
FROM car_sales;

-- sql/business_queries.sql
-- ─────────────────────────────────────────────────────────────
-- Business Insight Queries for Car Dealership Sales Analysis
-- These queries are designed to answer specific business questions
-- and serve as data sources for Tableau dashboards.
-- ─────────────────────────────────────────────────────────────

USE car_sales_db;


-- ──────────────────────────────────────────────────────────────
-- Q1: OVERALL SALES SUMMARY (KPI Cards)
-- ──────────────────────────────────────────────────────────────
SELECT
    COUNT(*)                                    AS total_units_sold,
    ROUND(SUM(sellingprice), 2)                 AS total_revenue,
    ROUND(AVG(sellingprice), 2)                 AS avg_selling_price,
    ROUND(AVG(mmr), 2)                          AS avg_mmr,
    ROUND(AVG(price_margin), 2)                 AS avg_price_margin,
    ROUND(AVG(margin_pct), 2)                   AS avg_margin_pct,
    ROUND(SUM(CASE WHEN above_market = 1 THEN 1 ELSE 0 END) / COUNT(*) * 100, 1)
                                                AS pct_above_market
FROM car_sales;


-- ──────────────────────────────────────────────────────────────
-- Q2: AVERAGE MARGIN BY MAKE (Top 15)
--     Identifies which brands command the highest premiums over MMR
-- ──────────────────────────────────────────────────────────────
SELECT
    make,
    COUNT(*)                        AS units_sold,
    ROUND(AVG(sellingprice), 2)     AS avg_selling_price,
    ROUND(AVG(mmr), 2)              AS avg_mmr,
    ROUND(AVG(price_margin), 2)     AS avg_margin,
    ROUND(AVG(margin_pct), 2)       AS avg_margin_pct,
    ROUND(SUM(sellingprice), 2)     AS total_revenue
FROM car_sales
GROUP BY make
HAVING units_sold >= 100
ORDER BY avg_margin DESC
LIMIT 15;


-- ──────────────────────────────────────────────────────────────
-- Q3: MONTHLY SALES VOLUME AND REVENUE TREND
--     Time-series for Tableau line chart
-- ──────────────────────────────────────────────────────────────
SELECT
    sale_year,
    sale_month,
    sale_month_name,
    CONCAT(sale_year, '-', LPAD(sale_month, 2, '0'))  AS year_month,
    COUNT(*)                                           AS units_sold,
    ROUND(SUM(sellingprice), 2)                        AS total_revenue,
    ROUND(AVG(sellingprice), 2)                        AS avg_selling_price,
    ROUND(AVG(price_margin), 2)                        AS avg_margin
FROM car_sales
GROUP BY sale_year, sale_month, sale_month_name
ORDER BY sale_year, sale_month;


-- ──────────────────────────────────────────────────────────────
-- Q4: SALES VOLUME AND MARGIN BY BODY STYLE
--     Powers the segment breakdown bar chart
-- ──────────────────────────────────────────────────────────────
SELECT
    body,
    COUNT(*)                    AS units_sold,
    ROUND(AVG(sellingprice), 2) AS avg_selling_price,
    ROUND(AVG(mmr), 2)          AS avg_mmr,
    ROUND(AVG(price_margin), 2) AS avg_margin,
    ROUND(AVG(margin_pct), 2)   AS avg_margin_pct,
    ROUND(SUM(sellingprice), 2) AS total_revenue
FROM car_sales
WHERE body IS NOT NULL
GROUP BY body
ORDER BY units_sold DESC;


-- ──────────────────────────────────────────────────────────────
-- Q5: TOP SELLERS BY REVENUE AND MARGIN
--     Powers the seller leaderboard table in Tableau
-- ──────────────────────────────────────────────────────────────
SELECT
    seller,
    COUNT(*)                    AS units_sold,
    ROUND(SUM(sellingprice), 2) AS total_revenue,
    ROUND(AVG(sellingprice), 2) AS avg_selling_price,
    ROUND(AVG(price_margin), 2) AS avg_margin,
    ROUND(AVG(margin_pct), 2)   AS avg_margin_pct,
    ROUND(SUM(CASE WHEN above_market = 1 THEN 1 ELSE 0 END) / COUNT(*) * 100, 1)
                                AS pct_above_market
FROM car_sales
WHERE seller IS NOT NULL
GROUP BY seller
HAVING units_sold >= 50
ORDER BY total_revenue DESC
LIMIT 20;


-- ──────────────────────────────────────────────────────────────
-- Q6: CONDITION SCORE vs. PRICE PREMIUM
--     Scatter plot data: does better condition = higher margin?
-- ──────────────────────────────────────────────────────────────
SELECT
    ROUND(condition_score, 0)   AS condition_bucket,
    COUNT(*)                    AS units_sold,
    ROUND(AVG(sellingprice), 2) AS avg_selling_price,
    ROUND(AVG(mmr), 2)          AS avg_mmr,
    ROUND(AVG(price_margin), 2) AS avg_margin,
    ROUND(AVG(odometer), 0)     AS avg_odometer
FROM car_sales
WHERE condition_score IS NOT NULL
GROUP BY condition_bucket
ORDER BY condition_bucket;


-- ──────────────────────────────────────────────────────────────
-- Q7: PRICE TIER DISTRIBUTION
--     Donut / pie chart — how is volume spread across segments?
-- ──────────────────────────────────────────────────────────────
SELECT
    price_tier,
    COUNT(*)                        AS units_sold,
    ROUND(SUM(sellingprice), 2)     AS total_revenue,
    ROUND(AVG(sellingprice), 2)     AS avg_price,
    ROUND(COUNT(*) / SUM(COUNT(*)) OVER() * 100, 1) AS pct_of_total
FROM car_sales
WHERE price_tier IS NOT NULL AND price_tier != 'nan'
GROUP BY price_tier
ORDER BY avg_price;


-- ──────────────────────────────────────────────────────────────
-- Q8: VEHICLE AGE vs. AVERAGE SELLING PRICE
--     Depreciation curve analysis
-- ──────────────────────────────────────────────────────────────
SELECT
    vehicle_age,
    COUNT(*)                    AS units_sold,
    ROUND(AVG(sellingprice), 2) AS avg_price,
    ROUND(AVG(mmr), 2)          AS avg_mmr,
    ROUND(AVG(odometer), 0)     AS avg_odometer
FROM car_sales
WHERE vehicle_age BETWEEN 0 AND 15
GROUP BY vehicle_age
ORDER BY vehicle_age;


-- ──────────────────────────────────────────────────────────────
-- Q9: SALES BY STATE (for geographic map visualization)
-- ──────────────────────────────────────────────────────────────
SELECT
    state,
    COUNT(*)                    AS units_sold,
    ROUND(AVG(sellingprice), 2) AS avg_price,
    ROUND(AVG(price_margin), 2) AS avg_margin,
    ROUND(SUM(sellingprice), 2) AS total_revenue
FROM car_sales
WHERE state IS NOT NULL AND LENGTH(state) = 2
GROUP BY state
ORDER BY units_sold DESC;


-- ──────────────────────────────────────────────────────────────
-- Q10: TOP MAKE + MODEL COMBINATIONS BY VOLUME
-- ──────────────────────────────────────────────────────────────
SELECT
    make,
    model,
    COUNT(*)                    AS units_sold,
    ROUND(AVG(sellingprice), 2) AS avg_price,
    ROUND(AVG(price_margin), 2) AS avg_margin,
    ROUND(MIN(sellingprice), 2) AS min_price,
    ROUND(MAX(sellingprice), 2) AS max_price
FROM car_sales
GROUP BY make, model
HAVING units_sold >= 30
ORDER BY units_sold DESC
LIMIT 25;

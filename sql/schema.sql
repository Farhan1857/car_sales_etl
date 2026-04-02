-- sql/schema.sql
-- ─────────────────────────────────────────────────────────────
-- MySQL DDL — Car Sales ETL Database Schema
-- Run once before executing the pipeline:
--   mysql -u your_user -p < sql/schema.sql
-- ─────────────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS car_sales_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE car_sales_db;

-- ── Dimension: Make / Model ────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_make (
    id    INT          NOT NULL AUTO_INCREMENT,
    make  VARCHAR(100) NOT NULL,
    model VARCHAR(100) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_make_model (make, model)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Dimension: Seller ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_seller (
    id     INT          NOT NULL AUTO_INCREMENT,
    seller VARCHAR(255) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_seller (seller)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Fact Table: Car Sales ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS car_sales (
    id              INT            NOT NULL AUTO_INCREMENT,

    -- Vehicle identity
    vin             CHAR(17)       NOT NULL,
    year            SMALLINT       NOT NULL,
    make            VARCHAR(100)   NOT NULL,
    model           VARCHAR(100)   NOT NULL,
    trim            VARCHAR(100),
    body            VARCHAR(50),
    transmission    VARCHAR(50),

    -- Sale context
    state           CHAR(2),
    condition_score DECIMAL(4,1),
    odometer        INT,
    color           VARCHAR(50),
    interior        VARCHAR(50),
    seller          VARCHAR(255),

    -- Pricing
    mmr             DECIMAL(10,2),
    sellingprice    DECIMAL(10,2)  NOT NULL,
    price_margin    DECIMAL(10,2),
    margin_pct      DECIMAL(8,2),

    -- Derived time features
    saledate        DATETIME       NOT NULL,
    sale_year       SMALLINT,
    sale_month      TINYINT,
    sale_month_name VARCHAR(3),
    sale_day_of_week VARCHAR(9),
    sale_quarter    TINYINT,

    -- Derived vehicle features
    vehicle_age     TINYINT,
    price_tier      VARCHAR(20),
    above_market    TINYINT(1)     DEFAULT 0,

    PRIMARY KEY (id),
    UNIQUE KEY uq_vin_saledate (vin, saledate),

    -- Indexes for common query patterns
    INDEX idx_make       (make),
    INDEX idx_model      (model),
    INDEX idx_seller     (seller),
    INDEX idx_saledate   (saledate),
    INDEX idx_sale_year  (sale_year),
    INDEX idx_sale_month (sale_month),
    INDEX idx_body       (body),
    INDEX idx_price_tier (price_tier)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ── Verify ────────────────────────────────────────────────────
SHOW TABLES;

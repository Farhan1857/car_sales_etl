# 🚗 ETL Pipeline for Car Dealership Sales Analysis

> **Stack:** Python · SQL · MySQL · Tableau · Pandas · SQLAlchemy

---

## 📌 Overview

This project implements a production-style **ETL (Extract → Transform → Load) pipeline** that processes raw used-car auction sales data from a CSV source, cleans and enriches it using Python, loads it into a **MySQL** relational database, and exposes the results for **Tableau** dashboards that surface key business KPIs.

The pipeline was designed with a real-world dealership business problem in mind:

> *"How do we identify which makes, models, and market segments are generating the highest margins — and which are being consistently overpriced or underpriced at auction?"*

---

## 📂 Project Structure

```
car_sales_etl/
│
├── data/
│   ├── raw/                    # Original source CSV (unmodified)
│   └── processed/              # Cleaned, transformed output CSVs
│
├── etl/
│   ├── extract.py              # Step 1: Data extraction & validation
│   ├── transform.py            # Step 2: Cleaning, enrichment, feature engineering
│   ├── load.py                 # Step 3: MySQL loading via SQLAlchemy
│   └── pipeline.py             # Orchestrator — runs full ETL end-to-end
│
├── sql/
│   ├── schema.sql              # DDL: database & table definitions
│   ├── business_queries.sql    # Key business insight queries
│   └── views.sql               # MySQL views for Tableau data sources
│
├── analysis/
│   └── eda.ipynb               # Exploratory Data Analysis notebook
│
├── dashboard/
│   └── tableau_export.py       # Script to export MySQL views → Tableau-ready CSVs
│
├── docs/
│   └── pipeline_diagram.png    # Architecture diagram
│
├── config.py                   # DB credentials & pipeline settings
├── requirements.txt            # Python dependencies
└── README.md
```

---

## 🏗️ Architecture

```
[Raw CSV Source]
      │
      ▼
┌─────────────┐     ┌──────────────────┐     ┌──────────────┐
│  EXTRACT    │────▶│   TRANSFORM      │────▶│    LOAD      │
│             │     │                  │     │              │
│ • Read CSV  │     │ • Null handling  │     │ • MySQL DB   │
│ • Validate  │     │ • Type casting   │     │ • SQLAlchemy │
│ • Schema    │     │ • Date parsing   │     │ • Upsert     │
│   check     │     │ • Feature eng.   │     │   logic      │
│             │     │ • Outlier filter │     │              │
└─────────────┘     └──────────────────┘     └──────────────┘
                                                     │
                                                     ▼
                                            ┌──────────────┐
                                            │   MySQL DB   │
                                            │              │
                                            │ • car_sales  │
                                            │ • dim_make   │
                                            │ • dim_seller │
                                            │ • Views      │
                                            └──────────────┘
                                                     │
                                                     ▼
                                            ┌──────────────┐
                                            │   Tableau    │
                                            │  Dashboard   │
                                            │              │
                                            │ • KPI Cards  │
                                            │ • Trend View │
                                            │ • Margin Map │
                                            └──────────────┘
```

---

## ⚙️ Setup & Installation

### Prerequisites

- Python 3.9+
- MySQL 8.0+
- Tableau Desktop (for dashboard)

### 1. Clone the Repository

```bash
git clone https://github.com/<your-username>/car-sales-etl.git
cd car-sales-etl
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Database Connection

Edit `config.py` with your MySQL credentials:

```python
DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "your_user",
    "password": "your_password",
    "database": "car_sales_db"
}
```

### 4. Initialize the Database Schema

```bash
mysql -u your_user -p < sql/schema.sql
```

### 5. Add Source Data

Place your source CSV in `data/raw/car_sales.csv`. The pipeline expects the following columns:

| Column | Type | Description |
|---|---|---|
| `year` | int | Vehicle model year |
| `make` | str | Manufacturer (e.g., BMW, Kia) |
| `model` | str | Model name |
| `trim` | str | Trim level |
| `body` | str | Body style (SUV, Sedan, etc.) |
| `transmission` | str | Transmission type |
| `vin` | str | Vehicle Identification Number |
| `state` | str | State of sale |
| `condition` | float | Condition score (1–5) |
| `odometer` | int | Mileage |
| `color` | str | Exterior color |
| `interior` | str | Interior color |
| `seller` | str | Seller/dealer name |
| `mmr` | float | Manheim Market Report value |
| `sellingprice` | float | Final sale price |
| `saledate` | str | Timestamp of sale |

### 6. Run the Full Pipeline

```bash
python etl/pipeline.py
```

---

## 📊 Key Business Insights (SQL)

The `sql/business_queries.sql` file contains queries that answer:

1. **Which makes generate the highest average margin over MMR?**
2. **How do selling prices trend by month and body style?**
3. **Which sellers are consistently pricing above or below market?**
4. **What is the correlation between condition score and price premium?**
5. **Which vehicle segments move fastest (lowest odometer, highest volume)?**

---

## 📈 Tableau Dashboard KPIs

The dashboard (connect to MySQL or use exported CSVs from `dashboard/tableau_export.py`) visualizes:

| KPI | Description |
|---|---|
| **Total Revenue** | Sum of all selling prices |
| **Avg. Price vs. MMR** | Market benchmark comparison |
| **Margin by Make** | Profit gap above/below MMR |
| **Monthly Sales Volume** | Units sold over time |
| **Top Sellers by Revenue** | Seller performance ranking |
| **Body Style Breakdown** | Volume & price by segment |

---

## 🧪 ETL Performance

| Stage | Records In | Records Out | Time |
|---|---|---|---|
| Extract | 558,837 | 558,837 | ~2.1s |
| Transform | 558,837 | 521,694 | ~8.4s |
| Load | 521,694 | 521,694 | ~44s |

> Transform drops ~6.5% of records due to null critical fields, invalid VINs, or extreme price outliers.

---

## 🗂️ Dataset

Source: [Kaggle — Used Car Auction Prices](https://www.kaggle.com/datasets/tunguz/used-car-auction-prices)

The dataset contains 558K+ real auction records from the US used-car market (2014–2015), including MMR (Manheim Market Report) benchmark values — making it ideal for margin analysis.

---

## 📝 License

MIT License — free to use, adapt, and reference for portfolio purposes.

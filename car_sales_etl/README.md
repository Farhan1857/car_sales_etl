# 🚗 ETL Pipeline for Car Dealership Sales Analysis

> **Stack:** Python · SQL · MySQL · Tableau · Pandas · SQLAlchemy

---

## 📌 Overview

This project implements a production-style **ETL (Extract → Transform → Load) pipeline** that processes raw used-car auction sales data from a CSV source, cleans and enriches it using Python, loads it into a **MySQL** relational database, and exposes the results for **Tableau** dashboards that surface key business KPIs.

The pipeline was designed with a real-world dealership business problem in mind:

> *"How do we identify which makes, models, and market segments are generating the highest margins - and which are being consistently overpriced or underpriced at auction?"*



## 🏗️ Architecture




## ⚙️ Setup

### 1. Configure Database Connection

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

### 2. Initialize the Database Schema

```bash
mysql -u your_user -p < sql/schema.sql
```

### 3. Add Source Data

Place your source CSV in `data/raw/car_sales.csv`. 

### 4. Run the Full Pipeline

```bash
python etl/pipeline.py
```


## 📊 Key Business Insights (SQL)

The `sql/business_queries.sql` file contains queries that answer:

1. **Which makes generate the highest average margin over MMR?**
2. **How do selling prices trend by month and body style?**
3. **Which sellers are consistently pricing above or below market?**
4. **What is the correlation between condition score and price premium?**
5. **Which vehicle segments move fastest (lowest odometer, highest volume)?**

---

## 📈 Tableau Dashboard KPIs

To see the dashboard, connect to MySQL or use exported CSVs from `dashboard/tableau_export.py`


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

MIT License - free to use, adapt, and reference for portfolio purposes.

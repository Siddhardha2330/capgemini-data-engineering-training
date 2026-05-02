# Day 3 - Flipkart Data Pipeline with Medallion Architecture

## Overview

This day focuses on building a Flipkart-style sales pipeline using Medallion Architecture. The notebook is structured around Bronze, Silver, and Gold layers, showing how raw data is ingested, cleaned, and transformed into business-ready analytics.

---

## Objective

- Understand Medallion Architecture using Bronze, Silver, and Gold layers
- Build a layered PySpark and Delta pipeline
- Clean nulls, invalid values, and duplicate records
- Generate business insights from curated Gold-layer data

---

## Pipeline Layers

### Bronze Layer

- Loaded raw source data as-is
- Preserved nulls, duplicates, and invalid values
- Wrote raw data into a Delta location for traceability

### Silver Layer

- Read Bronze data
- Filled null values such as missing amount and city
- Cast `amount` to integer
- Converted `date` into proper date type
- Filtered negative amount values
- Removed duplicates using `row_number()` over `order_id`, keeping the latest record
- Wrote cleaned data to the Silver layer

### Gold Layer

- Read curated Silver data
- Wrote business-ready data to the Gold layer
- Used Gold data for analytical aggregations

---

## Business Analysis Performed

- Total sales by product
- Total sales by category
- Total sales by city
- Total orders and total spend by customer
- Highest-spending customer
- Top-selling product

---

## Key Concepts Used

- Medallion Architecture
- Delta Lake storage
- `fillna()`
- `cast()`
- `to_date()`
- `filter()`
- window functions
- `row_number()`
- `groupBy()`
- `sum()`
- `count()`
- layered pipeline design

---

## Learning Outcome

By the end of this task, I learned how to:

- Separate raw, cleaned, and curated data into different layers
- Use Delta format for pipeline stages
- Clean and deduplicate data systematically
- Build business aggregations from Gold-layer datasets
- Design a simple production-style data engineering workflow

---

## Files

- `Day 8 Flipkart Data Pipeline Medallion Architecture (2).ipynb` - notebook implementation
- `Flipkart Data Pipeline Medallion Architecture Assignment.pdf` - assignment/problem statement
- `README.md` - day summary

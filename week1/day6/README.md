# Day 6 - Car Sales Data Engineering Pipeline

## Overview

This assignment builds a PySpark-based car sales pipeline using multiple related datasets such as customers, cars, sales, dealers, and sales-dealer mappings. The work includes data validation, cleaning, joins, business analysis, SQL-based reporting, and output generation.

---

## Objective

- Build an end-to-end sales analysis pipeline in PySpark
- Validate data quality across multiple tables
- Handle null values, trimming, and invalid records
- Perform joins between fact and dimension-style datasets
- Generate business insights using both PySpark and Spark SQL
- Save analytical outputs for downstream use

---

## Tables Used

- `customers`
- `cars`
- `sales`
- `dealers`
- `sales_dealer`

---

## What I Implemented

### 1. Data Validation

- Checked schema and row counts for all tables
- Counted null values column by column
- Detected duplicate rows
- Identified invalid relationships using anti-joins
- Checked for negative price values and orphan sales records

### 2. Data Cleaning

- Filled missing customer names and cities with `"Unknown"`
- Replaced missing car prices with `0`
- Filled missing sales quantity with `1`
- Converted negative car prices to `0`
- Trimmed spaces in text columns such as customer, dealer, and city names
- Removed invalid sales records by joining only with valid customers and cars

### 3. Core Transformations

- Converted `sale_date` to date format
- Joined sales with customers and cars
- Calculated `revenue = price * quantity`
- Created a validation report summarizing cleaned data quality

### 4. Business Analysis

- Customer revenue analysis
  - Total revenue by customer
  - Ranked top customers by revenue

- Brand analysis
  - Total quantity sold by brand
  - Total revenue by brand

- City analysis
  - Total revenue by customer city

- Dealer analysis
  - Joined sales with dealer mapping
  - Calculated dealer-level revenue
  - Ranked top dealers
  - Computed dealer-city revenue

### 5. SQL Analytics

- Created a temporary view for sales data
- Used Spark SQL with window functions to find:
  - Top 3 customers per city using `ROW_NUMBER()`
  - Monthly trends such as:
    - unique customers
    - total quantity sold
    - total revenue
  - Repeat customers using `HAVING COUNT(DISTINCT sale_id) > 1`

### 6. Output Writing

- Wrote analytics outputs as Parquet files
- Saved reports such as:
  - customer revenue
  - brand sales
  - city revenue
  - dealer revenue
  - top customers per city
  - monthly trends
  - repeat customers

---

## Key Concepts Used

- `fillna()`
- `trim()`
- `when().otherwise()`
- `join()`
- `left_anti`
- `groupBy()`
- `agg()`
- `sum()`
- `count()`
- `countDistinct()`
- `to_date()`
- `orderBy()`
- `createOrReplaceTempView()`
- Spark SQL
- `ROW_NUMBER()`
- Parquet output writing

---

## Learning Outcome

By completing this task, I learned how to:

- Validate and clean multi-table datasets
- Build a reliable PySpark transformation pipeline
- Use joins for business analysis and referential checks
- Apply Spark SQL and window functions for advanced reporting
- Export reusable outputs for downstream analytics

---

## Files

- `Day 6 Car Sales Assignment (1).ipynb` - PySpark implementation
- `car_sales_pipeline_advanced.pdf` - assignment/problem statement
- `README.md` - day summary

# Day 1 - Insurance Pipeline and SQL Subqueries with CTEs

## Overview

Day 1 combines two major practice areas:

1. A PySpark insurance data engineering pipeline
2. SQL practice on subqueries, CTEs, ranking, and analytical reporting

This day helped strengthen both PySpark pipeline-building and SQL query design skills.

---

## Part 1: Insurance Data Engineering Pipeline

### Objective

- Build an end-to-end PySpark pipeline on insurance data
- Validate and clean multiple related datasets
- Generate business metrics and risk analysis
- Use Spark SQL and window functions for advanced reporting

### Tables Used

- `customers`
- `policies`
- `claims`
- `agents`
- `policy_agent`

### What I Implemented

#### Data Validation

- Checked schema and row counts for all tables
- Counted nulls in every dataset
- Detected negative premium and claim values
- Validated foreign keys using `left_anti` joins

#### Data Cleaning

- Filled missing values in customer and agent columns
- Replaced negative premium and claim amounts with `0`
- Trimmed text fields for consistent values
- Removed invalid records by keeping only matched policy, claim, and agent mappings

#### Analysis and Reporting

- Built a validation report with record counts and quality metrics
- Calculated premium by policy type
- Calculated claim totals by status
- Evaluated regional agent performance
- Built customer risk analysis using total premium vs total claims
- Generated monthly policy trends

#### Advanced SQL and Window Logic

- Created temporary views for SQL analysis
- Found top risky customers per city using CTEs and window functions
- Built city-level and monthly summaries
- Ranked agents by premium contribution
- Calculated customer risk score, ranking, percent rank, and bucket-based analysis
- Generated top agents per region and cumulative monthly premium metrics

### Key Concepts Used

- `fillna()`
- `trim()`
- `when().otherwise()`
- `left_anti`
- `groupBy()`
- `sum()`, `avg()`, `count()`, `countDistinct()`
- `date_format()`
- `createOrReplaceTempView()`
- CTEs
- `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`
- `percent_rank()`, `ntile()`
- window functions with `partitionBy()` and cumulative windows

---

## Part 2: SQL Subqueries and CTE Practice

### Objective

- Practice subqueries in different SQL clauses
- Learn CTE-based query structuring
- Build customer, product, and sales analysis queries
- Use ranking and lag logic in SQL analytics

### What I Practiced

- Created schemas such as `mydatabase` and `salesdb`
- Created and populated multiple tables:
  - customers
  - orders
  - orders_archive
  - employees
  - products

- Wrote SQL for:
  - subqueries in `WHERE`
  - subqueries in `FROM`
  - subqueries in `SELECT`
  - subqueries in `JOIN`
  - CTE-based customer sales summaries
  - ranking customers by total sales
  - comparing product prices with average price
  - salary-based comparisons
  - customer segmentation and ordering behavior analysis
  - monthly sales comparisons using `LAG()`

### Key SQL Concepts Used

- Subqueries
- CTEs
- `RANK()`
- `LAG()`
- aggregations with `SUM()`, `AVG()`, `COUNT()`
- joins between summarized datasets

---

## Learning Outcome

By the end of this day, I was able to:

- Build and validate a multi-table PySpark insurance pipeline
- Generate analytical insights from cleaned datasets
- Use CTEs and subqueries to simplify complex SQL logic
- Combine SQL analytics with ranking and trend analysis

---

## Files

- `Insurance Data Engineering Pipeline.ipynb` - PySpark insurance pipeline
- `insurance_pipeline_detailed_assignment.pdf` - assignment description
- `SQL Subqueries and CTE.ipynb` - SQL practice notebook
- `README.md` - day summary

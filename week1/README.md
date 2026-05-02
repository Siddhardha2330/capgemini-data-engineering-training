# Week 1 - SQL and PySpark Foundations

## Overview

Week 1 covers foundational SQL and PySpark practice, starting with advanced analytics on e-commerce data, then moving into core SQL concepts, real-world SQL problem solving, and introductory PySpark pipeline work.

---

## Day-wise Summary

### Day 1

#### Phase 5 - Advanced Analytics with Window Functions

- Worked on Brazilian e-commerce datasets
- Practiced window functions such as ranking and cumulative totals
- Built customer segmentation and lifetime value analysis
- Generated final reporting outputs from multi-table joins

#### Phase 6 - Advanced PySpark Joins and Analytics Pipeline

- Practiced joins including inner, left, and left anti
- Used window functions like `rank()`, `dense_rank()`, and `row_number()`
- Performed date-based analysis and month-over-month reporting
- Built a complete analytics pipeline from cleaned joined data

### Day 2 - SQL Group By and Joins

- Practiced aggregations using `SUM()`, `COUNT()`, `AVG()`, `MAX()`, and `MIN()`
- Used `GROUP BY`, `WHERE`, and `HAVING`
- Worked on inner joins, left joins, and self-joins
- Solved employee, department, project, and sales-based SQL problems

### Day 3 - CASE Statements and Window Functions

- Built SQL logic using `CASE WHEN`
- Practiced nested conditions and business-rule implementation
- Used `ROW_NUMBER()`, `RANK()`, and `DENSE_RANK()`
- Worked on ranking, categorization, and date/time SQL practice

### Day 4 - NULL Handling and Real-World SQL Analysis

- Practiced handling null values using `IS NULL`, `IS NOT NULL`, `COALESCE`, and `NULLIF`
- Solved student submission analysis using joins and normalized views
- Identified valid, invalid, duplicate, and missing submissions
- Applied data-cleaning and reconciliation logic on real-world style data

### Day 5 - PySpark Fundamentals

- Loaded CSV data into PySpark
- Practiced filtering, selection, casting, renaming, and dropping columns
- Filled missing values and removed duplicates
- Performed grouped sales aggregation on retail data

### Day 6 - Car Sales Data Engineering Pipeline

- Built a multi-table PySpark sales pipeline
- Performed validation, null handling, trimming, and referential checks
- Calculated customer, brand, city, and dealer-level revenue metrics
- Used Spark SQL and window functions for top-customer and monthly-trend analysis

---

## Key Concepts Covered in Week 1

### SQL

- `GROUP BY`
- `HAVING`
- `INNER JOIN`, `LEFT JOIN`, `LEFT ANTI JOIN`
- self joins
- `CASE WHEN`
- `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`
- null handling
- views and reusable query logic

### PySpark

- DataFrame operations
- filtering and projection
- `withColumn()`
- `fillna()`
- `dropDuplicates()`
- joins
- aggregations
- Spark SQL
- window functions
- Parquet output writing

---

## Learning Outcome

By the end of Week 1, I was able to:

- Solve intermediate SQL problems using joins, aggregations, and conditional logic
- Apply window functions in both SQL and PySpark
- Handle real-world data quality issues such as nulls, duplicates, and invalid relationships
- Build small PySpark pipelines for analytics and reporting

---

## Folder Structure

- `day1/` - phase-based advanced PySpark analytics
- `day2/` - SQL group by and joins
- `day3/` - CASE, window functions, and date/time SQL
- `day4/` - null handling and real-data SQL practice
- `day5/` - PySpark fundamentals
- `day6/` - car sales pipeline

# Week 2 - Data Engineering Pipelines and Advanced SQL

## Overview

Week 2 focuses on practical data engineering workflows using PySpark and Delta-style layered processing, along with advanced SQL topics such as subqueries, CTEs, ranking, and analytical reporting.

---

## Day-wise Summary

### Day 1 - Insurance Pipeline and SQL Subqueries with CTEs

- Built a multi-table insurance data pipeline in PySpark
- Validated nulls, negative values, and foreign key issues
- Generated customer risk, claims, premium, and agent performance reports
- Practiced SQL subqueries in `WHERE`, `FROM`, `SELECT`, and `JOIN`
- Used CTEs, ranking, and lag-based analysis for sales reporting

### Day 2 - End-to-End PySpark Pipeline

- Built a reusable pipeline on dirty orders data
- Cleaned null values and cast string fields to proper types
- Added derived columns such as bonus, category, and amount bucket
- Implemented full-load and incremental-load logic
- Parameterized notebook execution with widgets

### Day 3 - Flipkart Medallion Architecture Pipeline

- Built Bronze, Silver, and Gold layers for a sales dataset
- Preserved raw data in Bronze
- Cleaned, filtered, and deduplicated records in Silver
- Created business-ready Gold data for reporting
- Calculated product, category, city, and customer-level sales insights

---

## Key Concepts Covered in Week 2

- end-to-end pipeline design
- data validation and cleaning
- incremental and full loads
- notebook parameterization
- Delta and layered architecture
- Medallion Architecture
- Spark SQL
- subqueries
- CTEs
- ranking and window functions
- business aggregation reporting

---

## Learning Outcome

By the end of Week 2, I was able to:

- Build structured PySpark pipelines from raw to curated data
- Use layered architecture patterns such as Bronze, Silver, and Gold
- Apply advanced SQL logic using subqueries and CTEs
- Generate business-ready analytical outputs from cleaned datasets

---

## Folder Structure

- `day1/` - insurance pipeline and SQL subqueries/CTE practice
- `day2/` - end-to-end pipeline implementation
- `day3/` - Flipkart Medallion Architecture pipeline

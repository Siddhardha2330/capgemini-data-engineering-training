# Phase 6 – Advanced PySpark: Joins, Window Functions & Analytics Pipeline

## 🔹 Objective

- Master advanced PySpark operations including joins and window functions
- Learn data quality validation using anti-joins
- Build complex analytical queries with ranking and aggregations
- Create end-to-end data pipeline with multiple transformations

---

## 🔹 Problem Summary

- Given e-commerce datasets with 9 tables (customers, orders, products, etc.)
- Required to:
  - Perform different types of joins
  - Apply window functions for ranking and running totals
  - Analyze date-based patterns
  - Build complete analytical pipeline with data quality checks

---

## 🔹 Approach

- Practice Set A: Join Drills
  - Applied inner join, left join, and left anti join
  - Validated referential integrity using anti-joins
  - Identified orphaned records in datasets

- Practice Set B: Window Functions
  - Used rank(), dense_rank(), row_number()
  - Implemented partitioned windows for city-level analysis
  - Calculated running totals using rowsBetween
  - Applied LAG and LEAD for sequential comparisons

- Practice Set C: Date Analysis
  - Extracted date parts using month(), year()
  - Calculated delivery time metrics with datediff()
  - Performed month-over-month growth analysis
  - Created time-based aggregations

- Practice Set D: Complete Pipeline
  - Cleaned data by filtering nulls and invalid values
  - Joined multiple tables (orders, customers, products, sellers)
  - Applied customer segmentation logic
  - Generated business metrics and reports

---

## 🔹 Tasks Implemented

### Practice Set A: Join Drills
- Inner Join → orders with customers
- Left Join → orders with customers (all orders)
- Left Anti Join → orphaned orders detection
- Referential Integrity Check → validate order_items relationships

### Practice Set B: Window Functions
- Customer Total Spending → aggregated by customer
- Global Ranking → rank all customers by spend
- City-Level Ranking → top 3 customers per city
- Running Totals → cumulative spend by state
- LAG/LEAD → compare with previous/next customer

### Practice Set C: Date Analysis
- Extract Date Parts → year, month from timestamps
- Monthly Sales → aggregated by year_month
- Delivery Time Analysis → actual vs estimated days
- Month-over-Month Growth → calculate growth percentage

### Practice Set D: Complete Pipeline
- Data Cleaning → remove nulls and invalid prices
- Master Dataset → join 5 tables
- Customer Metrics → total_spend, order_count, segment
- Category Performance → revenue by product category
- Monthly Trends → cumulative revenue and growth

---

## 🔹 Key Transformations Used

- Data Ingestion → `spark.read.csv()`
- Joins → `join()` with inner, left, left_anti
- Window Functions → `Window.partitionBy()`, `Window.orderBy()`
- Ranking → `rank()`, `dense_rank()`, `row_number()`
- Aggregation → `groupBy()`, `sum()`, `count()`, `countDistinct()`
- Sequential Analysis → `lag()`, `lead()`
- Date Operations → `to_date()`, `month()`, `year()`, `datediff()`
- Conditional Logic → `when()`, `otherwise()`
- Running Totals → `rowsBetween(Window.unboundedPreceding, Window.currentRow)`

---

## 🔹 Output

- Output 1: Join Results
  - Inner join shows matching records between orders and customers
  - Left anti join identifies data quality issues
    <img width="1570" height="816" alt="image" src="https://github.com/user-attachments/assets/22b1d0c3-03dc-4884-8c40-552663b4572f" />


- Output 2: Customer Rankings
  - Global ranking shows top spenders across all customers
  - City-level ranking shows top 3 customers per city
    <img width="1698" height="838" alt="image" src="https://github.com/user-attachments/assets/f12145c9-c514-4799-881b-e12a1fd74c5d" />


- Output 3: Running Totals
  - Cumulative spend calculated by state using window functions
  - Shows progressive accumulation of customer spend
    <img width="1704" height="831" alt="image" src="https://github.com/user-attachments/assets/fa66f080-ac1b-440d-8872-6f7939ff826e" />



---

## 🔹 Data Engineering Considerations

- Validated referential integrity before performing joins
- Cleaned data (nulls, duplicates, invalid values) before transformations
- Used appropriate join types based on business requirements
- Applied window functions instead of self-joins for better performance
- Partitioned windows for city/state level calculations
- Used rowsBetween for efficient running total calculations

---

## 🔹 Challenges Faced

- Understanding different join types and when to use each
- Implementing window functions with correct partitioning
- Calculating running totals without performance issues
- Managing multiple table joins efficiently
- Applying LAG/LEAD for sequential comparisons

---

## 🔹 Learnings

- Mastered different join types (inner, left, left anti)
- Learned to use window functions for advanced analytics
- Understood how to validate data quality using joins
- Learned efficient techniques for running totals and rankings
- Improved understanding of date operations in PySpark
- Built complete analytical pipeline from raw data to insights

---

## 🔹 Project Structure

Phase6_notebook.ipynb → PySpark implementation
phase6_problem_statement.pdf → problem statement
outputs/ → result screenshots
README.md → project explanation

---

## 🔹 Data Schema

### Main Tables
- **customers** (99,441 rows) → customer_id, customer_city, customer_state
- **orders** (99,441 rows) → order_id, customer_id, order_purchase_timestamp
- **order_items** (112,650 rows) → order_id, product_id, seller_id, price
- **products** (32,951 rows) → product_id, product_category_name
- **sellers** (3,095 rows) → seller_id, seller_city, seller_state

### Key Relationships
- customers → orders (customer_id)
- orders → order_items (order_id)
- order_items → products (product_id)
- order_items → sellers (seller_id)

---


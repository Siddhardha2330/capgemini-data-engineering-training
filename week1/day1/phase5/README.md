# Phase 5 – Advanced Analytics with Window Functions

## 🔹 Objective

- Master advanced PySpark window functions for analytical tasks
- Implement ranking, cumulative calculations, and customer segmentation
- Build complex multi-table joins and aggregations
- Generate business intelligence reports from e-commerce data

---

## 🔹 Problem Summary

- Given Brazilian E-Commerce datasets (9 tables with 100K+ records)
- Required to:
  - Rank customers within cities using window functions
  - Calculate running totals of sales over time
  - Identify top-performing products per category
  - Compute customer lifetime value
  - Segment customers based on spending patterns
  - Create comprehensive reporting table

---

## 🔹 Approach

- Data Loading:
  - Loaded 9 CSV datasets from Unity Catalog volumes
  - Tables: customers, orders, order_items, products, sellers, payments, reviews, geolocation, category_translation

- Data Transformation:
  - Applied window functions for ranking and cumulative calculations
  - Used partitioned windows for city-wise and category-wise analysis
  - Implemented conditional logic for customer segmentation
  - Performed multi-table joins to combine related data
  - Applied aggregations for business metrics

- Output Generation:
  - Created analytical views for each task
  - Generated final reporting table combining all metrics
  - Validated data quality and referential integrity

---

## 🔹 Tasks Implemented

- **Task 1: Top 3 Customers per City**
  - Output: city, customer_id, total_spend, rank
  - Used `RANK()` window function partitioned by city
  - Result: 9,401 rows across 4,110 cities

- **Task 2: Running Total of Sales**
  - Output: date, daily_sales, running_total
  - Used cumulative sum with `rowsBetween(unboundedPreceding, currentRow)`
  - Result: 616 days of sales data (Sept 2016 - Sept 2018)

- **Task 3: Top Products per Category**
  - Output: category, product_id, total_sales, rank
  - Used `DENSE_RANK()` window function partitioned by category
  - Result: 221 rows (top 3 products × 73 categories)

- **Task 4: Customer Lifetime Value**
  - Output: customer_id, total_spend
  - Aggregated total spend across all customer orders
  - Result: 98,666 customers analyzed

- **Task 5: Customer Segmentation**
  - Output: customer_id, total_spend, segment
  - Segmentation logic:
    - Gold → total_spend > $10,000
    - Silver → $5,000 - $10,000
    - Bronze → < $5,000
  - Distribution: 1 Gold, 5 Silver, 98,660 Bronze

- **Task 6: Final Reporting Table**
  - Output: customer_id, city, total_spend, segment, total_orders
  - Combined all customer metrics into single comprehensive view
  - Result: 98,666 customer records with complete analytics

---

## 🔹 Key Transformations Used

- **Window Functions**
  - `Window.partitionBy()` → Partition data by city/category
  - `rank()` → Rank customers within partitions
  - `dense_rank()` → Rank products without gaps
  - `rowsBetween()` → Define window frame for running totals
  - `sum().over()` → Calculate cumulative aggregations

- **Joins**
  - Multi-table joins: order_items → orders → customers
  - Product joins: order_items → products
  - Proper join sequence for optimal performance

- **Aggregations**
  - `groupBy()` + `agg()` → Calculate metrics per group
  - `sum()` → Total spend and revenue
  - `countDistinct()` → Count unique orders

- **Conditional Logic**
  - `when().otherwise()` → Customer segmentation rules
  - Multiple conditions for tier classification

- **Data Type Operations**
  - `to_date()` → Extract dates from timestamps
  - `col()` → Column references and operations

---

## 🔹 Output

- **Output 1: Top 3 Customers per City**
  - Shows highest-spending customers in each city with rankings
  - Validates window function partitioning and ranking logic
    <img width="1336" height="752" alt="image" src="https://github.com/user-attachments/assets/fbe00028-864e-4804-bc90-697a5fe508a6" />


- **Output 2: Running Total of Sales**
  - Displays daily sales with cumulative running total
  - Confirms correct ordering and window frame calculation
    <img width="1360" height="783" alt="image" src="https://github.com/user-attachments/assets/348a0bce-1366-41d3-b030-eb284d1e9e53" />


- **Output 3: Top Products per Category**
  - Lists top 3 best-selling products per category using DENSE_RANK
  - Verifies category-wise ranking without rank gaps
    <img width="1358" height="768" alt="image" src="https://github.com/user-attachments/assets/a5fc6b05-7b43-454e-928b-3ca5faff5504" />

    

---

## 🔹 Data Engineering Considerations

- **Performance Optimization**
  - Used appropriate window partitioning to minimize data shuffling
  - Filtered data early to reduce processing overhead
  - Proper join order (smaller tables first)

- **Data Quality**
  - Validated referential integrity between tables
  - 0 orphaned records found (perfect data quality)
  - All foreign keys properly validated

- **Scalability**
  - Window functions work efficiently on large datasets
  - Partitioned operations distribute computation evenly
  - Suitable for production workloads

- **Code Reusability**
  - Modular task structure for easy maintenance
  - Reusable DataFrames across multiple tasks
  - Clear naming conventions

---

## 🔹 Challenges Faced

- Understanding window function syntax and frame specifications
- Choosing between `RANK()`, `DENSE_RANK()`, and `ROW_NUMBER()`
- Handling cumulative calculations with proper window frames
- Managing multi-table joins with large datasets
- Implementing correct segmentation logic with multiple conditions

---

## 🔹 Learnings

- **Window Functions Mastery**
  - Learned how to partition data for analytical operations
  - Understood difference between ranking functions
  - Implemented running totals and cumulative calculations

- **Data Pipeline Design**
  - Designed efficient multi-step transformations
  - Validated data quality at each stage
  - Created reusable analytical patterns

- **Business Intelligence**
  - Translated business requirements into technical implementations
  - Generated actionable insights from raw data
  - Built executive-ready reporting tables

---

## 🔹 Project Structure
- sql_queries.sql → SQL queries
- solution.py → PySpark implementation
- phase4_problem_statement.pdf → problem statement
- outputs/ → result screenshots
- README.md → project explanation 

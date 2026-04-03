# Phase 4 – Mini Project: Business Pipeline & Analytics

## 🔹 Objective

- Build an end-to-end ETL pipeline using PySpark  
- Understand how data flows from raw input to final business insights  
- Apply transformations to generate meaningful outputs  

---

## 🔹 Problem Summary

- Given customer and sales datasets  
- Required to:
  - Clean and preprocess data  
  - Perform transformations  
  - Generate business insights  
  - Build a structured ETL pipeline  

---

## 🔹 Approach

- Extract:
  - Loaded datasets using `spark.read()`  

- Transform:
  - Cleaned data by removing null keys and duplicates  
  - Casted columns to correct data types  
  - Filtered invalid values (negative amounts)  
  - Created derived column `customer_name`  
  - Joined datasets using `customer_id`  
  - Applied aggregations and business logic  

- Load:
  - Saved final output using `.write.mode('overwrite').csv()`  

---

## 🔹 Tasks Implemented

- Task 1: Daily Sales → date, total_sales  
- Task 2: City-wise Revenue → city, total_revenue  
- Task 3: Top 5 Customers → customer_name, total_spend  
- Task 4: Repeat Customers (>1 order) → customer_id, order_count  
- Task 5: Customer Segmentation  
  - Gold → total_spend > 10000  
  - Silver → 5000–10000  
  - Bronze → <5000  
- Task 6: Final Reporting Table  
  - customer_name, city, total_spend, order_count, segment  
- Task 7: Save Output → `/samples/output/report`  

---

## 🔹 Key Transformations Used

- Data Ingestion → `spark.read()`  
- Data Cleaning → `dropna()`, `dropDuplicates()`  
- Filtering → `filter()`  
- Type Casting → `cast()`  
- Joins → `join()`  
- Aggregation → `groupBy()`, `sum()`, `count()`  
- Column Operations → `withColumn()`  
- Conditional Logic → `when()`  

---

## 🔹 Output

- Output 1: Daily Sales
  - This output shows total sales aggregated for each date  
  - It verifies correct grouping and aggregation logic  

    <img width="1370" height="799" alt="image" src="https://github.com/user-attachments/assets/4d450bbc-5239-459f-bc67-03c31d7cc950" />


- Output 2: City-wise Revenue
  - This output represents total revenue generated for each city  
  - It confirms that join between datasets is working correctly  

    <img width="1384" height="821" alt="image" src="https://github.com/user-attachments/assets/f6eacf6c-8869-47dd-9646-a5ed11aeaa7b" />


- Output 3: Top 5 Customers
  - This output shows customers with highest total spend  
  - It verifies sorting and limiting logic  

    <img width="1475" height="820" alt="image" src="https://github.com/user-attachments/assets/92bef183-726d-4896-a25a-8de32215cff7" />

---

## 🔹 Data Engineering Considerations

- Cleaned data before joins to avoid incorrect results  
- Ensured correct data types for accurate aggregation  
- Removed duplicates to prevent double counting  
- Used structured pipeline for better readability  

---

## 🔹 Challenges Faced

- Handling missing and invalid data  
- Creating correct segmentation logic  
- Managing joins and aggregations properly  

---

## 🔹 Learnings

- Understood complete ETL workflow (Extract → Transform → Load)  
- Learned how to build reusable data pipelines  
- Improved understanding of PySpark transformations  
- Learned how to convert business requirements into code  

---

## 🔹 Project Structure

- sql_queries.sql → SQL queries
- solution.py → PySpark implementation
- phase4_problem_statement.pdf → problem statement
- outputs/ → result screenshots
- README.md → project explanation 

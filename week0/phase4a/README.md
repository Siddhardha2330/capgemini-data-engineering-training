# Phase 4A – Bucketing & Segmentation using PySpark

## 🔹 Objective

- Understand how continuous data is converted into categories  
- Learn multiple segmentation techniques in PySpark  
- Apply different methods to compare results  

---

## 🔹 Problem Summary

- Given transactional data with continuous values (total_spend)  
- Required to:
  - Convert continuous values into segments (Gold, Silver, Bronze)  
  - Apply multiple segmentation techniques  
  - Compare results across different methods  

---

## 🔹 Approach

- Loaded customer and sales datasets using `spark.read()`  
- Joined datasets using `customer_id`  
- Calculated total spend per customer  
- Applied different segmentation techniques:
  - Conditional logic  
  - Quantile-based segmentation  
  - Bucketizer (MLlib)  
  - Window-based ranking  

---

## 🔹 Methods Used

- Conditional Logic → `when()`  
- Quantile Segmentation → `approxQuantile()`  
- Bucketizer → `pyspark.ml.feature.Bucketizer`  
- Window Functions → `percent_rank()`  

---

## 🔹 Tasks Implemented

- Task 1: Gold/Silver/Bronze segmentation using conditional logic  
- Task 2: Group customers by segment and count  
- Task 3: Quantile-based segmentation  
- Task 4: Compare segmentation methods  
- Task 5: Apply window-based ranking  

---

## 🔹 Output

- Output 1: Conditional Segmentation
  - Customers are categorized using fixed business rules  
  - Helps in simple business decision-making  

   <img width="1427" height="872" alt="image" src="https://github.com/user-attachments/assets/a35e5652-9da0-4adc-a25f-4dfdc1daddad" />

- Output 2: Segment-wise Customer Count
  - Shows number of customers in each segment  
  - Helps understand distribution  

    <img width="1520" height="656" alt="image" src="https://github.com/user-attachments/assets/e079628c-1ffe-4025-8c55-37aa71eabfbc" />


- Output 3: Quantile Segmentation
  - Divides customers based on data distribution  
  - Ensures balanced grouping  

    <img width="1657" height="863" alt="image" src="https://github.com/user-attachments/assets/c1863c24-b1f9-44cb-9710-da8cdfb8dda0" />



---

## 🔹 Data Engineering Considerations

- Different segmentation methods serve different use cases  
- Fixed thresholds may not work for all datasets  
- Data distribution plays a key role in segmentation  

---

## 🔹 Challenges Faced

- Understanding differences between segmentation methods  
- Choosing correct thresholds for segmentation  
- Interpreting quantile results  

---

## 🔹 Learnings

- Learned multiple ways to perform bucketing in PySpark  
- Understood difference between business rules and data-driven segmentation  
- Gained knowledge of MLlib and window functions  
- Learned how segmentation impacts business insights  

---

## 🔹 Reflection

- Continuous values are converted to categories for easier analysis  
- Business segmentation uses fixed rules, while technical bucketing is data-driven  
- Fixed thresholds can fail when data distribution changes  
- Quantile segmentation adapts automatically to data  
- In real-world projects, a combination of methods is often used  

---

## 🔹 Project Structure

- sql_queries.sql → SQL queries
- solution.py → PySpark implementation
- phase4a_data_quality_challenge.pdf → problem statement
- outputs/ → result screenshots
- README.md → project explanation 

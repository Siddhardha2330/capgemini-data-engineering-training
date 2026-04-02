# Phase 1 – SQL to PySpark

## 🔹 Objective

- The goal of this phase was to understand how basic SQL operations can be performed using PySpark
- Focus was on simple queries like filtering, selecting, and aggregation

## 🔹 Problem Summary

- A small dataset of customers was created manually
- The task was to:
  - Perform basic data operations using PySpark
  - Understand how SQL-like queries work in DataFrames
  - Generate simple outputs

## 🔹 Approach

- Created a Spark session using SparkSession
- Built a DataFrame using sample customer data
- Applied different operations similar to SQL:
  - Displayed all records
  - Filtered data based on conditions
  - Selected specific columns
  - Performed aggregation using groupBy()

## 🔹 Key Operations Used

- DataFrame Creation → createDataFrame()
- Viewing Data → show()
- Filtering → filter()
- Column Selection → select()
- Aggregation → groupBy(), count()
- Column Reference → col()

## 🔹 Output / Results

- Output 1: All Customers
  - Displays all records from the dataset
    
    <img width="1645" height="877" alt="image" src="https://github.com/user-attachments/assets/2c74e776-673d-46cb-af40-c3c83b67497d" />


- Output 2: Customers from Chennai
  - Filters and shows only customers belonging to Chennai
    
    <img width="1664" height="871" alt="image" src="https://github.com/user-attachments/assets/4dc2b77f-0af8-465e-bcaf-0c485e487c9b" />


- Output 3: Customers with Age > 25
  - Shows customers whose age is greater than 25
    
    <img width="1669" height="873" alt="image" src="https://github.com/user-attachments/assets/cfb8a5af-fffa-47db-970b-30c584fc875f" />



## 🔹 Learnings

- Understood how PySpark DataFrames work
- Learned how to convert basic SQL queries into PySpark
- Gained clarity on filtering and selecting data
- Learned how aggregation works using groupBy()

## 🔹 Challenges

- Understanding syntax differences between SQL and PySpark
- Getting familiar with DataFrame operations

## 🔹 Files in this Folder

- solution.py → PySpark implementation
- README.md → Explanation of the phase

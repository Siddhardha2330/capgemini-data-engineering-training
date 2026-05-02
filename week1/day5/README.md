# Day 5 - PySpark Fundamentals

## Overview

This day focuses on basic PySpark DataFrame operations using the `Big Sales.csv` dataset. The work covers data loading, filtering, column transformations, null handling, grouping, and common cleanup operations.

---

## Objective

- Learn how to read CSV data using PySpark
- Practice DataFrame selection and filtering
- Handle null values and missing data
- Apply transformations on columns
- Perform aggregations using `groupBy()`
- Clean and standardize a dataset using PySpark

---

## Dataset Used

- `Big Sales.csv`
- Contains retail sales-related columns such as:
  - `Item_Identifier`
  - `Item_Type`
  - `Item_Weight`
  - `Item_MRP`
  - `Outlet_Size`
  - `Outlet_Type`
  - `Item_Outlet_Sales`

---

## What I Practiced

### 1. Data Loading

- Loaded CSV data into a PySpark DataFrame using `spark.read.csv()`
- Verified the dataset before applying transformations

### 2. Column Selection and Filtering

- Selected important columns such as item identifier, type, and MRP
- Filtered rows where `Item_MRP` is greater than 200
- Applied range filters using `between(100, 300)`
- Filtered records using pattern matching such as `like("%Dairy%")`

### 3. Column Transformations

- Created or modified columns using `withColumn()`
- Renamed `Item_Outlet_Sales` to `Sales_Amount`
- Cast `Item_MRP` to integer type
- Standardized text values for better consistency

### 4. Null Handling

- Calculated average `Item_Weight`
- Replaced null `Item_Weight` values using the average
- Filled missing `Outlet_Size` values with `"Unknown"`
- Removed rows with null `Item_Outlet_Sales` where needed

### 5. Aggregation and Analysis

- Grouped data by `Outlet_Type`
- Calculated total sales using:
  - `groupBy()`
  - `sum()`

### 6. Data Cleanup Operations

- Dropped unnecessary columns such as `Outlet_Establishment_Year`
- Removed duplicate records using `dropDuplicates()`

---

## Key PySpark Concepts Used

- `spark.read.csv()`
- `select()`
- `filter()`
- `col()`
- `withColumn()`
- `withColumnRenamed()`
- `cast()`
- `fillna()`
- `dropna()`
- `drop()`
- `dropDuplicates()`
- `groupBy()`
- `agg()`
- `sum()`
- `avg()`

---

## Learning Outcome

By the end of this practice, I was able to:

- Read and inspect CSV data in PySpark
- Filter and transform data using DataFrame operations
- Handle nulls and duplicates
- Perform aggregations on grouped data
- Apply common data-cleaning steps on a real dataset

---

## Files

- `Day 5 Pyspark fundamentals.ipynb` - notebook with PySpark practice
- `Big Sales.csv` - source dataset
- `README.md` - day summary

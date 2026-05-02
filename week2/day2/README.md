# Day 2 - End-to-End PySpark Pipeline

## Overview

This day focuses on building a complete PySpark pipeline from dirty source data to cleaned target output. The notebook covers null handling, type casting, derived columns, UDF usage, full load, incremental load, and notebook parameterization.

---

## Objective

- Build an end-to-end PySpark processing pipeline
- Clean dirty input data with nulls and string-based numeric fields
- Derive new analytical columns
- Apply business logic using a UDF
- Implement both full-load and incremental-load patterns
- Parameterize execution using widgets

---

## Dataset Characteristics

The sample orders dataset includes:

- null values
- `amount` stored as string
- date column requiring casting
- records for both full and incremental processing

---

## What I Implemented

### 1. Data Creation and Inspection

- Created a sample dirty dataset in PySpark
- Loaded it into a DataFrame for processing

### 2. Null Handling

- Replaced null `amount` values with a default string value of `"1000"`

### 3. Data Type Conversion

- Cast `amount` to integer
- Converted `updated_date` to proper date format

### 4. Derived Columns

- Created `bonus` as 10 percent of amount
- Created a category column based on amount thresholds

### 5. UDF Application

- Built a UDF to create `amount_bucket`
- Categorized records into business-friendly amount ranges

### 6. Full Load

- Wrote the transformed DataFrame to a Parquet target path
- Used `coalesce(1)` to save output as a single file
- Read the saved output back for validation

### 7. Incremental Load

- Created a separate incremental dataset
- Applied the same cleaning and transformation logic
- Prepared the dataset for incremental processing

### 8. Notebook Parameterization

- Added `dbutils.widgets.dropdown()` for `load_type`
- Built reusable transformation logic driven by runtime parameters
- Wrote output conditionally based on selected load type

### 9. Summary Reporting

- Read final output from the target path
- Generated summary metrics including:
  - total records
  - category counts
  - amount bucket counts
  - total amount aggregation

---

## Key Concepts Used

- `fillna()`
- `cast()`
- `to_date()`
- `withColumn()`
- `when().otherwise()`
- UDFs
- Parquet write and read
- full load
- incremental load
- Databricks widgets
- reusable transformation functions

---

## Learning Outcome

By the end of this day, I was able to:

- Clean and transform a raw dataset in PySpark
- Build reusable transformation logic
- Implement both full and incremental data loads
- Parameterize a notebook for flexible execution
- Generate summary outputs for validation

---

## Files

- `End_To_End_Pipeline_6 (1).ipynb` - notebook implementation
- `README.md` - day summary

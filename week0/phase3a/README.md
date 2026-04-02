# Phase 3A – Data Quality & Cleaning using PySpark

## 🔹 Objective

- This phase focuses on handling messy data and cleaning it before using it for any analysis or processing.
- The goal is to understand how data issues affect results and how to fix them properly.

## 🔹 Problem Summary

- A dataset was given with several problems such as missing values, duplicate rows, and invalid entries.
- The task was to:

  - Identify data issues
  - Clean the dataset step by step
  - Validate the cleaned data
  - Generate basic insights

## 🔹 Approach

- Loaded the dataset into a PySpark DataFrame
- Checked for issues like null values, duplicates, and incorrect data
- Applied cleaning steps:
  - Removed rows with missing key values
  - Filled missing fields with default values
  - Removed duplicate records
  - Filtered out invalid values (example: negative age)
  - Verified results by comparing row counts
  - Performed aggregation (customers per city)

## 🔹 Key Operations Used

- filter() → remove invalid or unwanted rows
- fillna() → handle missing values
- dropDuplicates() → remove duplicate records
- groupBy() → group data
- count() → aggregation

## 🔹 Output / Results

- Output 1: Initial Data Summary
  - This output shows the total number of rows in the dataset before cleaning
  - It also displays null values count for each column
  - It helps in understanding data quality issues like missing values

    <img width="1258" height="871" alt="image" src="https://github.com/user-attachments/assets/6824807b-1db8-410f-8343-72a014dc1558" />


- Output 2: Duplicate and Invalid Records
  - This output identifies duplicate rows present in the dataset
  - It also highlights invalid records such as negative age values
  - It ensures that data issues are detected before cleaning

    <img width="1429" height="861" alt="image" src="https://github.com/user-attachments/assets/1da69fe5-69e2-4416-a173-0fdf18dc3c38" />


- Output 3: Cleaned Dataset
  - This output shows the dataset after applying cleaning steps
  - Null values are handled using fillna()
  - Duplicate records are removed using dropDuplicates()
  - Invalid records (like negative age) are filtered out
  - It confirms that the dataset is ready for further processing

    <img width="1465" height="874" alt="image" src="https://github.com/user-attachments/assets/61e82828-100c-4759-95ae-60f7578e5ca0" />


## 🔹 Data Engineering Considerations

- Cleaned data before any processing to avoid wrong insights
- Ensured duplicates do not affect aggregation results
- Verified data before and after cleaning

## 🔹 Challenges Faced

- Identifying different types of data issues
- Deciding how to handle missing values correctly

## 🔹 Learnings
- Real-world datasets are often messy
- Cleaning is an important step before analysis
- Small data issues can lead to incorrect results
- Validation is necessary in every pipeline

## 🔹 Project Structure
- solution.py → PySpark implementation
- phase3a_data_quality_challenge (1).pdf → problem statement
- outputs/ → result screenshots
- README.md → project explanation

  Phase 3A – Data Quality & Cleaning using PySpark

🔹 Objective

- This phase focuses on handling messy data and cleaning it before using it for any analysis or processing.
- The goal is to understand how data issues affect results and how to fix them properly.

🔹 Problem Summary

- A dataset was given with several problems such as missing values, duplicate rows, and invalid entries.
- The task was to:

  - Identify data issues
  - Clean the dataset step by step
  - Validate the cleaned data
  - Generate basic insights

🔹 Approach

- Loaded the dataset into a PySpark DataFrame
- Checked for issues like null values, duplicates, and incorrect data
- Applied cleaning steps:
  - Removed rows with missing key values
  - Filled missing fields with default values
  - Removed duplicate records
  - Filtered out invalid values (example: negative age)
  - Verified results by comparing row counts
  - Performed aggregation (customers per city)

🔹 Key Operations Used

- filter() → remove invalid or unwanted rows
- fillna() → handle missing values
- dropDuplicates() → remove duplicate records
- groupBy() → group data
- count() → aggregation

🔹 Output / Results

- Clean dataset with valid records only
- No duplicate entries
- Aggregated output showing number of customers per city

👉 Output screenshots are available in the outputs/ folder

🔹 Data Engineering Considerations

- Cleaned data before any processing to avoid wrong insights
- Ensured duplicates do not affect aggregation results
- Verified data before and after cleaning

🔹 Challenges Faced

- Identifying different types of data issues
- Deciding how to handle missing values correctly

🔹 Learnings
- Real-world datasets are often messy
- Cleaning is an important step before analysis
- Small data issues can lead to incorrect results
- Validation is necessary in every pipeline

🔹 Project Structure
- solution.py → PySpark implementation
- phase3a_data_quality_challenge (1).pdf → problem statement
- outputs/ → result screenshots
- README.md → project explanation

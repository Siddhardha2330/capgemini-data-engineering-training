Phase 3 — ETL Pipeline using PySpark

🔹 Objective

- Build an end-to-end ETL pipeline using PySpark
- Understand data flow from ingestion to final output
- Apply transformations to generate business insights

🔹 Problem Summary

- Given sample datasets in different formats (CSV, JSON, Parquet)
- Required to:
   
   - Read data from multiple sources
   - Clean and preprocess data
   - Perform transformations and aggregations
   - Build a reusable ETL pipeline

🔹 Approach

- Extract:
   
   - Loaded datasets using spark.read() from /samples/

- Transform:
   
   - Cleaned data using dropna() and fillna()
   - Filtered invalid records using filter()
   - Converted data types using cast()
   - Joined datasets using customer_id
   - Applied aggregations using groupBy()
   - Used window functions (rank()) for advanced analysis

- Load:
   
   - Displayed final outputs using .show()
   - Converted SQL queries into PySpark logic
   - Combined all steps into a reusable pipeline

🔹 Key Transformations

- Data Ingestion → spark.read()
- Data Cleaning → dropna(), fillna()
- Filtering → filter()
- Type Casting → cast()
- Joins → join()
- Aggregation → groupBy(), sum(), count(), avg()
- Column Operations → withColumn()
- Window Functions → rank()

🔹 Output

- Daily sales summary
- City-wise revenue
- Repeat customers (>2 orders)
- Highest spending customer in each city
- Final reporting table with customer, city, total spend, and order count

🔹 Learnings

- Understood ETL workflow (Extract → Transform → Load)
- Learned how to build reusable data pipelines
- Gained experience working with multiple file formats
- Learned how to convert SQL logic into PySpark
- Understood use of window functions for ranking

🔹 Challenges

- Understanding full ETL pipeline structure
- Debugging issues in joins and aggregations

🔹 Files in this Folder

- pyspark_code.py → ETL pipeline implementation
- sql_queries.sql → SQL queries
- phase3_problem_statement.pdf → Problem description
- outputs/ → Output screenshots

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
- SQL Integration → spark.sql()

🔹 Output

   - Output 1: Daily Sales
       - This output shows total sales aggregated for each date
       - It verifies that grouping and aggregation using groupBy() and sum() are working correctly
       - It also confirms that null values are handled before aggregation

         <img width="1502" height="877" alt="image" src="https://github.com/user-attachments/assets/0e8e8a08-c04f-4a56-9779-f18e863328ed" />

   - Output 2: City-wise Revenue
       - This output represents total revenue generated for each city
       - It confirms that the join between customers and sales datasets is performed correctly
       - It validates aggregation of revenue at city level

         <img width="1349" height="869" alt="image" src="https://github.com/user-attachments/assets/18be9972-72c7-41ef-9fbc-aab96aebf79b" />

   - Output 3: Repeat Customers
       - This output shows customers who have placed more than one order
       - It verifies that aggregation using count() and filtering conditions are applied correctly
       - It helps identify returning customers

         <img width="1438" height="755" alt="image" src="https://github.com/user-attachments/assets/825414df-67d6-4960-ad5e-3277e3654b82" />


🔹 Learnings

- Understood ETL workflow (Extract → Transform → Load)
- Learned how to build reusable data pipelines
- Gained experience working with multiple file formats
- Learned how to convert SQL logic into PySpark
- Understood use of window functions for ranking

🔹 Challenges Faced
- Understanding full ETL pipeline structure
- Handling null values and data type conversions
- Debugging issues in joins and aggregations

🔹 Files in this Folder

- pyspark_code.py → ETL pipeline implementation
- sql_queries.sql → SQL queries
- phase3_problem_statement.pdf → Problem description
- outputs/ → Output screenshots

Phase 2 — SQL & PySpark

🔹 Objective
-The main goal of this phase was to understand how data can be transformed using both SQL and PySpark. This includes combining datasets, applying filters, and generating useful insights from raw data.

🔹 Problem Summary
-We were given structured datasets containing customer and transaction-related information. The task was to:
  - Combine data from multiple sources
  - Apply filtering conditions
  - Perform aggregations
  - Generate meaningful outputs for analysis

🔹 Approach
- Loaded datasets into PySpark DataFrames using spark.read.csv()
- Inspected data using show() and printSchema()
- Cleaned data by handling null values and correcting data types
- Joined datasets using common keys like customer_id
- Applied transformations such as filtering, aggregation, and sorting
- Also implemented the same logic using SQL queries for comparison
🔹 Key Transformations
- DataFrame Creation → createDataFrame(), spark.read.csv()
- Viewing Data → show()
- View Structure → printSchema()
- Selection & Filtering → select(), filter(), where()
- Aggregation → groupBy(), sum(), count(), avg()
- Sorting → orderBy()
- Column Operations → withColumn(), withColumnRenamed()
- Removing Columns → drop()
- Unique Values → distinct()
- Column Reference → col()
- Conditional Logic → when()
- Formatting → round()
- Joins → join()
- SQL Integration → createOrReplaceTempView(), spark.sql()

🔹 Output
- Generated aggregated datasets such as customer spending and order summaries
- Produced filtered datasets based on business conditions
- Compared SQL and PySpark outputs to ensure correctness

🔹 Learnings
- Understood how PySpark DataFrames work compared to SQL tables
- Learned how joins and aggregations are used in real-world scenarios
- Gained clarity on when to use SQL vs PySpark
- Improved understanding of data cleaning before transformations

🔹 Challenges / Further Improvements
- Deciding when to use join() versus direct aggregation
- Handling data types properly using cast()
- Dealing with precision issues between float and double
- Debugging unexpected outputs after joins or aggregations
🔹 Files in this Folder
- solution.py → PySpark implementation
- queries.sql → SQL solutions
- phase2_problem_statement.pdf → Problem description
- outputs/ → Output screenshots

Phase 2 — SQL & PySpark

🔹 Objective

-The main goal of this phase was to understand how data can be transformed using both SQL and PySpark. This includes combining datasets, applying filters, and generating useful insights from raw data.

🔹 Problem Summary

 We were given structured datasets containing customer and transaction-related information. The task was to:
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
- Column Reference → col()
- Formatting → round()
- Joins → join()

🔹 Output

  - Output 1: Customer-wise Total Spend
      - This output shows the total amount spent by each customer
      - It verifies that aggregations using groupBy() and sum() are working correctly
        
        <img width="1339" height="872" alt="image" src="https://github.com/user-attachments/assets/6bff33f8-aa46-4412-9cfb-5abb02c5505b" />
        
  - Output 2: Filtered High-Value Customers
      - This output displays the top three customers who spent more money
      - It confirms that filtering logic using filter() is correctly applied
        
        <img width="1247" height="849" alt="image" src="https://github.com/user-attachments/assets/d37ad848-0b0d-4efb-9a7a-bff3073d3ae5" />
        
  - Output 3: Customers with No Orders
     - This output shows customers who do not have any matching records in the sales dataset
     - It verifies that the left_anti join correctly filters out customers with orders
       
        <img width="1671" height="866" alt="image" src="https://github.com/user-attachments/assets/5609ec7e-3e2b-45ee-b183-2718a898d7b3" />


🔹 Learnings

- Understood how PySpark DataFrames work compared to SQL tables
- Learned how joins and aggregations are used in real-world scenarios
- Gained clarity on when to use SQL vs PySpark
- Improved understanding of data cleaning before transformations

🔹 Challenges

- Handling data types properly using cast()
- Dealing with precision issues between float and double
- Debugging unexpected outputs after joins or aggregations

🔹 Files in this Folder

- solution.py → PySpark implementation
- queries.sql → SQL solutions
- phase2_problem_statement.pdf → Problem description
- outputs/ → Output screenshots

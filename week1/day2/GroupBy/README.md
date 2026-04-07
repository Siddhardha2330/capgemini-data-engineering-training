# Day 2 – SQL GroupBy Practice

## 🔹 Objective

- Master SQL GROUP BY clause with aggregations
- Learn to use HAVING clause for filtered aggregations
- Practice JOIN operations with GROUP BY
- Build real-world analytical queries

---

## 🔹 Problem Summary

- Given Employee and Sales tables
- Required to:
  - Perform basic aggregations (SUM, COUNT, AVG, MAX, MIN)
  - Apply GROUP BY with HAVING conditions
  - Combine JOIN and GROUP BY for cross-table analysis
  - Solve real-world business scenarios

---

## 🔹 Database Schema

### Employee Table (8 rows)
```sql
emp_id (INT) - Primary Key
emp_name (VARCHAR) - Employee name
department (VARCHAR) - Department name
salary (DECIMAL) - Employee salary
joining_date (DATE) - Date of joining
```

### Sales Table (8 rows)
```sql
sales_id (INT) - Primary Key
emp_id (INT) - Foreign Key to Employee
product (VARCHAR) - Product name
amount (DECIMAL) - Sale amount
sale_date (DATE) - Date of sale
```

---

## 🔹 Query Categories

### Basic GROUP BY (Queries 1-5)
- Total salary per department
- Employee count per department
- Average salary per department
- Maximum salary per department
- Minimum salary per department

### GROUP BY with Conditions (Queries 6-10)
- Filter departments by total salary > 100,000
- Count employees in departments with > 2 employees
- Average salary for employees joined after 2021-01-01
- Departments with max salary > 75,000
- Departments with total salary < 150,000

### GROUP BY with HAVING (Queries 11-15)
- Departments with more than 1 employee
- Departments with total salary > 125,000
- Departments with more than 2 employees
- Departments with average salary > 60,000
- Departments with salary between 100,000 and 200,000

### GROUP BY with JOINs (Queries 16-20)
- Total sales amount per employee
- Number of sales per employee
- Total sales per product
- Average sales per product
- Employees with more than 2 sales

### Advanced GROUP BY (Queries 21-25)
- Total salary and sales per employee
- Unique products sold per employee
- Highest sales amount per employee
- Products with total sales > 50,000
- Department with highest average sales

### Real-World Scenarios (Queries 26-30)
- Department with highest total sales
- Top 3 employees by sales amount
- Employee count and average salary by joining year
- Total sales per department (with LEFT JOIN)
- Employees with no sales by department

---

## 🔹 Key SQL Concepts Used

- Aggregation Functions
  - `SUM()` - Calculate total
  - `COUNT()` - Count records
  - `AVG()` - Calculate average
  - `MAX()` - Find maximum value
  - `MIN()` - Find minimum value
  - `COUNT(DISTINCT)` - Count unique values

- Grouping and Filtering
  - `GROUP BY` - Group rows by column
  - `HAVING` - Filter grouped results
  - `WHERE` - Filter before grouping

- Joins
  - `INNER JOIN` - Matching records only
  - `LEFT JOIN` - All records from left table

- Sorting and Limiting
  - `ORDER BY` - Sort results
  - `LIMIT` - Restrict number of rows

- Date Functions
  - `YEAR()` - Extract year from date

---

## 🔹 Sample Queries

### Query 1: Total Salary by Department
```sql
SELECT department, SUM(salary) AS total_salary 
FROM Employee 
GROUP BY department;
```

### Query 16: Total Sales per Employee
```sql
SELECT e.emp_id, e.emp_name, SUM(s.amount) AS total_sales 
FROM Employee e 
JOIN Sales s ON e.emp_id = s.emp_id 
GROUP BY e.emp_id, e.emp_name;
```

### Query 25: Department with Highest Average Sales
```sql
SELECT e.department, AVG(s.amount) AS avg_sales 
FROM Employee e 
JOIN Sales s ON e.emp_id = s.emp_id 
GROUP BY e.department 
ORDER BY avg_sales DESC 
LIMIT 1;
```

---

## 🔹 Learning Outcomes

- **Basic Aggregations**: Learned SUM, COUNT, AVG, MAX, MIN functions
- **GROUP BY**: Understood how to group data by columns
- **HAVING vs WHERE**: Learned when to filter before vs after grouping
- **JOIN with GROUP BY**: Combined tables and aggregated results
- **Business Analytics**: Applied SQL to solve real business problems
- **Query Optimization**: Used appropriate filters and aggregations

---

## 🔹 Key Differences

### WHERE vs HAVING
- **WHERE**: Filters rows before grouping
  ```sql
  WHERE joining_date > '2021-01-01'
  ```
- **HAVING**: Filters groups after aggregation
  ```sql
  HAVING SUM(salary) > 100000
  ```

### COUNT(*) vs COUNT(DISTINCT)
- **COUNT(*)**: Counts all rows
- **COUNT(DISTINCT column)**: Counts unique values only

---

## 🔹 Common Patterns

1. **Aggregation Pattern**
   ```sql
   SELECT column, AGG_FUNCTION(column)
   FROM table
   GROUP BY column;
   ```

2. **Filtered Aggregation Pattern**
   ```sql
   SELECT column, AGG_FUNCTION(column)
   FROM table
   GROUP BY column
   HAVING AGG_FUNCTION(column) > value;
   ```

3. **JOIN + Aggregation Pattern**
   ```sql
   SELECT t1.column, AGG_FUNCTION(t2.column)
   FROM table1 t1
   JOIN table2 t2 ON t1.key = t2.key
   GROUP BY t1.column;
   ```

---

## 🔹 Practice Tips

- Always include non-aggregated columns in GROUP BY
- Use HAVING for filtering aggregated results
- Use WHERE for filtering raw data before grouping
- Combine ORDER BY with LIMIT for top N queries
- Use aliases for better readability
- Test queries incrementally (select → join → group → filter)

---

## 🔹 File Structure

- day 2 SQL GroupBy.dbquery.ipynb → SQL queries file
- README_SQL_GroupBy.md → This documentation
- Database: workspace.default
- Tables: Employee, Sales

---

## 🔹 How to Run

1. Open "day 2 SQL GroupBy.dbquery.ipynb" in SQL Editor
2. Ensure catalog is set to `workspace` and schema to `default`
3. Run statements 1-4 to create and populate tables
4. Run any query (5-30) to practice GROUP BY concepts
5. Each query is independent and can be run separately

---

## 🔹 Query Summary Table

| Query # | Topic | Difficulty |
|---------|-------|------------|
| 1-5 | Basic GROUP BY | Easy |
| 6-10 | GROUP BY with Conditions | Easy |
| 11-15 | GROUP BY with HAVING | Medium |
| 16-20 | GROUP BY with JOINs | Medium |
| 21-25 | Advanced GROUP BY | Hard |
| 26-30 | Real-World Scenarios | Hard |

**Total Queries**: 30  
**Topics Covered**: Aggregations, GROUP BY, HAVING, JOINs, Date Functions  
**Use Cases**: Department analysis, Sales reporting, Employee performance

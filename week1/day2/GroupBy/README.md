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

## 🔹 File Structure
- README.md -> About groupby_queries.sql
- groupby_queries.sql -> Queries


---

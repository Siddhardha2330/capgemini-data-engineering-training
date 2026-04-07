# Day 2 - SQL Practice (GROUP BY & Joins)

## 🔹 What I Learned

Today I practiced two main SQL topics:
1. GROUP BY with aggregations
2. Different types of JOINs

---

## 🔹 Part 1: GROUP BY Practice

### Tables Used
- Employee (8 rows) - emp_id, emp_name, department, salary, joining_date
- Sales (8 rows) - sales_id, emp_id, product, amount, sale_date

### What I Practiced

**Basic Aggregations (Queries 1-5)**
- Calculate total salary per department using SUM
- Count employees in each department
- Find average, maximum, minimum salary per department

**Filtering Groups (Queries 6-15)**
- Used WHERE to filter data before grouping
- Used HAVING to filter groups after aggregation
- Example: Find departments with total salary > 100000

**Joins with GROUP BY (Queries 16-25)**
- Joined Employee and Sales tables
- Calculated total sales per employee
- Found top performing employees and products

**Real Scenarios (Queries 26-30)**
- Department with highest sales
- Top 3 employees by sales
- Employees without any sales

### Key Concepts
- `SUM()`, `COUNT()`, `AVG()`, `MAX()`, `MIN()`
- `GROUP BY` - group rows by column
- `HAVING` - filter after grouping
- `WHERE` - filter before grouping

---

## 🔹 Part 2: SQL Joins Practice

### Tables Used
- employees (8 rows) - emp_id, emp_name, manager_id, dept_id
- departments (5 rows) - dept_id, dept_name
- projects (5 rows) - project_id, project_name, emp_id

### What I Practiced

**Self-Joins (Queries 1, 3, 17, 23, 24)**
- Joined employees table with itself
- Found employee-manager relationships
- Used aliases like `e` for employee and `m` for manager

**LEFT JOIN (Queries 2, 5, 6, 11, 14, 15)**
- Showed all records from left table
- Included NULL for missing matches
- Found employees without departments
- Found departments without employees

**INNER JOIN (Queries 7, 13, 18, 19)**
- Showed only matching records
- Excluded NULL relationships

**Multiple Joins (Queries 17, 21, 29)**
- Joined 3 tables together
- Combined employees, departments, and projects

**NULL Handling (Queries 5, 14, 21, 27)**
- Used IS NULL to find missing relationships
- Identified data quality issues

**Aggregation (Queries 22, 25)**
- Counted employees per department
- Used LEFT JOIN to include zero counts

### Key Concepts
- `INNER JOIN` - only matching records
- `LEFT JOIN` - all from left table
- Self-join - table joined with itself
- `IS NULL` - find missing data

---

## 🔹 Key Differences I Learned

### WHERE vs HAVING
- WHERE filters rows before grouping
- HAVING filters groups after aggregation

### INNER JOIN vs LEFT JOIN
- INNER JOIN - only matching records
- LEFT JOIN - all records from left, NULL for non-matching

---

## 🔹 Files

**GROUP BY Practice:**
- day 2 SQL GroupBy.dbquery.ipynb
- README_SQL_GroupBy.md

**Joins Practice:**
- day 2 SQL Joins.dbquery.ipynb
- README_SQL_Joins.md

**Database:** workspace.default

---

## 🔹 Total Practice

- 30 GROUP BY queries
- 30 JOIN queries
- 60 total queries practiced

---

## 🔹 What I Can Do Now

- Calculate totals and averages by groups
- Count records per category
- Find top N records using ORDER BY and LIMIT
- Join multiple tables together
- Handle employee-manager hierarchies using self-joins
- Find missing relationships using LEFT JOIN and IS NULL
- Combine joins with aggregations

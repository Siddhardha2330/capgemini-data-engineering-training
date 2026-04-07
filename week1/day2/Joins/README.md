# Day 2 – SQL Joins Practice

## 🔹 Objective

- Master different types of SQL joins
- Learn self-joins for employee-manager relationships
- Practice multiple table joins
- Handle NULL values in join operations

---

## 🔹 Problem Summary

- Given three tables: employees, departments, and projects
- Required to:
  - Perform INNER JOIN, LEFT JOIN, and self-joins
  - Handle employees with and without managers
  - Find records with missing relationships
  - Combine multiple tables in single queries

---

## 🔹 Database Schema

### Employees (8 rows)
- emp_id, emp_name, manager_id, dept_id

### Departments (5 rows)
- dept_id, dept_name

### Projects (5 rows)
- project_id, project_name, emp_id

---

## 🔹 Approach

- Self-Joins:
  - Joined employees table with itself
  - Matched emp_id with manager_id
  - Displayed employee-manager relationships

- LEFT JOIN:
  - Included all records from left table
  - Showed NULL for non-matching records
  - Used to find missing relationships

- INNER JOIN:
  - Showed only matching records
  - Filtered out NULL relationships

- Multiple Joins:
  - Combined 3 tables in single query
  - Joined employees with departments and projects

---

## 🔹 Tasks Implemented

**Self-Joins (Queries 1, 3, 17, 23, 24)**
- Employee and manager names
- Employees who report to managers
- Employee hierarchy with projects

**LEFT JOIN (Queries 2, 5, 6, 11, 14, 15, 20, 21, 26, 28, 29, 30)**
- All employees with departments (including NULL)
- All departments with employees
- Employees without departments
- Employees without projects

**INNER JOIN (Queries 7, 13, 18, 19)**
- Employees with departments only
- Employees with projects only

**Multiple Joins (Queries 17, 21, 29)**
- Employees with managers and projects
- Employees with departments and projects

**NULL Handling (Queries 5, 14, 21, 27)**
- Find employees without departments
- Find employees without projects

**Aggregation (Queries 22, 25)**
- Count employees per department
- Include departments with zero employees

---

## 🔹 Key SQL Operations Used

- Join Types → `JOIN`, `LEFT JOIN`
- Self-Join → `FROM employees e JOIN employees m`
- NULL Filtering → `IS NULL`, `IS NOT NULL`
- Multiple Joins → Join 3+ tables
- Aggregation → `COUNT()`, `GROUP BY`
- Table Aliases → `e`, `m`, `d`, `p`

---

## 🔹 Sample Queries

**Query 1: Employee and Manager Names**
```sql
SELECT e.emp_name, m.emp_name AS manager_name
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.emp_id;
```

**Query 11: All Departments with Employees**
```sql
SELECT d.dept_name, e.emp_name
FROM departments d
LEFT JOIN employees e ON d.dept_id = e.dept_id;
```

**Query 22: Count Employees per Department**
```sql
SELECT d.dept_name, COUNT(e.emp_id) AS total_employees
FROM departments d
LEFT JOIN employees e ON d.dept_id = e.dept_id
GROUP BY d.dept_name;
```

---

## 🔹 Output

- Output 1: Self-Join Results
  - Shows employee-manager relationships
  - Includes employees without managers (NULL)

- Output 2: LEFT JOIN Results
  - Shows all departments including empty ones
  - Verifies NULL handling works correctly

- Output 3: Aggregation Results
  - Counts employees per department
  - Shows 0 for departments with no employees

---

## 🔹 Key Differences

### INNER JOIN vs LEFT JOIN

**INNER JOIN:**
- Only matching records
- Excludes NULL relationships

**LEFT JOIN:**
- All records from left table
- Includes NULL for non-matching records

---

## 🔹 Data Engineering Considerations

- Used LEFT JOIN to preserve all records from primary table
- Applied INNER JOIN only when strict matching required
- Handled NULL values appropriately using IS NULL
- Used self-joins for hierarchical data
- Combined multiple joins efficiently

---

## 🔹 Challenges Faced

- Understanding when to use INNER vs LEFT JOIN
- Handling self-joins with correct aliases
- Managing NULL values in join results
- Combining multiple tables in single query

---

## 🔹 Learnings

- Mastered different join types and their use cases
- Learned self-joins for hierarchical relationships
- Understood NULL handling in joins
- Practiced multiple table joins
- Applied joins with aggregations

---

## 🔹 Project Structure

- day 2 SQL Joins.dbquery.ipynb → SQL queries file
- README_SQL_Joins.md → Project documentation
- Database: workspace.default
- Tables: employees, departments, projects

---

## 🔹 How to Run

1. Open "day 2 SQL Joins.dbquery.ipynb" in SQL Editor
2. Set catalog to `workspace` and schema to `default`
3. Run statements 1-6 to create tables
4. Run statement 7 containing all 30 queries
5. Each query is independent and can run separately

---

## 🔹 Query Summary

**Total Queries**: 30

**By Join Type:**
- Self-Joins: 5 queries
- LEFT JOIN: 12 queries
- INNER JOIN: 4 queries
- Multiple Joins: 3 queries
- NULL Handling: 4 queries
- Aggregation: 2 queries

**Topics Covered**: INNER JOIN, LEFT JOIN, Self-Join, Multiple Joins, NULL Handling

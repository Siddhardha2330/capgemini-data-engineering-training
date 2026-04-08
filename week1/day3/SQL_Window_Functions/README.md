# Day 3 – SQL Window Functions Practice

## 🔹 Objective

- Master SQL window functions (ROW_NUMBER, RANK, DENSE_RANK)
- Learn to use PARTITION BY for grouped rankings
- Practice ORDER BY with window functions
- Understand differences between ranking functions
- Build real-world analytical queries with window functions

---

## 🔹 Problem Summary

- Given Employee table with 20 records across 3 departments
- Required to:
  - Assign row numbers with different ordering criteria
  - Rank employees by salary, joining date, and name
  - Use PARTITION BY for department-wise rankings
  - Apply RANK and DENSE_RANK to handle ties
  - Solve real-world ranking scenarios

---

## 🔹 Database Schema

### employees Table (20 rows)
```sql
emp_id (INT) - Employee ID
emp_name (VARCHAR) - Employee name
department (VARCHAR) - Department/City (Chennai, Hyderabad, Bangalore)
salary (INT) - Employee salary (1500-3000 range)
join_date (DATE) - Joining date
```

---

## 🔹 Query Categories

### ROW_NUMBER() Queries (Q1-Q8)
- Q1: Row numbers by salary (highest first)
- Q2: Row numbers within department by salary (desc)
- Q3: Row numbers by latest joining date
- Q4: Row numbers within department by earliest join date
- Q5: Row numbers by joining date (latest first)
- Q6: Row numbers within department by salary (asc)
- Q7: Row numbers by salary (lowest first)
- Q8: Row numbers within department by name alphabetically

### RANK() Queries (Q9-Q16)
- Q9: Rank all employees by salary (highest first)
- Q10: Rank within department by salary
- Q11: Rank by joining date (latest = rank 1)
- Q12: Rank within department by salary (lowest first)
- Q13: Rank by joining date (earliest first)
- Q14: Rank within department by joining date (desc)
- Q15: Rank by name alphabetically
- Q16: Rank within department by name

### DENSE_RANK() Queries (Q17-Q25)
- Q17: Dense rank by salary (highest first)
- Q18: Dense rank within department
- Q19: Dense rank by joining date (latest first)
- Q20: Dense rank within department by joining date
- Q21: Dense rank by joining date (earliest first)
- Q22: Dense rank by salary (lowest first)
- Q23: Dense rank within department by join date
- Q24: Dense rank by name alphabetically
- Q25: Dense rank within department by salary (asc)

---

## 🔹 Key SQL Concepts Used

- Window Functions
  - `ROW_NUMBER()` - Assigns unique sequential integers
  - `RANK()` - Assigns ranks with gaps for ties
  - `DENSE_RANK()` - Assigns ranks without gaps for ties

- Window Clauses
  - `PARTITION BY` - Divides result set into partitions (e.g., by department)
  - `ORDER BY` - Determines the order for ranking/numbering
  - `OVER()` - Defines the window specification

- Ordering
  - `DESC` - Descending order (highest to lowest)
  - `ASC` - Ascending order (lowest to highest)

---

## 🔹 Sample Queries

### Query 1: ROW_NUMBER by Salary (Highest First)
```sql
SELECT *, 
  ROW_NUMBER() OVER (ORDER BY salary DESC) AS row_num 
FROM employees;
```

### Query 2: ROW_NUMBER within Department
```sql
SELECT *, 
  ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS Salary_wise_rank 
FROM employees;
```

### Query 9: RANK by Salary (Highest First)
```sql
SELECT *, 
  RANK() OVER (ORDER BY salary DESC) AS salary_rank 
FROM employees;
```

### Query 10: RANK within Department
```sql
SELECT *, 
  RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_salary_rank 
FROM employees;
```

### Query 17: DENSE_RANK by Salary
```sql
SELECT *, 
  DENSE_RANK() OVER (ORDER BY salary DESC) AS dense_salary_rank 
FROM employees;
```

### Query 18: DENSE_RANK within Department
```sql
SELECT *, 
  DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_dense_rank 
FROM employees;
```

---

## 🔹 Learning Outcomes

- **Window Functions**: Mastered ROW_NUMBER, RANK, and DENSE_RANK
- **PARTITION BY**: Learned to apply rankings within groups (departments)
- **Handling Ties**: Understood how each function treats duplicate values differently
- **Ordering Options**: Practiced both ASC and DESC ordering
- **Real-World Analytics**: Applied window functions to employee ranking scenarios
- **Query Optimization**: Understood when to use each ranking function

---

## 🔹 File Structure
- README.md -> About window functions practice
- Day 3 SQL Window Functions.sql -> 25+ window function queries
- SQL_window_functions_problem_statement.pdf -> problem statement

---

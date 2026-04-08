
# DAY 3 – SQL PRACTICE (CASE STATEMENTS & WINDOW FUNCTIONS)

---

## OVERVIEW

This repository contains Day 3 SQL practice covering two important areas:

- SQL CASE and WHEN (Conditional Logic)
- SQL Window Functions (Analytical Queries)

The goal is to build strong foundations in writing real-world SQL queries involving business rules and data analysis.

---

# SECTION 1: SQL CASE AND WHEN

---

## OBJECTIVE

- Master SQL CASE statements for conditional logic  
- Learn simple and nested CASE expressions  
- Apply CASE with multiple conditions (AND, OR, BETWEEN)  
- Implement real-world business rules in SQL  

---

## PROBLEM SUMMARY

- Employee table with 25 records  
- Tasks included:
  - Bonus calculation based on department and performance  
  - Employee categorization using salary and ratings  
  - Risk assessment using experience and department  
  - Salary hike calculations with nested conditions  
  - Promotion eligibility logic  
  - Tax bracket assignment  

---

## DATABASE SCHEMA

### Employee Table (25 rows)

```sql
emp_id INT
emp_name VARCHAR
department VARCHAR  -- Engineering, HR, Finance, Marketing
salary INT          -- 45,000 to 102,000
experience INT      -- 1 to 14 years
performance_rating CHAR  -- A, B, C
```

---

## QUERY CATEGORIES

### Simple CASE Statements

- Bonus calculation by department and performance  
- Employee categorization  
- Risk assessment  

### Nested CASE Statements

- Salary hike calculation  
- Advanced employee categorization  
- Tax bracket assignment  
- Promotion eligibility  

---

## KEY SQL CONCEPTS

### Conditional Logic

- CASE WHEN ... THEN ... END  
- ELSE for default conditions  
- Nested CASE for complex logic  

### Operators

- Comparison: =, >, <, >=, <=  
- Range: BETWEEN  
- Multiple values: IN()  

### Logical Operators

- AND  
- OR  

### Arithmetic

- Percentage calculations using multiplication  

---

## SAMPLE QUERIES

### Bonus Calculation

```sql
SELECT *,
CASE 
    WHEN department='Finance' AND performance_rating='A' THEN salary * 0.20
    WHEN department='Finance' AND performance_rating='B' THEN salary * 0.15
    WHEN department='Finance' AND performance_rating='C' THEN salary * 0.05
    WHEN department='Engineering' AND performance_rating='A' THEN salary * 0.18
    WHEN department='Engineering' AND performance_rating='B' THEN salary * 0.12
    WHEN department='Engineering' AND performance_rating='C' THEN salary * 0.03
    ELSE salary * 0.10 
END AS bonus
FROM Employee;
```

### Employee Categorization

```sql
SELECT *, 
CASE 
    WHEN salary > 80000 AND performance_rating='A' THEN 'High Performer'
    WHEN salary BETWEEN 50000 AND 80000 AND performance_rating='B' THEN 'Mid Performer'
    WHEN salary < 50000 OR performance_rating='C' THEN 'Low Performer'
END AS category
FROM Employee;
```

### Risk Assessment

```sql
SELECT *, 
CASE
    WHEN department='HR' AND experience < 5 THEN 'High Risk'
    WHEN department='HR' AND experience > 5 THEN 'Low Risk'
    WHEN department IN ('Engineering', 'Finance') AND experience > 8 THEN 'Low Risk'
    WHEN department IN ('Engineering', 'Finance') AND experience < 8 THEN 'Medium Risk'
    ELSE 'Medium Risk'
END AS risk
FROM Employee;
```

---

## LEARNING OUTCOMES

- Mastered simple and nested CASE expressions  
- Implemented complex business rules  
- Used multiple conditions with AND/OR  
- Applied range-based filtering using BETWEEN  
- Built real-world SQL logic  

---

# SECTION 2: SQL WINDOW FUNCTIONS

---

## OBJECTIVE

- Master ROW_NUMBER, RANK, and DENSE_RANK  
- Use PARTITION BY for grouped analysis  
- Understand ranking differences  
- Build analytical queries  

---

## PROBLEM SUMMARY

- Employee table with 20 records across departments  
- Tasks included:
  - Assign row numbers  
  - Rank employees by salary and date  
  - Department-wise rankings  
  - Handle ties using RANK and DENSE_RANK  

---

## DATABASE SCHEMA

### employees Table (20 rows)

```sql
emp_id INT
emp_name VARCHAR
department VARCHAR  -- Chennai, Hyderabad, Bangalore
salary INT          -- 1500 to 3000
join_date DATE
```

---

## QUERY CATEGORIES

### ROW_NUMBER()

- Row numbering by salary  
- Department-wise numbering  
- Ordering by join date and name  

### RANK()

- Salary ranking  
- Department ranking  
- Ranking with ties  

### DENSE_RANK()

- Ranking without gaps  
- Department-wise dense ranking  

---

## KEY SQL CONCEPTS

### Window Functions

- ROW_NUMBER()  
- RANK()  
- DENSE_RANK()  

### Window Clause

- PARTITION BY  
- ORDER BY  
- OVER()  

### Ordering

- ASC (ascending)  
- DESC (descending)  

---

## SAMPLE QUERIES

### ROW_NUMBER by Salary

```sql
SELECT *, 
ROW_NUMBER() OVER (ORDER BY salary DESC) AS row_num 
FROM employees;
```

### ROW_NUMBER within Department

```sql
SELECT *, 
ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_rank 
FROM employees;
```

### RANK by Salary

```sql
SELECT *, 
RANK() OVER (ORDER BY salary DESC) AS salary_rank 
FROM employees;
```

### DENSE_RANK within Department

```sql
SELECT *, 
DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_dense_rank 
FROM employees;
```

---

## LEARNING OUTCOMES

- Mastered window functions  
- Understood differences between ranking methods  
- Applied partition-based analysis  
- Built real-world ranking queries  
- Improved analytical SQL skills  

---

## FILE STRUCTURE

- README.md → Combined documentation  
- Day3_SQL_Case_When.sql → CASE practice queries  
- Day3_SQL_Window_Functions.sql → Window function queries  
- SQL_Case_When_problem_statement.pdf → CASE problems  
- SQL_Window_Functions_problem_statement.pdf → Window problems  

---

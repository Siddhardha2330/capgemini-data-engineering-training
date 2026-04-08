# Day 3 – SQL CASE and WHEN Practice

## 🔹 Objective

- Master SQL CASE statements for conditional logic
- Learn simple CASE expressions for decision-making
- Practice nested CASE statements for complex conditions
- Apply CASE with multiple criteria (AND, OR, BETWEEN)
- Build real-world business rule queries

---

## 🔹 Problem Summary

- Given Employee table with 25 records
- Required to:
  - Calculate bonuses based on department and performance
  - Categorize employees by salary range and performance
  - Assess risk levels based on experience and department
  - Determine salary hikes with nested conditions
  - Evaluate promotion eligibility using multiple criteria
  - Assign tax brackets based on salary and experience

---

## 🔹 Database Schema

### Employee Table (25 rows)
```sql
emp_id (INT) - Employee ID
emp_name (VARCHAR) - Employee name
department (VARCHAR) - Department (Engineering, HR, Finance, Marketing)
salary (INT) - Employee salary (45,000-102,000 range)
experience (INT) - Years of experience (1-14 years)
performance_rating (CHAR) - Performance rating (A, B, C)
```

---

## 🔹 Query Categories

### Simple CASE Statements (Problems 1-4)

**Problem 1: Bonus Calculation by Department and Performance**
- Finance: 20% (A), 15% (B), 5% (C)
- Engineering: 18% (A), 12% (B), 3% (C)
- Other departments: Flat 10%

**Problem 2: Employee Categorization by Salary and Performance**
- High Performer: salary > 80,000 AND rating = 'A'
- Mid Performer: salary 50,000-80,000 AND rating = 'B'
- Low Performer: salary < 50,000 OR rating = 'C'

**Problem 3: Risk Assessment by Experience and Department**
- HR: High Risk (< 5 years), Low Risk (> 5 years)
- Engineering/Finance: Low Risk (> 8 years), Medium Risk (< 8 years)
- Other departments: Medium Risk

### Nested CASE Statements (Problems 4-8)

**Problem 4: Salary Hike with Multiple Criteria**
- Performance 'A':
  - Salary > 80,000: 25% hike (exp > 5), 20% hike (exp ≤ 5)
  - Salary 50,000-80,000: 15% hike
- Performance 'B': 12% (exp > 5), 10% (exp ≤ 5)
- Performance 'C': No hike

**Problem 5: Employee Categorization (Advanced)**
- Salary > 70,000:
  - Top Performer: Rating 'A' AND exp > 8 years
  - Mid Performer: exp ≤ 8 years
- Salary 50,000-70,000:
  - Rising Star: Rating 'A'
  - Average Performer: Otherwise
- Salary < 50,000: Low Performer

**Problem 6: Tax Bracket Assignment**
- Salary > 90,000: 35% (exp > 10), 30% (exp ≤ 10)
- Salary 60,000-90,000: 25% (exp > 5), 20% (exp ≤ 5)
- Salary < 60,000: 15%

**Problem 7: Promotion Eligibility**
- Performance 'A':
  - Eligible for Senior Role: salary > 75,000 AND exp > 7
  - Eligible for Junior Role: Otherwise
- Performance 'B':
  - Eligible for Consideration: exp > 5
  - Not Eligible: exp ≤ 5
- Performance 'C': Not Eligible

---

## 🔹 Key SQL Concepts Used

- Conditional Logic
  - `CASE WHEN ... THEN ... END` - Basic conditional expression
  - `ELSE` - Default condition when no match
  - Nested `CASE` - Multiple levels of conditions

- Comparison Operators
  - `=` - Equality check
  - `>`, `<`, `>=`, `<=` - Numeric comparisons
  - `BETWEEN` - Range checking
  - `IN()` - Multiple value matching

- Logical Operators
  - `AND` - Multiple conditions must be true
  - `OR` - At least one condition must be true

- Arithmetic Operations
  - `salary * 0.20` - Calculate percentages
  - Multiplication for bonus/hike calculations

---

## 🔹 Sample Queries

### Query 1: Bonus Calculation by Department
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

### Query 2: Employee Categorization
```sql
SELECT *, CASE 
    WHEN salary>80000 AND performance_rating='A' THEN 'High Performer'
    WHEN salary>50000 AND salary<=80000 AND performance_rating='B' THEN 'Mid Performer'
    WHEN salary<50000 OR performance_rating='C' THEN 'Low Performer'
END AS category
FROM Employee;
```

### Query 3: Risk Assessment
```sql
SELECT *, CASE
    WHEN department='HR' AND experience<5 THEN 'High Risk'
    WHEN department='HR' AND experience>5 THEN 'Low Risk'
    WHEN department IN ('Engineering', 'Finance') AND experience>8 THEN 'Low Risk'
    WHEN department IN ('Engineering', 'Finance') AND experience<8 THEN 'Medium Risk'
    ELSE 'Medium Risk'
END AS risk
FROM Employee;
```

### Query 4: Nested CASE for Salary Hike
```sql
SELECT *, CASE 
    WHEN performance_rating='A' AND salary>80000 AND experience>5 THEN salary*0.25
    WHEN performance_rating='A' AND salary>80000 AND experience<=5 THEN salary*0.20
    WHEN performance_rating='A' AND salary BETWEEN 50000 AND 80000 THEN salary*0.15
    WHEN performance_rating='B' AND experience>5 THEN salary*0.12
    WHEN performance_rating='B' AND experience<=5 THEN salary*0.10
    ELSE 0
END AS hike
FROM Employee;
```

### Query 5: Tax Bracket Assignment
```sql
SELECT *, CASE 
    WHEN salary>90000 AND experience>10 THEN '35%'
    WHEN salary>90000 AND experience<=10 THEN '30%'
    WHEN salary BETWEEN 60000 AND 90000 AND experience>5 THEN '25%'
    WHEN salary BETWEEN 60000 AND 90000 AND experience<=5 THEN '20%'
    ELSE '15%'
END AS tax_bracket
FROM Employee;
```

### Query 6: Promotion Eligibility
```sql
SELECT *, CASE 
    WHEN performance_rating='A' AND salary>75000 AND experience>7 THEN 'Eligible for Senior Role'
    WHEN performance_rating='A' AND salary>75000 AND experience<=7 THEN 'Eligible for Junior Role'
    WHEN performance_rating='B' AND experience>5 THEN 'Eligible for Consideration'
    WHEN performance_rating='B' AND experience<=5 THEN 'Not Eligible'
    WHEN performance_rating='C' THEN 'Not Eligible'
END AS promotion_eligibility
FROM Employee;
```

---

## 🔹 Learning Outcomes

- **CASE Statements**: Mastered simple and nested CASE expressions
- **Conditional Logic**: Learned to implement complex business rules in SQL
- **Multiple Criteria**: Combined AND/OR operators for multi-condition checks
- **Range Checking**: Used BETWEEN and comparison operators effectively
- **Real-World Applications**: Applied CASE to bonuses, categories, risk assessment, and promotions
- **Code Organization**: Structured nested conditions for readability and maintenance

---

## 🔹 File Structure
- README.md -> About SQL Case and When statements practice
- Day 3 SQL_Case_When.sql -> 25+ window function queries
- SQL_Case_When_problem_statement.pdf -> problem statement


---

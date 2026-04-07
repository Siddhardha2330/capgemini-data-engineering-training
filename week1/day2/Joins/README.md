# Day 2 – SQL Joins Practice

## 🔹 Objective

- Master different types of SQL joins (INNER, LEFT, RIGHT, FULL OUTER)
- Learn self-joins for hierarchical data (employee-manager relationships)
- Practice multiple table joins
- Handle NULL values in join operations
- Build real-world queries with complex join conditions

---

## 🔹 Problem Summary

- Given three tables: employees, departments, and projects
- Required to:
  - Perform various join operations (LEFT, INNER, self-joins)
  - Handle employees with and without managers
  - Include/exclude NULL values based on requirements
  - Combine multiple tables in a single query
  - Apply aggregations with joins

---

## 🔹 Database Schema

### Employees Table (8 rows)
```sql
emp_id (INT) - Primary Key
emp_name (VARCHAR) - Employee name
manager_id (INT) - Foreign Key to emp_id (self-reference)
dept_id (INT) - Foreign Key to departments
```

**Sample Data:**
- Karthik (emp_id=1, manager_id=NULL) - Top manager
- Ajay (emp_id=2, manager_id=1) - Reports to Karthik
- Vijay, Vinay, Meena, Veer, Keerthi, Priya

### Departments Table (5 rows)
```sql
dept_id (INT) - Primary Key
dept_name (VARCHAR) - Department name
```

**Departments:** HR, IT, Finance, Marketing, Sales

### Projects Table (5 rows)
```sql
project_id (INT) - Primary Key
project_name (VARCHAR) - Project name
emp_id (INT) - Foreign Key to employees
```

**Projects:** Project A, Project B, Project C, Project D, Project E

---

## 🔹 Query Categories

### Self-Joins (Employee-Manager Relationships)
**Queries: 1, 3, 17, 23, 24**
- Join employees table with itself
- Match emp_id with manager_id
- Display employee-manager hierarchies

### LEFT JOIN (Include All from Left Table)
**Queries: 2, 5, 6, 11, 14, 15, 20, 21, 26, 28, 29, 30**
- Include all employees even without departments
- Include all departments even without employees
- Include all employees even without projects

### INNER JOIN (Only Matching Records)
**Queries: 7, 13, 18, 19**
- Show only employees with departments
- Show only employees with projects
- Exclude NULL relationships

### Multiple Joins (3+ Tables)
**Queries: 17, 21, 29**
- Combine employees, managers, and projects
- Join employees, departments, and projects
- Handle complex relationships

### NULL Handling
**Queries: 5, 14, 21, 27**
- Find employees without departments
- Find employees without projects
- Filter using IS NULL conditions

### Aggregation with Joins
**Queries: 22, 25**
- Count employees per department
- Use GROUP BY with LEFT JOIN
- Include departments with zero employees

---

## 🔹 Key SQL Concepts Used

### Join Types
- **INNER JOIN** - Only matching records
  ```sql
  FROM table1 JOIN table2 ON condition
  ```
- **LEFT JOIN** - All from left, matching from right
  ```sql
  FROM table1 LEFT JOIN table2 ON condition
  ```
- **Self-Join** - Table joined with itself
  ```sql
  FROM employees e JOIN employees m ON e.manager_id = m.emp_id
  ```

### NULL Filtering
- `WHERE column IS NULL` - Find missing relationships
- `WHERE column IS NOT NULL` - Exclude missing data

### Multiple Joins
```sql
FROM table1 t1
JOIN table2 t2 ON t1.key = t2.key
LEFT JOIN table3 t3 ON t1.key = t3.key
```

### Aggregation with Joins
```sql
SELECT column, COUNT(*)
FROM table1 LEFT JOIN table2 ON condition
GROUP BY column
```

---

## 🔹 Sample Queries with Explanations

### Query 1: Employee and Manager Names (Self-Join with LEFT JOIN)
```sql
SELECT e.emp_name, m.emp_name AS manager_name
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.emp_id;
```
**Purpose:** Show all employees with their managers, including those without managers (NULL)

### Query 3: Employees Who Report to a Manager (INNER Self-Join)
```sql
SELECT e.emp_name, m.emp_name AS manager_name
FROM employees e
JOIN employees m ON e.manager_id = m.emp_id;
```
**Purpose:** Show only employees who have managers (excludes top-level managers)

### Query 11: All Departments with Employees (LEFT JOIN)
```sql
SELECT d.dept_name, e.emp_name
FROM departments d
LEFT JOIN employees e ON d.dept_id = e.dept_id;
```
**Purpose:** List all departments, including those with no employees

### Query 22: Count Employees per Department (Aggregation + LEFT JOIN)
```sql
SELECT d.dept_name, COUNT(e.emp_id) AS total_employees
FROM departments d
LEFT JOIN employees e ON d.dept_id = e.dept_id
GROUP BY d.dept_name;
```
**Purpose:** Count employees in each department, showing 0 for empty departments

### Query 29: Complete Employee Information (Multiple LEFT JOINs)
```sql
SELECT e.emp_name, d.dept_name, p.project_name
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.dept_id
LEFT JOIN projects p ON e.emp_id = p.emp_id;
```
**Purpose:** Show all employee information with departments and projects, including NULLs

---

## 🔹 Learning Outcomes

- **Join Types Mastery**: Understood difference between INNER JOIN and LEFT JOIN
- **Self-Joins**: Learned how to join a table with itself for hierarchical data
- **NULL Handling**: Practiced filtering and including NULL values appropriately
- **Multiple Joins**: Combined 3+ tables in complex queries
- **Real-World Scenarios**: Applied joins to employee-manager-project relationships
- **Aggregation with Joins**: Calculated counts while preserving all records with LEFT JOIN

---

## 🔹 Key Differences

### INNER JOIN vs LEFT JOIN

**INNER JOIN:**
- Returns only matching records from both tables
- Excludes records with NULL relationships
- Use when you want strictly related data

**LEFT JOIN:**
- Returns all records from left table
- Includes NULL for non-matching records from right table
- Use when you want to preserve all records from left table

### Example Comparison:
```sql
-- INNER JOIN: Only employees WITH departments
SELECT e.emp_name, d.dept_name
FROM employees e
JOIN departments d ON e.dept_id = d.dept_id;

-- LEFT JOIN: ALL employees (even without departments)
SELECT e.emp_name, d.dept_name
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.dept_id;
```

---

## 🔹 Common Patterns

### Pattern 1: Self-Join for Hierarchies
```sql
SELECT e.column, m.column AS parent_column
FROM table e
LEFT JOIN table m ON e.parent_id = m.id;
```

### Pattern 2: LEFT JOIN to Include All Records
```sql
SELECT t1.column, t2.column
FROM table1 t1
LEFT JOIN table2 t2 ON t1.key = t2.key;
```

### Pattern 3: Multiple Joins
```sql
SELECT t1.col, t2.col, t3.col
FROM table1 t1
JOIN table2 t2 ON t1.key = t2.key
LEFT JOIN table3 t3 ON t1.key = t3.key;
```

### Pattern 4: Find Records Without Relationships
```sql
SELECT t1.column
FROM table1 t1
LEFT JOIN table2 t2 ON t1.key = t2.key
WHERE t2.key IS NULL;
```

### Pattern 5: Aggregation Preserving All Records
```sql
SELECT t1.column, COUNT(t2.id)
FROM table1 t1
LEFT JOIN table2 t2 ON t1.key = t2.key
GROUP BY t1.column;
```

---

## 🔹 Practice Tips

1. **Start Simple**: Begin with 2-table joins before adding more tables
2. **Use Aliases**: Always use table aliases (e, d, p) for readability
3. **Check NULL Behavior**: Test if LEFT JOIN or INNER JOIN is appropriate
4. **Self-Join Naming**: Use meaningful aliases like `e` for employee, `m` for manager
5. **Verify Results**: Check if NULL values appear as expected
6. **Multiple Joins**: Add one join at a time and verify results incrementally
7. **Aggregation**: Use LEFT JOIN when counting to include zero counts

---

## 🔹 Common Mistakes to Avoid

❌ **Using INNER JOIN when you need all records**
```sql
-- Wrong: Excludes employees without departments
FROM employees e JOIN departments d
```
✅ **Use LEFT JOIN to include all employees**
```sql
-- Correct: Includes all employees
FROM employees e LEFT JOIN departments d
```

❌ **Forgetting table aliases in self-joins**
```sql
-- Wrong: Ambiguous column names
FROM employees JOIN employees
```
✅ **Use clear aliases**
```sql
-- Correct: Clear employee vs manager
FROM employees e JOIN employees m
```

❌ **Wrong join condition in self-joins**
```sql
-- Wrong: Joins on same column
ON e.emp_id = m.emp_id
```
✅ **Join manager_id to emp_id**
```sql
-- Correct: Employee's manager_id = Manager's emp_id
ON e.manager_id = m.emp_id
```

---

## 🔹 File Structure

- day 2 SQL Joins.dbquery.ipynb → SQL queries file
- README_SQL_Joins.md → This documentation
- Database: workspace.default
- Tables: employees, departments, projects

---

## 🔹 How to Run

1. Open "day 2 SQL Joins.dbquery.ipynb" in SQL Editor
2. Ensure catalog is set to `workspace` and schema to `default`
3. Run statements 1-6 to create and populate all three tables
4. Run statement 7 (contains all 30 practice queries)
5. Each query in statement 7 is independent and tests different join concepts
6. Uncomment and run individual queries to practice specific join types

---

## 🔹 Query Summary

### By Join Type
- **Self-Joins**: Queries 1, 3, 17, 23, 24 (5 queries)
- **LEFT JOIN**: Queries 2, 5, 6, 11, 14, 15, 20, 21, 26, 28, 29, 30 (12 queries)
- **INNER JOIN**: Queries 7, 13, 18, 19 (4 queries)
- **Multiple Joins**: Queries 17, 21, 29 (3 queries)
- **NULL Handling**: Queries 5, 14, 21, 27 (4 queries)
- **Aggregation**: Queries 22, 25 (2 queries)

### By Complexity
| Difficulty | Query Numbers | Count |
|------------|---------------|-------|
| Easy | 1-8 | 8 |
| Medium | 11, 13-20 | 9 |
| Hard | 21-30 | 10 |

**Total Queries**: 30  
**Topics Covered**: INNER JOIN, LEFT JOIN, Self-Join, Multiple Joins, NULL Handling, Aggregation  
**Use Cases**: Employee hierarchy, Department analysis, Project assignments

---

## 🔹 Real-World Applications

- **Employee Management Systems**: Track reporting relationships (manager-employee)
- **HR Analytics**: Analyze department distributions and staffing
- **Project Management**: Monitor project assignments and workload
- **Data Quality**: Identify missing relationships (employees without departments)
- **Organizational Charts**: Build hierarchical structures using self-joins
- **Resource Planning**: Count employees per department including empty departments


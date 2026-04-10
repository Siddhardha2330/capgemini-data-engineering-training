# SQL Assignment – Student Submission Analysis

## 🔹 Objective

* Master SQL JOINs (INNER, LEFT, LEFT ANTI) for data reconciliation
* Learn data normalization using views for reusable transformations
* Implement email matching logic using unified mapping tables
* Build efficient queries following DRY (Don't Repeat Yourself) principles
* Identify data quality issues (duplicates, invalid records, missing submissions)

---

## 🔹 Problem Summary

* Given student master data (57 students) with dual email addresses (college + personal)
* Given task submission data (60 records) with unknown email types
* Required to:
  * Match submissions to students using flexible email matching
  * Identify students who haven't submitted
  * Detect invalid submissions from unrecognized emails
  * Find duplicate submissions from the same student
  * Generate classification report for all students

---

## 🔹 Approach

**Phase 1: Data Preparation**
* Created unified email mapping view combining personal and college emails
* Normalized submission emails in a separate view
* Applied LOWER() and TRIM() transformations once for reusability
* Eliminated repetitive OR conditions and function calls

**Phase 2: Core Analysis**
* Used LEFT ANTI JOIN to find students who haven't submitted
* Applied INNER JOIN to identify valid submissions from registered students
* Implemented LEFT ANTI JOIN to detect invalid/orphaned submissions
* Leveraged normalized views for clean, simple joins

**Phase 3: Data Quality**
* Cross-referenced submission emails with student master list
* Validated referential integrity between tables
* Identified data entry errors and unrecognized emails
* Generated actionable insights for program coordinators

---

## 🔹 Tasks Implemented

### Data Loading
* **student table** → Master list (57 students) with dual emails
* **response table** → Raw submissions (151 records) - not used in core analysis
* **submission table** → Actual submissions to analyze (60 records)

### Phase 1: Data Preparation
* **student_email_mapping view** → Unified email lookup
* **submission_normalized view** → Normalized submission emails

### Phase 2: Core Analysis
* Query 1 → Students who have NOT submitted  
* Query 2 → Valid submissions  
* Query 3 → Invalid submissions  

---

## 🔹 Key Concepts Used

* LOWER(), TRIM() for cleaning  
* INNER JOIN, LEFT ANTI JOIN  
* UNION ALL  
* GROUP BY  
* WINDOW FUNCTIONS (ROW_NUMBER)  

---

## 🔹 Phase 3: Duplicate Detection

* Identified duplicates using ROW_NUMBER  
* Kept first submission, marked others as duplicate  
* Compared GROUP BY vs WINDOW functions  

---

## 🔹 Phase 4: Advanced Insights

* Count submissions per student  
* Find students using both emails  
* Final classification (Submitted / Duplicate / Not Submitted / Invalid)  

---

## 🔹 Additional Practice: NULL Handling

Practiced NULL handling using different tables.

### Tables Used

* Employees  
* Orders  
* Products  

---

### Concepts Practiced

* IS NULL / IS NOT NULL  
* COALESCE  
* NULLIF  
* Handling NULL in calculations  

---

### Key Learnings

* NULL is not 0  
* COUNT(column) ignores NULL  
* Always handle NULL before calculations  
* COALESCE is useful in real queries  

---

## 🔹 Project Structure

* README.md  
* SQL_practice_on_real_data.sql  
* SQL_null_handling_practice.sql  
* Problem Statement PDFs  

---

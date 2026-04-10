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
* Applied `multiLine => true` for multi-line CSV column headers
* Normalized column names from CSV headers to snake_case

### Phase 1: Data Preparation
* **student_email_mapping view** → Unified email lookup (114 email mappings)
  * Combined personal + college emails using UNION ALL
  * Applied LOWER(TRIM()) normalization
  * Added email_type tracking (personal/college)
* **submission_normalized view** → Normalized submission emails once
  * Eliminated repeated LOWER(TRIM()) calls in queries
  * Enabled clean joins without function overhead

### Phase 2: Core Analysis
* **Query 1: Not Submitted** → Students without any submission (2 students)
* **Query 2: Valid Submissions** → Matched submissions (56 valid records)
* **Query 3: Invalid Submissions** → Unrecognized emails (4 invalid records)

---

## 🔹 Key Transformations Used

**Data Normalization**
* `LOWER()` → Convert emails to lowercase
* `TRIM()` → Remove leading/trailing whitespace
* `UNION ALL` → Combine personal + college emails

**JOIN Operations**
* `INNER JOIN` → Match submissions to students (valid records only)
* `LEFT ANTI JOIN` → Find non-matches (not submitted, invalid emails)
* Multi-step joins through intermediate mapping views

**Data Loading**
* `read_files()` → Read CSV files from Databricks Volumes
* `multiLine => true` → Handle multi-line column headers
* Column aliasing with backticks for spaces/special characters

**View Creation**
* `CREATE OR REPLACE VIEW` → Reusable transformations
* Virtual tables without data duplication
* Centralized normalization logic

---

## 🔹 Output

**Output 1: Email Mapping (114 records)**
* Each student mapped to 2 emails (personal + college)
* Enables flexible matching regardless of which email students use
* Example:
  | student_id | student_name | email | email_type |
  |------------|-------------|-------|------------|
  | 22PA1A1275 | John Doe | john@gmail.com | personal |
  | 22PA1A1275 | John Doe | 22pa1a1275@vishnu.edu.in | college |

**Output 2: Valid Submissions (56 records)**
* Submissions matched to registered students
* Shows which email type was used (personal/college)
* Includes full student details (regno, branch, CGPA, etc.)

**Output 3: Invalid Submissions (4 records)**
* Unrecognized emails not in master student list:
  * `22pa1a4235@vishnu.edu.in` (typo or non-existent student)
  * `22pa1a4244@vishnu.edu.in` (not in master list)
  * `22pa1a42b4@vishnu.edu.in` (invalid registration number format)
  * `varmagadiraju0318@gmail.com` (personal email not registered)

**Output 4: Not Submitted (2 students)**
* Students from master list with no matching submissions
* Sorted by CGPA descending (prioritize high performers for follow-up)

---

## 🔹 Data Engineering Considerations

* **Normalized emails once** in Phase 1 views instead of repeating in every query
* **Avoided OR conditions** by creating unified email mapping table
* **Used views instead of tables** for transformation logic (no data duplication)
* **Applied LEFT ANTI JOIN** for efficient non-match queries (better than LEFT JOIN + IS NULL)
* **Validated data quality** before analysis (identified 4 invalid emails)
* **Preserved both email addresses** in output for contact/follow-up purposes
* **Used UNION ALL** instead of UNION (no need to remove duplicates, better performance)

---

## 🔹 Challenges Faced

* **Multiple email addresses per student** → Needed flexible matching strategy
* **Case sensitivity and whitespace** → Required email normalization
* **Repetitive OR conditions** → Created unified mapping to avoid code duplication
* **Multi-line CSV headers** → Required `multiLine => true` option
* **Column names with spaces** → Used backticks in SELECT statements
* **Invalid/unrecognized emails** → Used LEFT ANTI JOIN to identify orphaned records

---

## 🔹 Learnings

* **DRY Principle** → Centralize normalization logic in views for reusability
* **JOIN Types** → When to use INNER vs LEFT ANTI JOIN for different business questions
* **Data Normalization** → Apply transformations once at the source, not repeatedly
* **View Benefits** → Reusable transformation logic without data duplication
* **Email Matching** → Unified mapping table eliminates complex OR conditions
* **Data Quality** → Use anti-joins to identify orphaned/invalid records
* **Performance** → Computed functions (LOWER, TRIM) once instead of in every query
* **Maintainability** → Changes to normalization logic only need updating in one place

---

## 🔹 Project Structure

- README.md → Combined documentation
- SQL_practice_on_real_data.sql → Window function queries
- Student_Submission_Analysis_problem_statement.pdf → contains problem statement


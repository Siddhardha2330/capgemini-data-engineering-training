-- Databricks SQL Student Notebook (Fill-in Version)

-- STEP 0: LOAD DATA
CREATE OR REPLACE TABLE master_raw
USING CSV
OPTIONS (
  path "__________",
  header "true",
  inferSchema "true"
);

CREATE OR REPLACE TABLE task1_responses_raw
USING CSV
OPTIONS (
  path "__________",
  header "true",
  inferSchema "true"
);

CREATE OR REPLACE TABLE task1_file2_raw
USING CSV
OPTIONS (
  path "__________",
  header "true",
  inferSchema "true"
);

-- STEP 1: CLEANING
CREATE OR REPLACE TABLE master_clean AS
SELECT
    *,
    __________________________ AS college_email_clean,
    __________________________ AS personal_email_clean
FROM master_raw;

CREATE OR REPLACE TABLE task1_responses_clean AS
SELECT
    *,
    __________________________ AS email_clean
FROM task1_responses_raw;

CREATE OR REPLACE TABLE task1_file2_clean AS
SELECT
    *,
    __________________________ AS email_clean
FROM task1_file2_raw;

-- STEP 2: EMAIL MAPPING
CREATE OR REPLACE TABLE student_email_map AS
SELECT 
    __________ AS student_id,
    __________ AS email
FROM master_clean

UNION

SELECT 
    __________,
    __________
FROM master_clean;

-- STEP 3: MAP SUBMISSIONS
CREATE OR REPLACE TABLE responses_mapped AS
SELECT 
    r.*,
    __________ AS student_id
FROM task1_responses_clean r
LEFT JOIN student_email_map m
ON __________________________;

CREATE OR REPLACE TABLE file2_mapped AS
SELECT 
    f.*,
    __________ AS student_id
FROM task1_file2_clean f
LEFT JOIN student_email_map m
ON __________________________;

-- STEP 4: COMBINE
CREATE OR REPLACE VIEW all_submissions AS
SELECT 
    __________,
    __________,
    'task1_responses' AS source
FROM responses_mapped

UNION ALL

SELECT 
    __________,
    __________,
    'task1_file2' AS source
FROM file2_mapped;

-- STEP 5: WINDOW FUNCTION
CREATE OR REPLACE VIEW submissions_with_rank AS
SELECT *,
       __________________________ AS rn
FROM all_submissions;

-- STEP 6: FINAL CLASSIFICATION
CREATE OR REPLACE TABLE final_student_status AS
SELECT 
    m.student_id,
    CASE 
        WHEN __________________ THEN 'NOT_SUBMITTED'
        WHEN __________________ THEN 'VALID_SUBMISSION'
        ELSE 'DUPLICATE'
    END AS status
FROM master_clean m
LEFT JOIN submissions_with_rank s
ON __________________________;

-- ANALYSIS QUESTIONS

-- 1. Count of each category
-- write query

-- 2. Invalid submissions
-- write query

-- 3. Students using both emails
-- write query

-- 4. GROUP BY vs WINDOW FUNCTION comparison
-- write query

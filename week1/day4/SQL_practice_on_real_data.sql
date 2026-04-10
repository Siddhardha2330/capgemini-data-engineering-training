
-- DATA LOADING (Pre-requisite)


CREATE OR REPLACE TABLE student AS
SELECT 
  `S.No` as sno,
  `Superset ID` as superset_id,
  REGNO as regno,
  `STUDENT NAME` as student_name,
  GENDER as gender,
  Branch as branch,
  `Tenth Percentage` as tenth_percentage,
  `Inter Percentage` as inter_percentage,
  `B.Tech CGPA` as btech_cgpa,
  Backlogs as backlogs,
  MOBILE as mobile,
  `College Domain MAILID` as college_email,
  `Personal Mail ID` as personal_email
FROM read_files(
  '/Volumes/workspace/default/collegedata/Capgemini Databricks VITB-2026 (1).csv',
  format => 'csv',
  header => true
);

CREATE OR REPLACE TABLE response AS
SELECT 
  Timestamp as timestamp,
  `Email address` as email,
  `What are the various methods you learnt in pyspark?  Continue to explore pyspark through https://www.sparkplayground.com/tutorials/pyspark` as pyspark_methods_learned,
  `Have you completed all the 5 queries? ` as completed_all_queries,
  TRIM(`  GitHub Username  `) as github_username,
  `Repository Link 
Example:
https://github.com/username/databricks-week0-foundation` as repository_link,
  TRIM(`  Phase 1 Folder Link  `) as phase1_folder_link
FROM read_files(
  '/Volumes/workspace/default/collegedata/Task 1 (Responses).csv',
  format => 'csv',
  header => true,
  multiLine => true
);

CREATE OR REPLACE TABLE submission AS
SELECT 
  Timestamp as timestamp,
  `Email address` as email,
  `What are the various methods you learnt in pyspark?  Continue to explore pyspark through https://www.sparkplayground.com/tutorials/pyspark` as pyspark_methods_learned,
  `Have you completed all the 5 queries? ` as completed_all_queries,
  TRIM(`  GitHub Username  `) as github_username,
  `Repository Link 
Example:
https://github.com/username/databricks-week0-foundation` as repository_link,
  TRIM(`  Phase 1 Folder Link  `) as phase1_folder_link
FROM read_files(
  '/Volumes/workspace/default/collegedata/Task 1 file 2.csv',
  format => 'csv',
  header => true,
  multiLine => true
);


-- Phase 1: Data Preparation


-- 1. Normalize emails (lowercase, trim spaces)

CREATE OR REPLACE VIEW student_email_mapping AS
SELECT 
  regno AS student_id,
  student_name,
  LOWER(TRIM(personal_email)) AS email,
  'personal' AS email_type
FROM student
WHERE personal_email IS NOT NULL

UNION ALL

SELECT 
  regno AS student_id,
  student_name,
  LOWER(TRIM(college_email)) AS email,
  'college' AS email_type
FROM student
WHERE college_email IS NOT NULL;


-- 2. Create normalized submission view (normalize emails once!)
CREATE OR REPLACE VIEW submission_normalized AS
SELECT 
  timestamp,
  LOWER(TRIM(email)) AS email,
  pyspark_methods_learned,
  completed_all_queries,
  github_username,
  repository_link,
  phase1_folder_link
FROM submission;



-- Phase 2: Core Analysis


-- 1. Find students who have NOT submitted (LEFT JOIN)
SELECT 
  s.regno,
  s.student_name,
  s.branch,
  s.btech_cgpa,
  s.college_email,
  s.personal_email
FROM student s
LEFT ANTI JOIN (
  SELECT DISTINCT sem.student_id
  FROM student_email_mapping sem
  INNER JOIN submission_normalized sub
    ON sem.email = sub.email
) submitted
  ON s.regno = submitted.student_id
ORDER BY s.btech_cgpa DESC;


-- 2. Find valid submissions (INNER JOIN with master)
SELECT 
  s.regno,
  s.student_name,
  s.branch,
  sub.email,
  sub.timestamp,
  sub.github_username,
  sub.completed_all_queries,
  sem.email_type
FROM submission_normalized sub
INNER JOIN student_email_mapping sem
  ON sub.email = sem.email
INNER JOIN student s
  ON sem.student_id = s.regno
ORDER BY sub.timestamp DESC;


-- 3. Find invalid submissions (emails not in master)
SELECT 
  sub.email,
  sub.github_username,
  sub.repository_link,
  sub.timestamp,
  sub.completed_all_queries
FROM submission_normalized sub
LEFT ANTI JOIN student_email_mapping sem
  ON sub.email = sem.email
ORDER BY sub.timestamp DESC;


-- Phase 3: Duplicate Detection

-- 1. Identify duplicates

select sem.student_id,
sem.student_name,
sub.email,
sub.timestamp,
row_number() over (
partition by sem.student_id 
order by sub.timestamp
) as rn
from submission sub
join student_email_mapping sem
on lower(trim(sub.email)) = sem.email
order by sem.student_id,sub.timestamp;



-- 2. Keep first submission, mark duplicates

select student_id,
student_name,
email,
timestamp,
case 
when rn=1 then 'First'
else 'Duplicate'
end as status
from (

select sem.student_id,
sem.student_name,
sub.email,
sub.timestamp,
row_number() over (
partition by sem.student_id 
order by sub.timestamp
) as rn
from submission sub
join student_email_mapping sem
on lower(trim(sub.email)) = sem.email

) t
order by student_id,timestamp;



-- 3. GROUP BY vs WINDOW FUNCTION


-- GROUP BY approach (summary only)
select sem.student_id,
sem.student_name,
count(sub.email) as submission_count,
min(sub.timestamp) as first_submission,
max(sub.timestamp) as last_submission
from submission sub
join student_email_mapping sem
on lower(trim(sub.email)) = sem.email
group by sem.student_id,sem.student_name
having count(sub.email)>1
order by submission_count desc;



-- WINDOW FUNCTION approach (keeps all rows)

select sem.student_id,
sem.student_name,
sub.email,
sub.timestamp,
row_number() over (
partition by sem.student_id 
order by sub.timestamp
) as rn,
count(*) over (
partition by sem.student_id
) as total_submissions
from submission sub
join student_email_mapping sem
on lower(trim(sub.email)) = sem.email
order by sem.student_id,sub.timestamp;


-- Phase 4: Advanced Insights


-- 1. Count submissions per student
select s.regno,
s.student_name,
count(sub.email) as submission_count,
max(sub.timestamp) as last_submission_time
from student s
left join student_email_mapping sem 
on s.regno = sem.student_id
left join submission_normalized sub 
on sem.email = sub.email
group by s.regno,s.student_name
order by submission_count desc;



-- 2. Find students using both emails
select student_id,
student_name
from student_email_mapping
where email in (select email from submission_normalized)
group by student_id,student_name
having count(distinct email_type)=2
order by student_id;



-- 3. Final classification

with student_count as (
select s.regno,
s.student_name,
count(sub.email) as submission_count
from student s
left join student_email_mapping sem 
on s.regno = sem.student_id
left join submission_normalized sub 
on sem.email = sub.email
group by s.regno,s.student_name
)

select regno,
student_name,
submission_count,
case 
when submission_count=0 then 'Not Submitted'
when submission_count=1 then 'Submitted'
else 'Duplicate'
end as status
from student_count

union all

select null,
'Unknown',
count(*) as submission_count,
'Invalid'
from submission_normalized sub
left anti join student_email_mapping sem 
on sub.email = sem.email;

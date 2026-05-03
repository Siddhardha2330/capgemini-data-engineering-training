-- 1. No-Show Rate by Department
SELECT * FROM health_cat.gold.gold_no_show_rate ORDER BY no_show_rate_pct DESC;

-- 2. Doctor Utilization
SELECT * FROM health_cat.gold.gold_doctor_utilization ORDER BY total_appointments DESC;

-- 3. Department Revenue
SELECT * FROM health_cat.gold.gold_department_revenue ORDER BY total_revenue DESC;

-- 4. Wait Time Trends
SELECT * FROM health_cat.gold.gold_wait_time_trends ORDER BY visit_month, avg_wait_time_minutes DESC;

-- 5. Patient Revisit Rate
SELECT * FROM health_cat.gold.gold_patient_revisit_rate ORDER BY visit_count DESC;

-- 6. Diagnostics Volume
SELECT * FROM health_cat.gold.gold_diagnostics_volume ORDER BY test_count DESC;

-- 7. Feedback Summary
SELECT * FROM health_cat.gold.gold_feedback_summary;

-- 8. Kaggle No-Show by Age
SELECT * FROM health_cat.gold.gold_kaggle_no_show_by_age ORDER BY no_show_rate_pct DESC;

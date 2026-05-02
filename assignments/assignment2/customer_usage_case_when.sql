CREATE OR REPLACE TABLE CustomerUsage (
customer_id INT PRIMARY KEY,
customer_name VARCHAR(50),
plan_type VARCHAR(20),
data_used_gb INT,
call_minutes INT,
monthly_bill DECIMAL(10,2)
);

INSERT INTO CustomerUsage VALUES
(1, 'Veena', 'Postpaid', 9, 471, 842.03),
(2, 'Veer', 'Postpaid', 7, 354, 434.94),
(3, 'Mohan', 'Prepaid', 33, 497, 569.23),
(4, 'Pratik', 'Prepaid', 15, 113, 1107.48),
(5, 'Farhan', 'Prepaid', 14, 1284, 845.75),
(6, 'Jiya', 'Postpaid', 28, 1578, 1325.15);

-- Question 1
SELECT *, CASE
    WHEN plan_type = 'Prepaid' AND (monthly_bill > 400 OR call_minutes > 550) THEN 'Suggest Postpaid Upgrade'
    WHEN plan_type = 'Prepaid' THEN 'No Upgrade'
    WHEN plan_type = 'Postpaid' AND data_used_gb > 25 THEN 'Suggest Premium Plan'
    ELSE 'Satisfactory Usage'
END AS upgrade_suggestion
FROM CustomerUsage;

-- Question 2
SELECT *, CASE
    WHEN plan_type = 'Postpaid' AND data_used_gb >= 30 THEN 'Heavy User - Postpaid'
    WHEN plan_type = 'Postpaid' AND data_used_gb BETWEEN 15 AND 29 THEN 'Moderate User - Postpaid'
    WHEN plan_type = 'Postpaid' THEN 'Light User - Postpaid'
    WHEN plan_type = 'Prepaid' AND data_used_gb >= 20 THEN 'Heavy User - Prepaid'
    ELSE 'Light User - Prepaid'
END AS usage_classification
FROM CustomerUsage;

-- Question 3
SELECT *, CASE
    WHEN call_minutes > 1200 AND monthly_bill > 1000 THEN 'Excessive Talker - Premium'
    WHEN call_minutes > 1200 THEN 'Excessive Talker - Standard'
    WHEN call_minutes BETWEEN 800 AND 1200 THEN 'Frequent Caller'
    ELSE 'Normal Caller'
END AS voice_pattern
FROM CustomerUsage;

-- Question 4
SELECT *, CASE
    WHEN plan_type = 'Postpaid' AND data_used_gb > 25 AND call_minutes > 1000 THEN 'Combo Offer'
    WHEN plan_type = 'Prepaid' AND monthly_bill > 600 THEN 'Recharge Cashback Offer'
    ELSE 'No Offer'
END AS offer_eligibility
FROM CustomerUsage;

-- Question 5
SELECT *, CASE
    WHEN data_used_gb > 25 AND call_minutes > 1000 THEN 'Gold Tier'
    WHEN data_used_gb > 25 THEN 'Silver Tier'
    WHEN call_minutes > 800 THEN 'Bronze Tier'
    ELSE 'Basic Tier'
END AS reward_tier
FROM CustomerUsage;

-- Question 6
SELECT *, CASE
    WHEN plan_type = 'Postpaid' AND monthly_bill < 500 THEN 'Underutilized Postpaid'
    WHEN plan_type = 'Prepaid' AND monthly_bill > 900 THEN 'Overpaying Prepaid'
    ELSE 'Plan Matches Usage'
END AS usage_mismatch
FROM CustomerUsage;

-- Question 7
SELECT *, CASE
    WHEN data_used_gb > 25 AND call_minutes > 1000 AND monthly_bill > 1200 THEN 'Super User'
    WHEN data_used_gb > 25 AND call_minutes > 1000 THEN 'High Usage Customer'
    ELSE 'Standard User'
END AS all_round_usage
FROM CustomerUsage;

-- Question 8
SELECT *, CASE
    WHEN call_minutes > data_used_gb * 30 THEN 'Voice Oriented User'
    WHEN data_used_gb > call_minutes / 30.0 THEN 'Data Oriented User'
    ELSE 'Balanced Usage'
END AS usage_preference
FROM CustomerUsage;

-- Question 9
SELECT *, CASE
    WHEN monthly_bill > 1000 AND plan_type = 'Prepaid' THEN 'Offer Switch + Discount'
    WHEN monthly_bill > 1000 AND plan_type = 'Postpaid' THEN 'Offer Loyalty Discount'
    ELSE 'No Discount'
END AS discount_suggestion
FROM CustomerUsage;

-- Question 10
SELECT *, CASE
    WHEN data_used_gb > 25 AND monthly_bill < 700 THEN 'High Efficiency User'
    WHEN data_used_gb > 25 THEN 'Premium Digital User'
    ELSE 'Standard Engagement'
END AS digital_engagement
FROM CustomerUsage;

-- Question 11
SELECT *, CASE
    WHEN call_minutes > 1500 AND data_used_gb < 10 THEN 'Heavy Voice, Low Data'
    WHEN data_used_gb > 25 AND call_minutes < 500 THEN 'Heavy Data, Light Talk'
    ELSE 'Balanced or Low Usage'
END AS mix_pattern
FROM CustomerUsage;

-- Question 12
SELECT *, CASE
    WHEN plan_type = 'Postpaid' AND monthly_bill > 800 AND call_minutes > 1200 THEN 'Elite Postpaid User'
    WHEN plan_type = 'Postpaid' AND monthly_bill > 800 THEN 'Premium Postpaid User'
    ELSE 'Prepaid User'
END AS engagement_tag
FROM CustomerUsage;

-- Question 13
SELECT *, CASE
    WHEN data_used_gb > 30 THEN 'Platinum Data Pack Suggested'
    WHEN data_used_gb > 20 AND monthly_bill BETWEEN 800 AND 1000 THEN 'Gold Data Pack Suggested'
    ELSE 'Standard Plan'
END AS data_pack_suggestion
FROM CustomerUsage;

-- Question 14
SELECT *, CASE
    WHEN call_minutes > 1000 AND monthly_bill < 700 THEN 'Value Talker'
    WHEN call_minutes > 1000 THEN 'Premium Talker'
    ELSE 'Normal Caller'
END AS talker_segment
FROM CustomerUsage;

-- Question 15
SELECT *, CASE
    WHEN plan_type = 'Prepaid' AND data_used_gb > 25 THEN 'Prepaid Power User'
    WHEN plan_type = 'Postpaid' AND data_used_gb > 25 THEN 'Postpaid Power User'
    ELSE 'Normal User'
END AS power_user_tag
FROM CustomerUsage;
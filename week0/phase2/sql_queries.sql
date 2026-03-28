-- =============================================================================
-- Phase 2 — Starter SQL (customers)
-- Run in your SQL client or Spark SQL after registering the table as needed.
-- =============================================================================

CREATE TABLE customers (
  customer_id INT,
  customer_name VARCHAR(50),
  city VARCHAR(50),
  age INT
);

INSERT INTO customers VALUES
  (1, 'Ravi', 'Hyderabad', 25),
  (2, 'Sita', 'Chennai', 32),
  (3, 'Arun', 'Hyderabad', 28);

-- -----------------------------------------------------------------------------
-- Guided exercises — write your queries below each prompt
-- -----------------------------------------------------------------------------

-- 1. Show all customers
-- SELECT * FROM customers;

-- 2. Show customers from Chennai
-- SELECT * FROM customers WHERE city = 'Chennai';

-- 3. Show customers with age > 25
-- SELECT * FROM customers WHERE age > 25;

-- 4. Show only customer_name and city
-- SELECT customer_name, city FROM customers;

-- 5. Count customers city-wise
-- SELECT city, COUNT(*) AS customer_count FROM customers GROUP BY city;

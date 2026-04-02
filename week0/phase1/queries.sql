-- Phase 1 — SQL Basics 

-- -----------------------------
-- Create Table
-- -----------------------------
CREATE TABLE customers (
    customer_id INT,
    customer_name VARCHAR(50),
    city VARCHAR(50),
    age INT
);

-- -----------------------------
-- Insert Data
-- -----------------------------
INSERT INTO customers VALUES
    (1, 'Ravi', 'Hyderabad', 25),
    (2, 'Sita', 'Chennai', 32),
    (3, 'Arun', 'Hyderabad', 28);

-- -----------------------------
-- Query 1: Show All Customers
-- -----------------------------
SELECT *
FROM customers;

-- -----------------------------
-- Query 2: Customers from Chennai
-- -----------------------------
SELECT *
FROM customers
WHERE city = 'Chennai';

-- -----------------------------
-- Query 3: Customers with Age > 25
-- -----------------------------
SELECT *
FROM customers
WHERE age > 25;

-- -----------------------------
-- Query 4: Select Name and City
-- -----------------------------
SELECT customer_name, city
FROM customers;

-- -----------------------------
-- Query 5: City-wise Customer Count
-- -----------------------------
SELECT city, COUNT(*) AS customer_count
FROM customers
GROUP BY city;

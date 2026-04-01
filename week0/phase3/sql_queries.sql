CREATE TABLE customers (
    customer_id INT,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(150),
    phone_number VARCHAR(20),
    address VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20)
);

CREATE TABLE sales (
    sale_id INT,
    customer_id INT,
    product_id INT,
    sale_date DATE,
    quantity INT,
    total_amount DOUBLE
);


INSERT INTO customers VALUES
(1,'John','Smith','john.smith@domain.com','555-0001','123 Elm St','Springfield','IL','62701'),
(2,'Emma','Jones','emma.jones@webmail.com','555-0002','456 Oak St','Centerville','OH','45459'),
(3,'Olivia','Brown','olivia.brown@outlook.com','555-0003','789 Pine St','Greenville','SC','29601'),
(4,'Liam','Johnson','liam.johnson@mail.com','555-0004','321 Maple St','Austin','TX','73301'),
(5,'Noah','Williams','noah.williams@mail.com','555-0005','654 Cedar St','Dallas','TX','75001');


INSERT INTO sales VALUES
(1,1,101,'2024-01-15',2,39.98),
(2,1,102,'2024-01-20',1,29.99),
(3,2,103,'2024-01-16',1,25.00),
(4,2,104,'2024-01-22',3,89.97),
(5,3,105,'2024-01-17',2,49.98),
(6,1,106,'2024-01-25',1,19.99),
(7,4,107,'2024-01-18',2,59.98),
(8,5,108,'2024-01-19',1,15.00),
(9,5,109,'2024-01-21',2,30.00),
(10,5,110,'2024-01-23',1,20.00);


-- ------------------------------------------------------------
-- 1. Daily Sales
-- ------------------------------------------------------------
SELECT sale_date, ROUND(SUM(total_amount),2) AS daily_sales
FROM sales
WHERE total_amount IS NOT NULL
GROUP BY sale_date;


-- ------------------------------------------------------------
-- 2. City-wise Revenue
-- ------------------------------------------------------------
SELECT c.city, ROUND(SUM(s.total_amount),2) AS city_revenue
FROM customers c
JOIN sales s
ON c.customer_id = s.customer_id
GROUP BY c.city;


-- ------------------------------------------------------------
-- 3. Repeat Customers (>2 orders)
-- ------------------------------------------------------------
SELECT customer_id, COUNT(*) AS order_count
FROM sales
GROUP BY customer_id
HAVING COUNT(*) > 2;


-- ------------------------------------------------------------
-- 4. Highest spending customer in each city (FIXED)
-- ------------------------------------------------------------
SELECT city, customer_id, total_spent
FROM (
    SELECT c.city, s.customer_id,
           SUM(s.total_amount) AS total_spent,
           RANK() OVER (PARTITION BY c.city ORDER BY SUM(s.total_amount) DESC) AS rnk
    FROM customers c
    JOIN sales s
        ON c.customer_id = s.customer_id
    GROUP BY c.city, s.customer_id
) t
WHERE rnk = 1;


-- ------------------------------------------------------------
-- 5. Final Reporting Table
-- ------------------------------------------------------------
SELECT c.customer_id, c.city,
       ROUND(SUM(s.total_amount),2) AS total_spent,
       COUNT(*) AS order_count
FROM customers c
JOIN sales s
ON c.customer_id = s.customer_id
GROUP BY c.customer_id, c.city;

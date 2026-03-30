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
(3,'Olivia','Brown','olivia.brown@outlook.com','555-0003','789 Pine St','Greenville','SC','29601');

INSERT INTO sales VALUES
(1,1,1,'2024-01-15',2,39.98),
(2,1,3,'2024-01-20',1,29.99),
(3,2,2,'2024-01-16',1,25.0),
(4,2,4,'2024-01-22',3,89.97),
(5,3,5,'2024-01-17',2,49.98);


-- 1. Total order amount for each customer
SELECT customer_id,ROUND(SUM(total_amount),2) AS total_spent
FROM sales
GROUP BY customer_id;


-- 2. Top 3 customers by total spend
SELECT customer_id,ROUND(SUM(total_amount),2) AS total_spent
FROM sales
GROUP BY customer_id
ORDER BY total_spent DESC
LIMIT 3;


-- 3. Customers with no orders
SELECT c.*
FROM customers c
LEFT JOIN sales s
ON c.customer_id=s.customer_id
WHERE s.customer_id IS NULL;


-- 4. City-wise total revenue
SELECT c.city,ROUND(SUM(s.total_amount),2) AS total_revenue
FROM customers c
JOIN sales s
ON c.customer_id=s.customer_id
GROUP BY c.city;


-- 5. Average order amount per customer
SELECT customer_id,ROUND(AVG(total_amount),2) AS avg_order_amount
FROM sales
GROUP BY customer_id;


-- 6. Customers with more than one order
SELECT customer_id,COUNT(*) AS order_count
FROM sales
GROUP BY customer_id
HAVING COUNT(*)>1;


-- 7. Sort customers by total spend descending
SELECT customer_id,ROUND(SUM(total_amount),2) AS total_spent
FROM sales
GROUP BY customer_id
ORDER BY total_spent DESC;

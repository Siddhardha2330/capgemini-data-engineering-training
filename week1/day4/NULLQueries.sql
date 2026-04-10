
-- NULL HANDLING PRACTICE


-- TABLE 1: Employees

CREATE TABLE Employees (
emp_id INT,
name VARCHAR(50),
salary INT,
bonus INT,
manager_id INT
);

INSERT INTO Employees VALUES
(1, 'Amit', 50000, NULL, 101),
(2, 'John', NULL, 5000, 102),
(3, 'Sara', 60000, NULL, NULL),
(4, 'David', NULL, NULL, 103),
(5, 'Priya', 45000, 3000, 101),
(6, 'Kiran', NULL, NULL, NULL),
(7, 'Ravi', 70000, 7000, 102),
(8, 'Neha', NULL, 2000, NULL);

-- TABLE 2: Orders
CREATE TABLE Orders (
order_id INT,
customer_name VARCHAR(50),
amount INT,
discount INT,
coupon_code VARCHAR(20)
);

INSERT INTO Orders VALUES
(101, 'Amit', 1000, NULL, 'DISC10'),
(102, 'John', NULL, 50, NULL),
(103, 'Sara', 2000, NULL, 'DISC20'),
(104, 'David', NULL, NULL, NULL),
(105, 'Priya', 1500, 100, NULL),
(106, 'Kiran', NULL, NULL, 'DISC5'),
(107, 'Ravi', 3000, NULL, NULL),
(108, 'Neha', NULL, 200, 'DISC15');

-- TABLE 3: Products

CREATE TABLE Products (
product_id INT,
product_name VARCHAR(50),
price INT,
category VARCHAR(50),
stock INT
);

INSERT INTO Products VALUES
(1, 'Laptop', 50000, 'Electronics', 10),
(2, 'Phone', NULL, 'Electronics', NULL),
(3, 'Tablet', 30000, NULL, 5),
(4, 'Headphones', NULL, NULL, NULL),
(5, 'Monitor', 20000, 'Electronics', 0),
(6, 'Keyboard', NULL, 'Accessories', 15),
(7, 'Mouse', 500, NULL, NULL),
(8, 'Printer', NULL, 'Electronics', 3);


-- LEVEL 1 (BASIC)

-- 1. Employees with NULL salary
SELECT * FROM Employees WHERE salary IS NULL;

-- 2. Orders with NOT NULL discount
SELECT * FROM Orders WHERE discount IS NOT NULL;

-- 3. Products with NULL category
SELECT * FROM Products WHERE category IS NULL;

-- 4. Count employees with NULL manager
SELECT COUNT(*) FROM Employees WHERE manager_id IS NULL;


-- LEVEL 2 (ISNULL / COALESCE)


-- 5. Replace NULL salary with 0
SELECT name, COALESCE(salary, 0) AS salary FROM Employees;

-- 6. Replace NULL bonus with 1000
SELECT name, COALESCE(bonus, 1000) AS bonus FROM Employees;

-- 7. Replace NULL order amount with 500
SELECT customer_name, COALESCE(amount, 500) AS amount FROM Orders;

-- 8. Replace NULL stock with 0
SELECT product_name, COALESCE(stock, 0) AS stock FROM Products;


-- LEVEL 3 (COALESCE)


-- 9. Employee earnings (salary else bonus)
SELECT name, COALESCE(salary, bonus) AS earnings FROM Employees;

-- 10. First available value (salary → bonus → 0)
SELECT name, COALESCE(salary, bonus, 0) AS income FROM Employees;

-- 11. Product price default 1000
SELECT product_name, COALESCE(price, 1000) AS price FROM Products;

-- 12. Customer payment (amount → discount → 0)
SELECT customer_name, COALESCE(amount, discount, 0) AS payment FROM Orders;

-- =========================================
-- LEVEL 4 (NULLIF)
-- =========================================

-- 13. Salary NULL if 0
SELECT name, NULLIF(salary, 0) AS salary FROM Employees;

-- 14. Discount NULL if 0
SELECT order_id, NULLIF(discount, 0) AS discount FROM Orders;

-- 15. Avoid divide by zero
SELECT order_id, amount / NULLIF(discount, 0) AS result FROM Orders;

-- 16. Replace 'DISC10' with NULL
SELECT order_id, NULLIF(coupon_code, 'DISC10') AS coupon_code FROM Orders;


-- LEVEL 5 (REAL-TIME)


-- 17. Total earnings (handle NULL)
SELECT name,
COALESCE(salary,0) + COALESCE(bonus,0) AS total_income
FROM Employees;

-- 18. Employees with both salary and bonus NULL
SELECT * FROM Employees
WHERE salary IS NULL AND bonus IS NULL;

-- 19. Products where price NULL but category NOT NULL
SELECT * FROM Products
WHERE price IS NULL AND category IS NOT NULL;

-- 20. Orders where amount and discount both NULL
SELECT * FROM Orders
WHERE amount IS NULL AND discount IS NULL;


-- LEVEL 6 (ADVANCED)

-- 21. Employee income with default 1000
SELECT name, COALESCE(salary, bonus, 1000) AS income FROM Employees;

-- 22. Replace discount 0 with NULL
SELECT order_id, NULLIF(discount, 0) AS discount FROM Orders;

-- 23. Final payable amount
SELECT order_id,
COALESCE(amount,0) - COALESCE(discount,0) AS final_amount
FROM Orders;

-- 24. Employees where salary NULL but manager exists
SELECT * FROM Employees
WHERE salary IS NULL AND manager_id IS NOT NULL;


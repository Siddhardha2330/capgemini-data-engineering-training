-- Dashboard analytics queries for the FMCG consolidated pipeline

-- 1. Total sold quantity by month
SELECT
    date,
    SUM(sold_quantity) AS total_sold_quantity
FROM fmcg.gold.fact_orders
GROUP BY date
ORDER BY date;

-- 2. Top 10 products by sold quantity
SELECT
    product_code,
    SUM(sold_quantity) AS total_sold_quantity
FROM fmcg.gold.sb_fact_orders
GROUP BY product_code
ORDER BY total_sold_quantity DESC
LIMIT 10;

-- 3. Top 10 customers by sold quantity
SELECT
    customer_code,
    SUM(sold_quantity) AS total_sold_quantity
FROM fmcg.gold.sb_fact_orders
GROUP BY customer_code
ORDER BY total_sold_quantity DESC
LIMIT 10;

-- 4. Product performance by month
SELECT
    date,
    product_code,
    SUM(sold_quantity) AS monthly_quantity
FROM fmcg.gold.fact_orders
GROUP BY date, product_code
ORDER BY date, monthly_quantity DESC;

-- 5. Customer purchase trend by month
SELECT
    date,
    customer_code,
    SUM(sold_quantity) AS monthly_quantity
FROM fmcg.gold.fact_orders
GROUP BY date, customer_code
ORDER BY date, monthly_quantity DESC;

-- 6. Orders joined with customer dimension
SELECT
    f.date,
    f.customer_code,
    c.customer_name,
    c.market,
    c.platform,
    SUM(f.sold_quantity) AS total_sold_quantity
FROM fmcg.gold.fact_orders f
LEFT JOIN fmcg.gold.dim_customers c
    ON f.customer_code = c.customer_id
GROUP BY f.date, f.customer_code, c.customer_name, c.market, c.platform
ORDER BY f.date, total_sold_quantity DESC;

-- 7. Orders joined with product dimension
SELECT
    f.date,
    f.product_code,
    p.product,
    p.category,
    p.division,
    SUM(f.sold_quantity) AS total_sold_quantity
FROM fmcg.gold.fact_orders f
LEFT JOIN fmcg.gold.dim_products p
    ON f.product_code = p.product_code
GROUP BY f.date, f.product_code, p.product, p.category, p.division
ORDER BY f.date, total_sold_quantity DESC;

-- 8. Revenue estimate using latest gross price
SELECT
    s.date,
    s.product_code,
    p.price_inr,
    SUM(s.sold_quantity) AS total_sold_quantity,
    SUM(s.sold_quantity * p.price_inr) AS estimated_revenue
FROM fmcg.gold.sb_fact_orders s
LEFT JOIN fmcg.gold.dim_gross_price p
    ON s.product_code = p.product_code
GROUP BY s.date, s.product_code, p.price_inr
ORDER BY estimated_revenue DESC;

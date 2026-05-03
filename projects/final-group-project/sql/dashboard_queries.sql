-- ============================================================
-- 1. Revenue Concentration Risk (datasets/revenue_concentration_risk)
-- ============================================================
WITH sku_revenue AS (
  SELECT
    sku_id,
    product_category_name,
    SUM(sales_value) as sku_sales
  FROM
    fmcg.default.silver_sales
  WHERE
    sku_id IS NOT NULL
    AND product_category_name IS NOT NULL
  GROUP BY
    sku_id,
    product_category_name
),
total_revenue AS (
  SELECT
    SUM(sku_sales) as total
  FROM
    sku_revenue
),
ranked_skus AS (
  SELECT
    sr.sku_id,
    sr.product_category_name,
    sr.sku_sales,
    (sr.sku_sales * 100.0 / tr.total) as contribution_pct,
    SUM(sr.sku_sales * 100.0 / tr.total) OVER (
        ORDER BY sr.sku_sales DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) as cumulative_pct,
    ROW_NUMBER() OVER (ORDER BY sr.sku_sales DESC) as sku_rank
  FROM
    sku_revenue sr CROSS JOIN total_revenue tr
)
SELECT
  sku_rank,
  sku_id,
  product_category_name,
  sku_sales,
  ROUND(contribution_pct, 2) as contribution_pct,
  ROUND(cumulative_pct, 2) as cumulative_pct,
  CASE
    WHEN cumulative_pct <= 20 THEN 'Top 20% - Critical Dependency'
    WHEN cumulative_pct <= 50 THEN 'Top 50% - High Risk'
    WHEN cumulative_pct <= 80 THEN 'Top 80% - Moderate'
    ELSE 'Bottom 20% - Low Impact'
  END as risk_category
FROM
  ranked_skus
ORDER BY
  sku_rank
LIMIT 30;

-- ============================================================
-- 2. SKU Profitability Matrix (datasets/sku_profitability_matrix)
-- ============================================================
WITH sku_revenue AS (
  SELECT
    sku_id,
    SUM(sales_value) as revenue,
    SUM(net_amount) as net_revenue,
    COUNT(DISTINCT invoice_id) as order_count
  FROM
    fmcg.default.silver_sales
  WHERE
    sku_id IS NOT NULL
  GROUP BY
    sku_id
),
total_revenue AS (
  SELECT
    SUM(revenue) as total
  FROM
    sku_revenue
)
SELECT
  sr.sku_id,
  sr.revenue,
  sr.net_revenue as profit,
  ROUND(sr.net_revenue / NULLIF(sr.order_count, 0), 2) as avg_profit_per_order,
  ROUND((sr.net_revenue * 100.0 / NULLIF(sr.revenue, 0)), 2) as profit_margin_pct,
  ROUND((sr.revenue * 100.0 / tr.total), 2) as revenue_contribution_pct,
  CASE
    WHEN (sr.net_revenue * 100.0 / NULLIF(sr.revenue, 0)) >= 30 THEN 'High Margin'
    WHEN (sr.net_revenue * 100.0 / NULLIF(sr.revenue, 0)) >= 15 THEN 'Medium Margin'
    WHEN (sr.net_revenue * 100.0 / NULLIF(sr.revenue, 0)) >= 5 THEN 'Low Margin'
    ELSE 'Loss Leader'
  END as profitability_tier,
  CASE
    WHEN
      (sr.net_revenue * 100.0 / NULLIF(sr.revenue, 0)) >= 20
      AND (sr.revenue * 100.0 / tr.total) >= 1
    THEN
      'Star - High Revenue High Margin'
    WHEN
      (sr.net_revenue * 100.0 / NULLIF(sr.revenue, 0)) >= 20
      AND (sr.revenue * 100.0 / tr.total) < 1
    THEN
      'Niche - Low Revenue High Margin'
    WHEN
      (sr.net_revenue * 100.0 / NULLIF(sr.revenue, 0)) < 10
      AND (sr.revenue * 100.0 / tr.total) >= 1
    THEN
      'Cash Cow - High Revenue Low Margin'
    ELSE 'Question Mark - Low Revenue Low Margin'
  END as bcg_matrix_position
FROM
  sku_revenue sr CROSS JOIN total_revenue tr
WHERE
  sr.revenue > 0
ORDER BY
  sr.revenue DESC
LIMIT 50;

-- ============================================================
-- 3. Distributor Dependency Risk (datasets/distributor_dependency_risk)
-- ============================================================
WITH distributor_totals AS (
  SELECT
    SUM(total_sales) as overall_sales
  FROM
    fmcg.default.distributor_performance
),
distributor_ranking AS (
  SELECT
    dp.distributor_id,
    dp.region,
    dp.channel,
    dp.total_sales,
    dp.order_count,
    dp.approx_fill_rate,
    dp.rank,
    dt.overall_sales,
    ROUND((dp.total_sales * 100.0 / dt.overall_sales), 2) as revenue_share_pct,
    SUM(dp.total_sales * 100.0 / dt.overall_sales) OVER (
        ORDER BY dp.total_sales DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) as cumulative_share_pct
  FROM
    fmcg.default.distributor_performance dp CROSS JOIN distributor_totals dt
)
SELECT
  rank,
  distributor_id,
  region,
  channel,
  total_sales,
  order_count,
  ROUND(approx_fill_rate, 2) as fill_rate_pct,
  revenue_share_pct,
  ROUND(cumulative_share_pct, 2) as cumulative_share_pct,
  CASE
    WHEN cumulative_share_pct <= 50 THEN 'Top 50% - Critical Partners'
    WHEN cumulative_share_pct <= 80 THEN 'Top 80% - Key Partners'
    ELSE 'Bottom 20% - Standard'
  END as dependency_tier,
  CASE
    WHEN revenue_share_pct > 15 THEN 'High Risk - Single Point Failure'
    WHEN revenue_share_pct > 10 THEN 'Medium Risk - Concentrated'
    WHEN revenue_share_pct > 5 THEN 'Low Risk - Balanced'
    ELSE 'Minimal Risk - Diversified'
  END as risk_status
FROM
  distributor_ranking
ORDER BY
  rank
LIMIT 20;

-- ============================================================
-- 4. Customer Concentration Risk (datasets/customer_concentration_risk)
-- ============================================================
WITH customer_revenue AS (
  SELECT
    retailer_id,
    SUM(sales_value) as customer_revenue,
    COUNT(DISTINCT invoice_id) as order_count,
    COUNT(DISTINCT product_category_name) as categories_purchased
  FROM
    fmcg.default.silver_sales
  WHERE
    retailer_id IS NOT NULL
  GROUP BY
    retailer_id
),
total_revenue AS (
  SELECT
    SUM(customer_revenue) as total
  FROM
    customer_revenue
),
ranked_customers AS (
  SELECT
    cr.retailer_id,
    cr.customer_revenue,
    cr.order_count,
    cr.categories_purchased,
    ROUND((cr.customer_revenue * 100.0 / tr.total), 2) as revenue_share_pct,
    SUM(cr.customer_revenue * 100.0 / tr.total) OVER (
        ORDER BY cr.customer_revenue DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) as cumulative_pct,
    ROW_NUMBER() OVER (ORDER BY cr.customer_revenue DESC) as customer_rank
  FROM
    customer_revenue cr CROSS JOIN total_revenue tr
)
SELECT
  customer_rank,
  retailer_id,
  customer_revenue,
  order_count,
  categories_purchased,
  revenue_share_pct,
  ROUND(cumulative_pct, 2) as cumulative_pct,
  CASE
    WHEN cumulative_pct <= 20 THEN 'Top 20% - Critical Accounts'
    WHEN cumulative_pct <= 50 THEN 'Top 50% - Key Accounts'
    WHEN cumulative_pct <= 80 THEN 'Top 80% - Standard'
    ELSE 'Bottom 20% - Transactional'
  END as customer_tier,
  CASE
    WHEN revenue_share_pct > 5 THEN 'High Risk - Single Customer Dependency'
    WHEN revenue_share_pct > 2 THEN 'Medium Risk - Concentrated'
    WHEN revenue_share_pct > 1 THEN 'Low Risk - Balanced'
    ELSE 'Minimal Risk - Diversified'
  END as concentration_risk
FROM
  ranked_customers
ORDER BY
  customer_rank
LIMIT 50;

-- ============================================================
-- 5. Inventory Aging Risk (datasets/inventory_aging_risk)
-- ============================================================
WITH aging_summary AS (
  SELECT
    stock_age_bucket,
    SUM(qty_at_risk) as total_qty,
    AVG(avg_stock_age) as avg_days_aging,
    COUNT(DISTINCT sku_id) as sku_count,
    COUNT(DISTINCT distributor_id) as distributor_count,
    CASE
      WHEN stock_age_bucket = '<30 days' THEN 1
      WHEN stock_age_bucket = '30-60 days' THEN 2
      WHEN stock_age_bucket = '60-90 days' THEN 3
      ELSE 4
    END as bucket_order,
    CASE
      WHEN stock_age_bucket = '90+ days' THEN 'Critical - Dead Stock Risk'
      WHEN stock_age_bucket = '60-90 days' THEN 'High - Liquidation Needed'
      WHEN stock_age_bucket = '30-60 days' THEN 'Medium - Monitor Closely'
      ELSE 'Low - Healthy Turnover'
    END as risk_level
  FROM
    fmcg.default.stock_aging
  WHERE
    qty_at_risk > 0
  GROUP BY
    stock_age_bucket,
    bucket_order
)
SELECT
  stock_age_bucket,
  total_qty,
  ROUND(avg_days_aging, 0) as avg_days_aging,
  sku_count,
  distributor_count,
  bucket_order,
  risk_level,
  total_qty * 50 as estimated_value_at_risk,
  CASE
    WHEN stock_age_bucket = '90+ days' THEN ROUND(total_qty * 50 * 0.30, 0)
    WHEN stock_age_bucket = '60-90 days' THEN ROUND(total_qty * 50 * 0.15, 0)
    WHEN stock_age_bucket = '30-60 days' THEN ROUND(total_qty * 50 * 0.05, 0)
    ELSE 0
  END as estimated_write_off_risk,
  ROUND((total_qty * 100.0 / SUM(total_qty) OVER ()), 2) as qty_distribution_pct
FROM
  aging_summary
ORDER BY
  bucket_order;

-- ============================================================
-- 6. Regional Growth Opportunities (datasets/regional_growth)
-- ============================================================
WITH monthly_regional_sales AS (
  SELECT
    region,
    channel,
    DATE_TRUNC('MONTH', invoice_date) as month,
    SUM(sales_value) as revenue,
    COUNT(DISTINCT retailer_id) as customer_count,
    COUNT(DISTINCT invoice_id) as order_count
  FROM
    fmcg.default.silver_sales
  GROUP BY
    region,
    channel,
    month
),
growth_calcs AS (
  SELECT
    region,
    channel,
    month,
    revenue,
    customer_count,
    order_count,
    LAG(revenue, 1) OVER (PARTITION BY region, channel ORDER BY month) as prev_month_revenue,
    LAG(revenue, 12) OVER (PARTITION BY region, channel ORDER BY month) as prev_year_revenue
  FROM
    monthly_regional_sales
)
SELECT
  region,
  channel,
  SUM(revenue) as total_revenue,
  ROUND(AVG(customer_count), 0) as avg_monthly_customers,
  ROUND(AVG(order_count), 0) as avg_monthly_orders,
  ROUND(
    AVG((revenue - prev_month_revenue) / NULLIF(prev_month_revenue, 0) * 100),
    2
  ) as avg_mom_growth_pct,
  ROUND(
    AVG((revenue - prev_year_revenue) / NULLIF(prev_year_revenue, 0) * 100),
    2
  ) as avg_yoy_growth_pct,
  CASE
    WHEN
      AVG((revenue - prev_year_revenue) / NULLIF(prev_year_revenue, 0) * 100) > 20
    THEN
      'High Growth'
    WHEN
      AVG((revenue - prev_year_revenue) / NULLIF(prev_year_revenue, 0) * 100) > 5
    THEN
      'Moderate Growth'
    WHEN
      AVG((revenue - prev_year_revenue) / NULLIF(prev_year_revenue, 0) * 100) >= 0
    THEN
      'Low Growth'
    ELSE 'Declining'
  END as growth_status
FROM
  growth_calcs
WHERE
  month >= DATE '2017-06-01'
GROUP BY
  region,
  channel
ORDER BY
  avg_yoy_growth_pct DESC;

-- ============================================================
-- 7. Repeat Purchase Rate (datasets/repeat_purchase_rate)
-- ============================================================
WITH customer_orders AS (
  SELECT
    retailer_id,
    COUNT(DISTINCT invoice_id) AS order_count,
    COUNT(DISTINCT DATE_TRUNC('month', invoice_date)) AS active_months,
    MIN(invoice_date) AS first_purchase_date,
    MAX(invoice_date) AS last_purchase_date
  FROM
    fmcg.default.silver_sales
  WHERE
    invoice_date >= DATE_SUB(CURRENT_DATE(), 180)
  GROUP BY
    retailer_id
)
SELECT
  CASE
    WHEN order_count = 1 THEN 'One-time'
    WHEN order_count BETWEEN 2 AND 5 THEN 'Occasional'
    WHEN order_count BETWEEN 6 AND 15 THEN 'Regular'
    ELSE 'Frequent'
  END AS purchase_frequency,
  COUNT(DISTINCT retailer_id) AS customer_count,
  AVG(order_count) AS avg_orders,
  AVG(active_months) AS avg_active_months,
  AVG(DATEDIFF(last_purchase_date, first_purchase_date)) AS avg_customer_age_days,
  ROUND(
    COUNT(DISTINCT retailer_id) * 100.0 / SUM(COUNT(DISTINCT retailer_id)) OVER (),
    2
  ) AS customer_pct
FROM
  customer_orders
GROUP BY
  CASE
    WHEN order_count = 1 THEN 'One-time'
    WHEN order_count BETWEEN 2 AND 5 THEN 'Occasional'
    WHEN order_count BETWEEN 6 AND 15 THEN 'Regular'
    ELSE 'Frequent'
  END
ORDER BY
  avg_orders DESC;

-- ============================================================
-- 8. Channel Performance (datasets/channel_performance)
-- ============================================================
SELECT
  channel,
  SUM(total_sales) AS total_sales,
  SUM(total_orders) AS total_orders,
  AVG(avg_order_value) AS avg_order_value,
  COUNT(DISTINCT sales_date) AS active_days,
  ROUND(SUM(total_sales) * 100.0 / SUM(SUM(total_sales)) OVER (), 2) AS revenue_share_pct
FROM
  fmcg.default.sales_summary
WHERE
  sales_date >= DATE_SUB(CURRENT_DATE(), 90)
GROUP BY
  channel
ORDER BY
  total_sales DESC;

-- ============================================================
-- 9. Product Lifecycle (datasets/product_lifecycle)
-- ============================================================
WITH recent_sales AS (
  SELECT
    sku_id,
    product_category_name,
    SUM(
      CASE
        WHEN invoice_date >= DATE_SUB(CURRENT_DATE(), 30) THEN sales_value
        ELSE 0
      END
    ) AS revenue_last_30d,
    SUM(
      CASE
        WHEN
          invoice_date >= DATE_SUB(CURRENT_DATE(), 60)
          AND invoice_date < DATE_SUB(CURRENT_DATE(), 30)
        THEN
          sales_value
        ELSE 0
      END
    ) AS revenue_prev_30d,
    COUNT(DISTINCT
      CASE
        WHEN invoice_date >= DATE_SUB(CURRENT_DATE(), 30) THEN invoice_id
      END
    ) AS orders_last_30d
  FROM
    fmcg.default.silver_sales
  WHERE
    invoice_date >= DATE_SUB(CURRENT_DATE(), 90)
  GROUP BY
    sku_id,
    product_category_name
)
SELECT
  sku_id,
  product_category_name,
  revenue_last_30d,
  revenue_prev_30d,
  orders_last_30d,
  CASE
    WHEN
      revenue_prev_30d = 0
      AND revenue_last_30d > 0
    THEN
      'Launch'
    WHEN revenue_last_30d > revenue_prev_30d * 1.2 THEN 'Growth'
    WHEN revenue_last_30d >= revenue_prev_30d * 0.8 THEN 'Mature'
    ELSE 'Decline'
  END AS lifecycle_stage,
  ROUND(
    (revenue_last_30d - revenue_prev_30d) * 100.0 / NULLIF(revenue_prev_30d, 0),
    2
  ) AS growth_rate_pct
FROM
  recent_sales
WHERE
  revenue_last_30d > 0
  OR revenue_prev_30d > 0
ORDER BY
  revenue_last_30d DESC;

-- ============================================================
-- 10. Customer LTV Segmentation (datasets/customer_ltv_segmentation)
-- ============================================================
SELECT
  customer_segment,
  retention_status,
  COUNT(DISTINCT retailer_id) AS customer_count,
  SUM(customer_revenue) AS total_revenue,
  AVG(customer_revenue) AS avg_ltv,
  AVG(order_count) AS avg_orders,
  ROUND(
    SUM(customer_revenue) * 100.0 / SUM(SUM(customer_revenue)) OVER (),
    2
  ) AS revenue_contribution_pct
FROM
  fmcg.default.customer_segmentation
GROUP BY
  customer_segment,
  retention_status
ORDER BY
  total_revenue DESC;

-- ============================================================
-- 11. Market Basket Analysis (datasets/market_basket_analysis)
-- ============================================================
WITH category_pairs AS (
  SELECT
    s1.invoice_id,
    s1.product_category_name AS category_a,
    s2.product_category_name AS category_b
  FROM
    fmcg.default.silver_sales s1
      JOIN fmcg.default.silver_sales s2
        ON s1.invoice_id = s2.invoice_id
        AND s1.product_category_name < s2.product_category_name
  WHERE
    s1.invoice_date >= DATE_SUB(CURRENT_DATE(), 90)
)
SELECT
  category_a,
  category_b,
  COUNT(DISTINCT invoice_id) AS co_purchase_count,
  ROUND(
    COUNT(DISTINCT invoice_id)
      * 100.0
      / (
        SELECT
          COUNT(DISTINCT invoice_id)
        FROM
          fmcg.default.silver_sales
        WHERE
          invoice_date >= DATE_SUB(CURRENT_DATE(), 90)
      ),
    2
  ) AS affinity_pct
FROM
  category_pairs
GROUP BY
  category_a,
  category_b
HAVING
  COUNT(DISTINCT invoice_id) >= 10
ORDER BY
  co_purchase_count DESC
LIMIT 20;

-- ============================================================
-- 12. Distributor Performance Ranking (datasets/distributor_ranking)
-- ============================================================
SELECT
  rank,
  distributor_id,
  region,
  channel,
  total_sales,
  order_count,
  ROUND(approx_fill_rate, 2) AS fill_rate_pct,
  ROUND(total_sales / order_count, 2) AS avg_order_size,
  CASE
    WHEN rank <= 10 THEN 'Tier 1 - Strategic'
    WHEN rank <= 30 THEN 'Tier 2 - Core'
    WHEN rank <= 60 THEN 'Tier 3 - Standard'
    ELSE 'Tier 4 - Emerging'
  END AS distributor_tier,
  CASE
    WHEN
      approx_fill_rate >= 95
      AND total_sales > 100000
    THEN
      'Excellent'
    WHEN
      approx_fill_rate >= 85
      AND total_sales > 50000
    THEN
      'Good'
    WHEN approx_fill_rate >= 75 THEN 'Average'
    ELSE 'Needs Improvement'
  END AS performance_rating
FROM
  fmcg.default.distributor_performance
ORDER BY
  rank
LIMIT 50;

-- ============================================================
-- 13. Stock Aging Details (datasets/stock_aging_details)
-- ============================================================
SELECT
  sku_id,
  distributor_id,
  stock_age_bucket,
  qty_at_risk,
  ROUND(avg_stock_age, 0) AS avg_days_in_stock,
  CASE
    WHEN stock_age_bucket = '90+ days' THEN 'Critical'
    WHEN stock_age_bucket = '60-90 days' THEN 'High'
    WHEN stock_age_bucket = '30-60 days' THEN 'Medium'
    ELSE 'Low'
  END AS urgency_level,
  qty_at_risk * 50 AS estimated_value,
  CASE
    WHEN stock_age_bucket = '90+ days' THEN ROUND(qty_at_risk * 50 * 0.30, 0)
    WHEN stock_age_bucket = '60-90 days' THEN ROUND(qty_at_risk * 50 * 0.15, 0)
    WHEN stock_age_bucket = '30-60 days' THEN ROUND(qty_at_risk * 50 * 0.05, 0)
    ELSE 0
  END AS potential_write_off
FROM
  fmcg.default.stock_aging
WHERE
  qty_at_risk > 0
ORDER BY
  avg_stock_age DESC,
  qty_at_risk DESC
LIMIT 200;

-- ============================================================
-- 14. Customer Reactivation Opportunities (datasets/customer_reactivation)
-- ============================================================
WITH customer_activity AS (
  SELECT
    retailer_id,
    MAX(invoice_date) AS last_purchase_date,
    MIN(invoice_date) AS first_purchase_date,
    SUM(sales_value) AS lifetime_revenue,
    COUNT(DISTINCT invoice_id) AS total_orders,
    COUNT(DISTINCT product_category_name) AS categories_purchased
  FROM
    fmcg.default.silver_sales
  GROUP BY
    retailer_id
)
SELECT
  retailer_id,
  last_purchase_date,
  first_purchase_date,
  DATEDIFF(CURRENT_DATE(), last_purchase_date) AS days_since_last_purchase,
  lifetime_revenue,
  total_orders,
  categories_purchased,
  ROUND(lifetime_revenue / total_orders, 2) AS avg_order_value,
  CASE
    WHEN DATEDIFF(CURRENT_DATE(), last_purchase_date) BETWEEN 90 AND 180 THEN 'Recently Lapsed'
    WHEN DATEDIFF(CURRENT_DATE(), last_purchase_date) BETWEEN 181 AND 365 THEN 'At Risk'
    WHEN DATEDIFF(CURRENT_DATE(), last_purchase_date) > 365 THEN 'Lost'
    ELSE 'Active'
  END AS reactivation_segment,
  CASE
    WHEN
      lifetime_revenue > 50000
      AND DATEDIFF(CURRENT_DATE(), last_purchase_date) > 90
    THEN
      'High Priority'
    WHEN
      lifetime_revenue > 20000
      AND DATEDIFF(CURRENT_DATE(), last_purchase_date) > 90
    THEN
      'Medium Priority'
    WHEN DATEDIFF(CURRENT_DATE(), last_purchase_date) > 180 THEN 'Low Priority'
    ELSE 'No Action'
  END AS win_back_priority
FROM
  customer_activity
WHERE
  DATEDIFF(CURRENT_DATE(), last_purchase_date) >= 90
ORDER BY
  lifetime_revenue DESC
LIMIT 100;

-- ============================================================
-- 15. Cross-Category Adoption (datasets/cross_category_adoption)
-- ============================================================
WITH total_categories AS (
  SELECT
    COUNT(DISTINCT product_category_name) AS category_count
  FROM
    fmcg.default.silver_sales
),
customer_categories AS (
  SELECT
    retailer_id,
    COUNT(DISTINCT product_category_name) AS categories_purchased,
    SUM(sales_value) AS total_revenue,
    COUNT(DISTINCT invoice_id) AS order_count
  FROM
    fmcg.default.silver_sales
  GROUP BY
    retailer_id
)
SELECT
  CASE
    WHEN categories_purchased = 1 THEN 'Single Category'
    WHEN categories_purchased BETWEEN 2 AND 3 THEN 'Limited Portfolio'
    WHEN categories_purchased BETWEEN 4 AND 6 THEN 'Moderate Portfolio'
    ELSE 'Diverse Portfolio'
  END AS portfolio_breadth,
  COUNT(DISTINCT retailer_id) AS customer_count,
  AVG(categories_purchased) AS avg_categories,
  AVG(total_revenue) AS avg_revenue,
  AVG(order_count) AS avg_orders,
  ROUND(
    AVG(categories_purchased)
      * 100.0
      / (
        SELECT
          category_count
        FROM
          total_categories
      ),
    2
  ) AS category_penetration_pct,
  ROUND(
    COUNT(DISTINCT retailer_id) * 100.0 / SUM(COUNT(DISTINCT retailer_id)) OVER (),
    2
  ) AS customer_distribution_pct
FROM
  customer_categories
GROUP BY
  CASE
    WHEN categories_purchased = 1 THEN 'Single Category'
    WHEN categories_purchased BETWEEN 2 AND 3 THEN 'Limited Portfolio'
    WHEN categories_purchased BETWEEN 4 AND 6 THEN 'Moderate Portfolio'
    ELSE 'Diverse Portfolio'
  END
ORDER BY
  avg_categories DESC;

-- ============================================================
-- 16. Sales Trend Analysis (datasets/sales_trend)
-- ============================================================
WITH monthly_sales AS (
  SELECT
    DATE_TRUNC('month', sales_date) AS month,
    SUM(total_sales) AS revenue,
    SUM(total_orders) AS orders,
    AVG(avg_order_value) AS avg_order_value
  FROM
    fmcg.default.sales_summary
  GROUP BY
    month
)
SELECT
  month,
  revenue,
  orders,
  ROUND(avg_order_value, 2) AS avg_order_value,
  LAG(revenue, 1) OVER (ORDER BY month) AS prev_month_revenue,
  LAG(revenue, 12) OVER (ORDER BY month) AS prev_year_revenue,
  ROUND(
    (revenue - LAG(revenue, 1) OVER (ORDER BY month))
      * 100.0
      / NULLIF(LAG(revenue, 1) OVER (ORDER BY month), 0),
    2
  ) AS mom_growth_pct,
  ROUND(
    (revenue - LAG(revenue, 12) OVER (ORDER BY month))
      * 100.0
      / NULLIF(LAG(revenue, 12) OVER (ORDER BY month), 0),
    2
  ) AS yoy_growth_pct,
  ROUND(
    AVG(revenue) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
    2
  ) AS ma_3month,
  ROUND(AVG(revenue) OVER (ORDER BY month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), 2) AS ma_6month
FROM
  monthly_sales
ORDER BY
  month DESC
LIMIT 24;

-- ============================================================
-- 17. Demand Volatility Analysis (datasets/demand_volatility_analysis)
-- ============================================================
SELECT
  sku_id,
  product_category_name,
  ROUND(quantity_volatility_pct, 2) AS quantity_volatility_pct,
  ROUND(sales_volatility_pct, 2) AS sales_volatility_pct,
  CASE
    WHEN quantity_volatility_pct > 50 THEN 'High Volatility'
    WHEN quantity_volatility_pct > 25 THEN 'Moderate Volatility'
    ELSE 'Stable Demand'
  END AS demand_pattern,
  CASE
    WHEN quantity_volatility_pct > 50 THEN 'Safety Stock + Flexible Supply'
    WHEN quantity_volatility_pct > 25 THEN 'Buffer Inventory'
    ELSE 'Standard Planning'
  END AS inventory_strategy,
  CASE
    WHEN sales_volatility_pct > 40 THEN 'High Risk'
    WHEN sales_volatility_pct > 20 THEN 'Medium Risk'
    ELSE 'Low Risk'
  END AS forecast_risk
FROM
  fmcg.default.demand_volatility
ORDER BY
  quantity_volatility_pct DESC
LIMIT 100;

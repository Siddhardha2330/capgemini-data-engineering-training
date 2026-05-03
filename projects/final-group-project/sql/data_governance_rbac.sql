-- FMCG Distribution Analytics Platform
-- Simplified governance model
-- 1. Row-level security using secure views
-- 2. Distributor restriction inside secure views
-- 3. Column masking for sensitive revenue fields
-- 4. Minimal grants on Gold layer


-- =========================================================
-- 1. BASE ROLE ACCESS
-- =========================================================

GRANT USE CATALOG ON CATALOG fmcg TO `all_users`;
GRANT USE SCHEMA ON SCHEMA fmcg.gold TO `all_users`;

GRANT USE CATALOG ON CATALOG fmcg TO `finance_team`;
GRANT USE SCHEMA ON SCHEMA fmcg.gold TO `finance_team`;


-- =========================================================
-- 2. REGION-LEVEL SECURE VIEW
-- =========================================================
-- Each region team sees only its own region's data

CREATE OR REPLACE VIEW fmcg.gold.secure_sales AS
SELECT *
FROM fmcg.gold.sales_summary
WHERE
    region = CASE
        WHEN is_account_group_member('south_team') THEN 'SOUTH'
        WHEN is_account_group_member('north_team') THEN 'NORTH'
        WHEN is_account_group_member('southeast_team') THEN 'SOUTHEAST'
        WHEN is_account_group_member('northeast_team') THEN 'NORTHEAST'
        WHEN is_account_group_member('central_west_team') THEN 'CENTRAL-WEST'
    END;


-- =========================================================
-- 3. REGION + DISTRIBUTOR RESTRICTED VIEW
-- =========================================================
-- Each region team sees only distributors belonging to its region

CREATE OR REPLACE VIEW fmcg.gold.secure_distributor_sales AS
SELECT *
FROM fmcg.gold.distributor_performance
WHERE
    region = CASE
        WHEN is_account_group_member('south_team') THEN 'SOUTH'
        WHEN is_account_group_member('north_team') THEN 'NORTH'
        WHEN is_account_group_member('southeast_team') THEN 'SOUTHEAST'
        WHEN is_account_group_member('northeast_team') THEN 'NORTHEAST'
        WHEN is_account_group_member('central_west_team') THEN 'CENTRAL-WEST'
    END
AND
    distributor_id IN (
        SELECT distributor_id
        FROM fmcg.gold.distributor_performance
        WHERE
            region = CASE
                WHEN is_account_group_member('south_team') THEN 'SOUTH'
                WHEN is_account_group_member('north_team') THEN 'NORTH'
                WHEN is_account_group_member('southeast_team') THEN 'SOUTHEAST'
                WHEN is_account_group_member('northeast_team') THEN 'NORTHEAST'
                WHEN is_account_group_member('central_west_team') THEN 'CENTRAL-WEST'
            END
    );


-- =========================================================
-- 4. COLUMN MASKING FUNCTION
-- =========================================================
-- Finance can see real revenue, others see masked/null values

CREATE OR REPLACE FUNCTION fmcg.gold.mask_revenue(revenue DOUBLE)
RETURNS DOUBLE
RETURN CASE
    WHEN is_account_group_member('finance_team') THEN revenue
    ELSE NULL
END;


-- Optional partial masking version
-- CREATE OR REPLACE FUNCTION fmcg.gold.mask_partial_revenue(revenue DOUBLE)
-- RETURNS DOUBLE
-- RETURN CASE
--     WHEN is_account_group_member('finance_team') THEN revenue
--     ELSE revenue * 0.1
-- END;


-- =========================================================
-- 5. MASKED CONSUMPTION VIEW
-- =========================================================

CREATE OR REPLACE VIEW fmcg.gold.secure_masked_sales AS
SELECT
    sales_date,
    region,
    channel,
    total_quantity,
    mask_revenue(total_revenue) AS total_revenue,
    total_orders,
    avg_order_value
FROM fmcg.gold.secure_sales;


-- Distributor-masked view if needed for regional managers
CREATE OR REPLACE VIEW fmcg.gold.secure_masked_distributor_sales AS
SELECT
    distributor_id,
    region,
    channel,
    total_quantity,
    order_count,
    approx_fill_rate,
    rank,
    mask_revenue(total_sales) AS total_sales
FROM fmcg.gold.secure_distributor_sales;


-- =========================================================
-- 6. FINAL ACCESS
-- =========================================================
-- Users only consume secure views, not raw Gold tables directly

GRANT SELECT ON VIEW fmcg.gold.secure_masked_sales TO `all_users`;
GRANT SELECT ON VIEW fmcg.gold.secure_masked_distributor_sales TO `all_users`;

GRANT SELECT ON VIEW fmcg.gold.secure_masked_sales TO `south_team`;
GRANT SELECT ON VIEW fmcg.gold.secure_masked_sales TO `north_team`;
GRANT SELECT ON VIEW fmcg.gold.secure_masked_sales TO `southeast_team`;
GRANT SELECT ON VIEW fmcg.gold.secure_masked_sales TO `northeast_team`;
GRANT SELECT ON VIEW fmcg.gold.secure_masked_sales TO `central_west_team`;

GRANT SELECT ON VIEW fmcg.gold.secure_masked_distributor_sales TO `south_team`;
GRANT SELECT ON VIEW fmcg.gold.secure_masked_distributor_sales TO `north_team`;
GRANT SELECT ON VIEW fmcg.gold.secure_masked_distributor_sales TO `southeast_team`;
GRANT SELECT ON VIEW fmcg.gold.secure_masked_distributor_sales TO `northeast_team`;
GRANT SELECT ON VIEW fmcg.gold.secure_masked_distributor_sales TO `central_west_team`;


-- =========================================================
-- 7. VALIDATION
-- =========================================================

SHOW GRANTS ON CATALOG fmcg;
SHOW GRANTS ON SCHEMA fmcg.gold;

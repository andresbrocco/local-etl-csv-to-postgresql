-- ============================================================================
-- Analytics Query Library for Finance ETL Data Warehouse
-- ============================================================================
-- This file contains comprehensive SQL queries organized by category to
-- demonstrate the value of the star schema design and provide business insights.
--
-- Query Categories:
--   1. Data Validation Queries
--   2. Time-Based Analysis
--   3. Category Analysis
--   4. Merchant Analysis
--   5. Payment Method Analysis
--   6. User Analysis
--   7. Complex Analytical Queries
-- ============================================================================


-- ============================================================================
-- 1. DATA VALIDATION QUERIES
-- ============================================================================

-- Check record counts across all tables
-- Purpose: Verify data has been loaded correctly into all tables
SELECT 'fact_transactions' AS table_name, COUNT(*) AS record_count FROM fact_transactions
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL
SELECT 'dim_category', COUNT(*) FROM dim_category
UNION ALL
SELECT 'dim_merchant', COUNT(*) FROM dim_merchant
UNION ALL
SELECT 'dim_payment_method', COUNT(*) FROM dim_payment_method
UNION ALL
SELECT 'dim_user', COUNT(*) FROM dim_user
ORDER BY table_name;


-- Check for orphaned fact records (should return 0)
-- Purpose: Verify referential integrity - all fact records should have valid dimension references
SELECT COUNT(*) AS orphaned_records
FROM fact_transactions f
WHERE NOT EXISTS (SELECT 1 FROM dim_date d WHERE d.date_key = f.date_key)
   OR NOT EXISTS (SELECT 1 FROM dim_category c WHERE c.category_key = f.category_key)
   OR NOT EXISTS (SELECT 1 FROM dim_merchant m WHERE m.merchant_key = f.merchant_key)
   OR NOT EXISTS (SELECT 1 FROM dim_payment_method pm WHERE pm.payment_method_key = f.payment_method_key)
   OR NOT EXISTS (SELECT 1 FROM dim_user u WHERE u.user_key = f.user_key);


-- Verify no duplicate transaction_ids
-- Purpose: Ensure each transaction is unique (should return no rows)
SELECT transaction_id, COUNT(*) AS duplicate_count
FROM fact_transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;


-- Check amount data quality
-- Purpose: Statistical summary of transaction amounts to identify outliers or data issues
SELECT
    COUNT(*) AS total_transactions,
    MIN(amount) AS min_amount,
    MAX(amount) AS max_amount,
    AVG(amount) AS avg_amount,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) AS median_amount
FROM fact_transactions;


-- ============================================================================
-- 2. TIME-BASED ANALYSIS
-- ============================================================================

-- Monthly spending trends
-- Purpose: Track spending patterns over time by month
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(*) AS transaction_count,
    SUM(f.amount) AS total_spending,
    AVG(f.amount) AS avg_transaction_amount
FROM fact_transactions f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- Quarterly spending comparison
-- Purpose: Compare spending across quarters to identify seasonal patterns
SELECT
    d.year,
    d.quarter,
    COUNT(*) AS transaction_count,
    SUM(f.amount) AS total_spending
FROM fact_transactions f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.quarter
ORDER BY d.year, d.quarter;


-- Day of week spending patterns
-- Purpose: Identify which days of the week have highest transaction volume/spending
SELECT
    d.day_name,
    d.day_of_week,
    COUNT(*) AS transaction_count,
    SUM(f.amount) AS total_spending,
    AVG(f.amount) AS avg_transaction
FROM fact_transactions f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.day_name, d.day_of_week
ORDER BY d.day_of_week;


-- Weekend vs Weekday spending
-- Purpose: Compare spending behavior between weekdays and weekends
SELECT
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(*) AS transaction_count,
    SUM(f.amount) AS total_spending,
    AVG(f.amount) AS avg_transaction
FROM fact_transactions f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.is_weekend
ORDER BY day_type;


-- ============================================================================
-- 3. CATEGORY ANALYSIS
-- ============================================================================

-- Top spending categories
-- Purpose: Identify which categories consume the most budget (with percentage of total)
SELECT
    c.category_name,
    COUNT(*) AS transaction_count,
    SUM(f.amount) AS total_spending,
    AVG(f.amount) AS avg_transaction,
    ROUND(SUM(f.amount) * 100.0 / (SELECT SUM(amount) FROM fact_transactions), 2) AS percentage_of_total
FROM fact_transactions f
JOIN dim_category c ON f.category_key = c.category_key
GROUP BY c.category_name
ORDER BY total_spending DESC;


-- Category spending by month (pivot-like view)
-- Purpose: Track how spending in key categories changes month over month
SELECT
    d.year,
    d.month_name,
    SUM(CASE WHEN c.category_name = 'Groceries' THEN f.amount ELSE 0 END) AS groceries,
    SUM(CASE WHEN c.category_name = 'Dining' THEN f.amount ELSE 0 END) AS dining,
    SUM(CASE WHEN c.category_name = 'Transportation' THEN f.amount ELSE 0 END) AS transportation,
    SUM(CASE WHEN c.category_name = 'Shopping' THEN f.amount ELSE 0 END) AS shopping
FROM fact_transactions f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_category c ON f.category_key = c.category_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- ============================================================================
-- 4. MERCHANT ANALYSIS
-- ============================================================================

-- Top 20 merchants by spending
-- Purpose: Identify which merchants receive the most business
SELECT
    m.merchant_name,
    c.category_name,
    COUNT(*) AS transaction_count,
    SUM(f.amount) AS total_spent,
    AVG(f.amount) AS avg_transaction
FROM fact_transactions f
JOIN dim_merchant m ON f.merchant_key = m.merchant_key
JOIN dim_category c ON f.category_key = c.category_key
GROUP BY m.merchant_name, c.category_name
ORDER BY total_spent DESC
LIMIT 20;


-- Merchants visited most frequently
-- Purpose: Find merchants with highest visit frequency and unique customer count
SELECT
    m.merchant_name,
    COUNT(DISTINCT f.user_key) AS unique_users,
    COUNT(*) AS total_visits,
    SUM(f.amount) AS total_spent
FROM fact_transactions f
JOIN dim_merchant m ON f.merchant_key = m.merchant_key
GROUP BY m.merchant_name
ORDER BY total_visits DESC
LIMIT 15;


-- ============================================================================
-- 5. PAYMENT METHOD ANALYSIS
-- ============================================================================

-- Payment method usage and spending
-- Purpose: Understand payment method preferences and spending distribution
SELECT
    pm.payment_method_name,
    COUNT(*) AS transaction_count,
    SUM(f.amount) AS total_spending,
    AVG(f.amount) AS avg_transaction,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_transactions), 2) AS percentage_of_transactions
FROM fact_transactions f
JOIN dim_payment_method pm ON f.payment_method_key = pm.payment_method_key
GROUP BY pm.payment_method_name
ORDER BY total_spending DESC;


-- Payment method by category
-- Purpose: Identify payment method preferences for different spending categories
SELECT
    c.category_name,
    pm.payment_method_name,
    COUNT(*) AS transaction_count,
    SUM(f.amount) AS total_spent
FROM fact_transactions f
JOIN dim_category c ON f.category_key = c.category_key
JOIN dim_payment_method pm ON f.payment_method_key = pm.payment_method_key
GROUP BY c.category_name, pm.payment_method_name
ORDER BY c.category_name, total_spent DESC;


-- ============================================================================
-- 6. USER ANALYSIS
-- ============================================================================

-- Top 10 spending users
-- Purpose: Identify highest-value users by total spending
SELECT
    u.user_id,
    COUNT(*) AS transaction_count,
    SUM(f.amount) AS total_spending,
    AVG(f.amount) AS avg_transaction,
    MAX(f.amount) AS largest_transaction
FROM fact_transactions f
JOIN dim_user u ON f.user_key = u.user_key
GROUP BY u.user_id
ORDER BY total_spending DESC
LIMIT 10;


-- User spending by category (top 5 users)
-- Purpose: Break down spending by category for top users
SELECT
    u.user_id,
    c.category_name,
    COUNT(*) AS transaction_count,
    SUM(f.amount) AS total_spent
FROM fact_transactions f
JOIN dim_user u ON f.user_key = u.user_key
JOIN dim_category c ON f.category_key = c.category_key
WHERE u.user_id IN (
    -- Get top 5 users by total spending
    SELECT u2.user_id
    FROM fact_transactions f2
    JOIN dim_user u2 ON f2.user_key = u2.user_key
    GROUP BY u2.user_id
    ORDER BY SUM(f2.amount) DESC
    LIMIT 5
)
GROUP BY u.user_id, c.category_name
ORDER BY u.user_id, total_spent DESC;


-- ============================================================================
-- 7. COMPLEX ANALYTICAL QUERIES
-- ============================================================================

-- Month-over-month spending growth
-- Purpose: Calculate MoM spending changes using LAG window function
WITH monthly_spending AS (
    SELECT
        d.year,
        d.month,
        SUM(f.amount) AS total_spending
    FROM fact_transactions f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month
)
SELECT
    year,
    month,
    total_spending,
    LAG(total_spending) OVER (ORDER BY year, month) AS prev_month_spending,
    total_spending - LAG(total_spending) OVER (ORDER BY year, month) AS spending_change,
    ROUND(
        (total_spending - LAG(total_spending) OVER (ORDER BY year, month)) * 100.0 /
        NULLIF(LAG(total_spending) OVER (ORDER BY year, month), 0),
        2
    ) AS percent_change
FROM monthly_spending
ORDER BY year, month;


-- Running total by category
-- Purpose: Calculate cumulative spending over time for each category
SELECT
    d.date,
    c.category_name,
    f.amount,
    SUM(f.amount) OVER (
        PARTITION BY c.category_name
        ORDER BY d.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM fact_transactions f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_category c ON f.category_key = c.category_key
ORDER BY c.category_name, d.date;


-- Find anomalous transactions (3x above category average)
-- Purpose: Detect outlier transactions using z-scores for fraud detection or data quality
WITH category_stats AS (
    SELECT
        category_key,
        AVG(amount) AS avg_amount,
        STDDEV(amount) AS stddev_amount
    FROM fact_transactions
    GROUP BY category_key
)
SELECT
    f.transaction_id,
    d.date,
    c.category_name,
    m.merchant_name,
    f.amount,
    ROUND(cs.avg_amount, 2) AS category_avg,
    ROUND((f.amount - cs.avg_amount) / NULLIF(cs.stddev_amount, 0), 2) AS z_score
FROM fact_transactions f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_category c ON f.category_key = c.category_key
JOIN dim_merchant m ON f.merchant_key = m.merchant_key
JOIN category_stats cs ON f.category_key = cs.category_key
WHERE f.amount > cs.avg_amount * 3
ORDER BY f.amount DESC;

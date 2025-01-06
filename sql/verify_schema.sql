-- ============================================================================
-- Schema Verification Script - Personal Finance Data Warehouse
-- ============================================================================
-- This script verifies that the star schema has been created correctly.
-- It checks for table existence, structure, constraints, and initial data.
-- ============================================================================

\echo '============================================================================'
\echo 'SCHEMA VERIFICATION REPORT'
\echo '============================================================================'
\echo ''

-- ============================================================================
-- 1. TABLE EXISTENCE CHECK
-- ============================================================================
\echo '1. Checking table existence...'
\echo '-------------------------------------------'

SELECT
    table_name,
    CASE
        WHEN table_type = 'BASE TABLE' THEN 'EXISTS'
        ELSE 'MISSING'
    END AS status
FROM information_schema.tables
WHERE table_schema = 'public'
    AND table_name IN (
        'fact_transactions',
        'dim_date',
        'dim_category',
        'dim_merchant',
        'dim_payment_method',
        'dim_user'
    )
ORDER BY
    CASE table_name
        WHEN 'fact_transactions' THEN 1
        WHEN 'dim_date' THEN 2
        WHEN 'dim_category' THEN 3
        WHEN 'dim_merchant' THEN 4
        WHEN 'dim_payment_method' THEN 5
        WHEN 'dim_user' THEN 6
    END;

\echo ''

-- ============================================================================
-- 2. ROW COUNT CHECK
-- ============================================================================
\echo '2. Checking row counts...'
\echo '-------------------------------------------'

SELECT
    table_name,
    row_count
FROM (
    SELECT 'fact_transactions' AS table_name, COUNT(*) AS row_count FROM fact_transactions
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
) AS counts
ORDER BY
    CASE table_name
        WHEN 'fact_transactions' THEN 1
        WHEN 'dim_date' THEN 2
        WHEN 'dim_category' THEN 3
        WHEN 'dim_merchant' THEN 4
        WHEN 'dim_payment_method' THEN 5
        WHEN 'dim_user' THEN 6
    END;

\echo ''

-- ============================================================================
-- 3. COLUMN STRUCTURE CHECK
-- ============================================================================
\echo '3. Checking column structure...'
\echo '-------------------------------------------'

\echo 'fact_transactions columns:'
SELECT
    column_name,
    data_type,
    character_maximum_length,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'public'
    AND table_name = 'fact_transactions'
ORDER BY ordinal_position;

\echo ''
\echo 'dim_date columns:'
SELECT
    column_name,
    data_type,
    character_maximum_length,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'public'
    AND table_name = 'dim_date'
ORDER BY ordinal_position;

\echo ''
\echo 'dim_category columns:'
SELECT
    column_name,
    data_type,
    character_maximum_length,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'public'
    AND table_name = 'dim_category'
ORDER BY ordinal_position;

\echo ''
\echo 'dim_merchant columns:'
SELECT
    column_name,
    data_type,
    character_maximum_length,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'public'
    AND table_name = 'dim_merchant'
ORDER BY ordinal_position;

\echo ''
\echo 'dim_payment_method columns:'
SELECT
    column_name,
    data_type,
    character_maximum_length,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'public'
    AND table_name = 'dim_payment_method'
ORDER BY ordinal_position;

\echo ''
\echo 'dim_user columns:'
SELECT
    column_name,
    data_type,
    character_maximum_length,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'public'
    AND table_name = 'dim_user'
ORDER BY ordinal_position;

\echo ''

-- ============================================================================
-- 4. PRIMARY KEY CHECK
-- ============================================================================
\echo '4. Checking primary keys...'
\echo '-------------------------------------------'

SELECT
    tc.table_name,
    kcu.column_name AS primary_key_column
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
WHERE tc.constraint_type = 'PRIMARY KEY'
    AND tc.table_schema = 'public'
    AND tc.table_name IN (
        'fact_transactions',
        'dim_date',
        'dim_category',
        'dim_merchant',
        'dim_payment_method',
        'dim_user'
    )
ORDER BY tc.table_name;

\echo ''

-- ============================================================================
-- 5. FOREIGN KEY CHECK
-- ============================================================================
\echo '5. Checking foreign key constraints...'
\echo '-------------------------------------------'

SELECT
    tc.table_name AS fact_table,
    kcu.column_name AS foreign_key_column,
    ccu.table_name AS referenced_table,
    ccu.column_name AS referenced_column,
    tc.constraint_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage ccu
    ON ccu.constraint_name = tc.constraint_name
    AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_schema = 'public'
    AND tc.table_name = 'fact_transactions'
ORDER BY kcu.ordinal_position;

\echo ''

-- ============================================================================
-- 6. INDEX CHECK
-- ============================================================================
\echo '6. Checking indexes...'
\echo '-------------------------------------------'

SELECT
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'
    AND tablename IN (
        'fact_transactions',
        'dim_date',
        'dim_category',
        'dim_merchant',
        'dim_payment_method',
        'dim_user'
    )
ORDER BY tablename, indexname;

\echo ''

-- ============================================================================
-- 7. UNIQUE CONSTRAINT CHECK
-- ============================================================================
\echo '7. Checking unique constraints...'
\echo '-------------------------------------------'

SELECT
    tc.table_name,
    kcu.column_name,
    tc.constraint_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
WHERE tc.constraint_type = 'UNIQUE'
    AND tc.table_schema = 'public'
    AND tc.table_name IN (
        'fact_transactions',
        'dim_date',
        'dim_category',
        'dim_merchant',
        'dim_payment_method',
        'dim_user'
    )
ORDER BY tc.table_name, kcu.column_name;

\echo ''

-- ============================================================================
-- 8. DATE DIMENSION VERIFICATION
-- ============================================================================
\echo '8. Date dimension data verification...'
\echo '-------------------------------------------'

-- Check date range
SELECT
    MIN(date) AS min_date,
    MAX(date) AS max_date,
    COUNT(*) AS total_dates,
    COUNT(DISTINCT year) AS distinct_years
FROM dim_date;

\echo ''

-- Check sample records
\echo 'Sample records from dim_date:'
SELECT
    date_key,
    date,
    year,
    quarter,
    month,
    month_name,
    day_name,
    is_weekend
FROM dim_date
ORDER BY date
LIMIT 10;

\echo ''

-- ============================================================================
-- VERIFICATION COMPLETE
-- ============================================================================
\echo '============================================================================'
\echo 'VERIFICATION COMPLETE'
\echo '============================================================================'
\echo ''
\echo 'Review the output above to ensure:'
\echo '  - All 6 tables exist (1 fact, 5 dimensions)'
\echo '  - All primary keys are defined'
\echo '  - All foreign keys are defined on fact_transactions'
\echo '  - Indexes are created for performance'
\echo '  - dim_date is populated with data (should have 1461 rows for 2023-2026)'
\echo '  - Other dimension tables are empty (populated during ETL)'
\echo ''

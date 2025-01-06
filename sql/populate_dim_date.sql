-- ============================================================================
-- Populate Date Dimension - Personal Finance Data Warehouse
-- ============================================================================
-- This script populates the dim_date table with all dates from 2022-01-01
-- to 2026-12-31 (5 years), covering the 2-year data range plus buffer.
--
-- The date dimension enables time-based analysis and reporting by providing
-- pre-calculated calendar attributes for each date.
-- ============================================================================

-- Clear existing data (if any) for idempotency
TRUNCATE TABLE dim_date;

-- ============================================================================
-- POPULATE DATE DIMENSION
-- ============================================================================
-- Generate all dates and calculate their attributes using PostgreSQL's
-- date functions and generate_series()

INSERT INTO dim_date (
    date_key,
    date,
    year,
    quarter,
    month,
    month_name,
    day,
    day_of_week,
    day_name,
    week_of_year,
    is_weekend
)
SELECT
    -- date_key: Integer in YYYYMMDD format
    TO_CHAR(date_series, 'YYYYMMDD')::INTEGER AS date_key,

    -- date: Actual date value
    date_series::DATE AS date,

    -- year: Extract year (e.g., 2022)
    EXTRACT(YEAR FROM date_series)::SMALLINT AS year,

    -- quarter: Calculate quarter (1-4)
    EXTRACT(QUARTER FROM date_series)::SMALLINT AS quarter,

    -- month: Extract month (1-12)
    EXTRACT(MONTH FROM date_series)::SMALLINT AS month,

    -- month_name: Full month name (e.g., 'January')
    TO_CHAR(date_series, 'Month') AS month_name,

    -- day: Extract day of month (1-31)
    EXTRACT(DAY FROM date_series)::SMALLINT AS day,

    -- day_of_week: ISO day of week (1=Monday, 7=Sunday)
    EXTRACT(ISODOW FROM date_series)::SMALLINT AS day_of_week,

    -- day_name: Full day name (e.g., 'Monday')
    TO_CHAR(date_series, 'Day') AS day_name,

    -- week_of_year: ISO week number (1-53)
    EXTRACT(WEEK FROM date_series)::SMALLINT AS week_of_year,

    -- is_weekend: True if Saturday (6) or Sunday (7)
    CASE
        WHEN EXTRACT(ISODOW FROM date_series) IN (6, 7) THEN TRUE
        ELSE FALSE
    END AS is_weekend

FROM
    -- Generate series of dates from 2022-01-01 to 2026-12-31
    GENERATE_SERIES(
        '2022-01-01'::DATE,
        '2026-12-31'::DATE,
        '1 day'::INTERVAL
    ) AS date_series;

-- ============================================================================
-- VERIFICATION
-- ============================================================================
-- Display summary statistics

DO $$
DECLARE
    row_count INTEGER;
    min_date DATE;
    max_date DATE;
    year_count INTEGER;
BEGIN
    SELECT COUNT(*), MIN(date), MAX(date), COUNT(DISTINCT year)
    INTO row_count, min_date, max_date, year_count
    FROM dim_date;

    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'DATE DIMENSION POPULATION COMPLETE';
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Total rows inserted: %', row_count;
    RAISE NOTICE 'Date range: % to %', min_date, max_date;
    RAISE NOTICE 'Number of years: %', year_count;
    RAISE NOTICE '============================================================================';
END $$;

-- Display sample records
\echo ''
\echo 'Sample records from populated dim_date:'
\echo '-------------------------------------------'
SELECT
    date_key,
    date,
    year,
    quarter,
    month,
    month_name,
    day,
    day_of_week,
    day_name,
    week_of_year,
    is_weekend
FROM dim_date
WHERE date IN ('2022-01-01', '2023-12-31', '2024-06-15', '2025-07-04', '2026-12-31')
ORDER BY date;

\echo ''
\echo 'Weekend vs Weekday distribution:'
\echo '-------------------------------------------'
SELECT
    is_weekend,
    COUNT(*) AS day_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM dim_date), 2) AS percentage
FROM dim_date
GROUP BY is_weekend
ORDER BY is_weekend;

\echo ''
\echo 'Records per year:'
\echo '-------------------------------------------'
SELECT
    year,
    COUNT(*) AS day_count
FROM dim_date
GROUP BY year
ORDER BY year;

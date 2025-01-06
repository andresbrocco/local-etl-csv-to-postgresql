-- ============================================================================
-- Drop Schema Script - Personal Finance Data Warehouse
-- ============================================================================
-- This script cleanly removes all tables from the star schema.
-- Tables must be dropped in the correct order to avoid foreign key violations.
--
-- Order: Fact table first (contains FKs), then dimension tables
-- ============================================================================

-- Display information about what will be dropped
DO $$
BEGIN
    RAISE NOTICE 'Starting schema cleanup...';
    RAISE NOTICE 'This will drop all tables in the finance data warehouse';
END $$;

-- ============================================================================
-- DROP FACT TABLE
-- ============================================================================
-- Drop fact table first since it contains foreign keys to dimension tables

DROP TABLE IF EXISTS fact_transactions CASCADE;
-- CASCADE automatically drops dependent objects (indexes, constraints, etc.)

-- ============================================================================
-- DROP DIMENSION TABLES
-- ============================================================================
-- Safe to drop dimension tables after fact table is removed

DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_category CASCADE;
DROP TABLE IF EXISTS dim_merchant CASCADE;
DROP TABLE IF EXISTS dim_payment_method CASCADE;
DROP TABLE IF EXISTS dim_user CASCADE;

-- ============================================================================
-- CLEANUP COMPLETE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE 'Schema cleanup complete!';
    RAISE NOTICE 'All tables have been dropped successfully';
END $$;

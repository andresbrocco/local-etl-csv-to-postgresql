-- ============================================================================
-- Personal Finance Data Warehouse - Star Schema Implementation
-- ============================================================================
-- This script creates a dimensional model (star schema) for analyzing
-- personal finance transaction data. The schema consists of one fact table
-- and five dimension tables optimized for analytical queries.
--
-- Design Pattern: Star Schema
-- - Central fact table: fact_transactions
-- - Dimension tables: dim_date, dim_category, dim_merchant,
--                     dim_payment_method, dim_user
-- ============================================================================

-- ============================================================================
-- CLEANUP: Drop existing tables if they exist
-- ============================================================================
-- Drop in correct order: fact table first, then dimensions to avoid FK conflicts

DROP TABLE IF EXISTS fact_transactions CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_category CASCADE;
DROP TABLE IF EXISTS dim_merchant CASCADE;
DROP TABLE IF EXISTS dim_payment_method CASCADE;
DROP TABLE IF EXISTS dim_user CASCADE;

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================
-- Dimension tables store descriptive attributes for analysis.
-- They are created before the fact table to satisfy FK constraints.

-- ----------------------------------------------------------------------------
-- dim_date: Date Dimension
-- ----------------------------------------------------------------------------
-- Provides calendar attributes for time-based analysis.
-- Pre-populated with all dates needed for the analysis period.
-- Uses integer surrogate key in YYYYMMDD format for efficient storage/joins.

CREATE TABLE dim_date (
    -- Surrogate key in YYYYMMDD format (e.g., 20231109)
    date_key INTEGER PRIMARY KEY,

    -- Actual date value (unique constraint ensures data quality)
    date DATE UNIQUE NOT NULL,

    -- Year attributes
    year SMALLINT NOT NULL,

    -- Quarter (1-4)
    quarter SMALLINT NOT NULL CHECK (quarter BETWEEN 1 AND 4),

    -- Month attributes
    month SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
    month_name VARCHAR(20) NOT NULL,

    -- Day attributes
    day SMALLINT NOT NULL CHECK (day BETWEEN 1 AND 31),
    day_of_week SMALLINT NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),
    day_name VARCHAR(20) NOT NULL,

    -- Week number (1-53)
    week_of_year SMALLINT NOT NULL CHECK (week_of_year BETWEEN 1 AND 53),

    -- Weekend flag for analysis
    is_weekend BOOLEAN NOT NULL
);

-- Index on date for lookups when joining from source data
CREATE INDEX idx_dim_date_date ON dim_date(date);

COMMENT ON TABLE dim_date IS 'Date dimension for time-based analysis of transactions';
COMMENT ON COLUMN dim_date.date_key IS 'Surrogate key in YYYYMMDD format';
COMMENT ON COLUMN dim_date.day_of_week IS 'Day of week: 1=Monday, 7=Sunday';
COMMENT ON COLUMN dim_date.is_weekend IS 'True if Saturday or Sunday';

-- ----------------------------------------------------------------------------
-- dim_category: Spending Category Dimension
-- ----------------------------------------------------------------------------
-- Stores transaction categories (e.g., Groceries, Entertainment, Utilities).
-- Uses Type 1 SCD (Slowly Changing Dimension) - updates overwrite.

CREATE TABLE dim_category (
    -- Auto-incrementing surrogate key
    category_key SERIAL PRIMARY KEY,

    -- Natural key from source system (unique constraint for data quality)
    category_name VARCHAR(50) UNIQUE NOT NULL,

    -- Optional description for category
    category_description TEXT,

    -- Audit column
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for lookups by natural key during ETL
CREATE INDEX idx_dim_category_name ON dim_category(category_name);

COMMENT ON TABLE dim_category IS 'Spending categories dimension for transaction classification';
COMMENT ON COLUMN dim_category.category_key IS 'Surrogate key for fact table joins';
COMMENT ON COLUMN dim_category.category_name IS 'Natural key from source system';

-- ----------------------------------------------------------------------------
-- dim_merchant: Merchant/Vendor Dimension
-- ----------------------------------------------------------------------------
-- Stores merchant/vendor information.
-- Uses Type 1 SCD - updates overwrite existing records.

CREATE TABLE dim_merchant (
    -- Auto-incrementing surrogate key
    merchant_key SERIAL PRIMARY KEY,

    -- Natural key from source system
    merchant_name VARCHAR(200) UNIQUE NOT NULL,

    -- Audit column
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for lookups by natural key during ETL
CREATE INDEX idx_dim_merchant_name ON dim_merchant(merchant_name);

COMMENT ON TABLE dim_merchant IS 'Merchant/vendor dimension for transaction analysis';
COMMENT ON COLUMN dim_merchant.merchant_key IS 'Surrogate key for fact table joins';
COMMENT ON COLUMN dim_merchant.merchant_name IS 'Natural key from source system';

-- ----------------------------------------------------------------------------
-- dim_payment_method: Payment Method Dimension
-- ----------------------------------------------------------------------------
-- Stores payment method information (e.g., Credit Card, Cash, Digital Wallet).
-- Includes payment type classification for higher-level analysis.

CREATE TABLE dim_payment_method (
    -- Auto-incrementing surrogate key
    payment_method_key SERIAL PRIMARY KEY,

    -- Natural key from source system
    payment_method_name VARCHAR(50) UNIQUE NOT NULL,

    -- Payment type classification: 'Card', 'Cash', 'Digital'
    payment_type VARCHAR(20) CHECK (payment_type IN ('Card', 'Cash', 'Digital', 'Other')),

    -- Audit column
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for lookups by natural key during ETL
CREATE INDEX idx_dim_payment_method_name ON dim_payment_method(payment_method_name);

COMMENT ON TABLE dim_payment_method IS 'Payment method dimension for analyzing payment preferences';
COMMENT ON COLUMN dim_payment_method.payment_method_key IS 'Surrogate key for fact table joins';
COMMENT ON COLUMN dim_payment_method.payment_type IS 'Classification: Card, Cash, Digital, or Other';

-- ----------------------------------------------------------------------------
-- dim_user: User Dimension
-- ----------------------------------------------------------------------------
-- Stores user information. Simplified for demo; could be expanded with
-- additional attributes (name, demographics, preferences, etc.).

CREATE TABLE dim_user (
    -- Auto-incrementing surrogate key
    user_key SERIAL PRIMARY KEY,

    -- Natural key from source system
    user_id INTEGER UNIQUE NOT NULL,

    -- Audit column
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for lookups by natural key during ETL
CREATE INDEX idx_dim_user_user_id ON dim_user(user_id);

COMMENT ON TABLE dim_user IS 'User dimension for multi-user transaction analysis';
COMMENT ON COLUMN dim_user.user_key IS 'Surrogate key for fact table joins';
COMMENT ON COLUMN dim_user.user_id IS 'Natural key from source system';

-- ============================================================================
-- FACT TABLE
-- ============================================================================

-- ----------------------------------------------------------------------------
-- fact_transactions: Transaction Fact Table
-- ----------------------------------------------------------------------------
-- Central fact table storing measurable transaction events.
-- Contains foreign keys to all dimensions and the transaction amount measure.
-- Optimized for analytical queries with strategic indexing.

CREATE TABLE fact_transactions (
    -- Auto-incrementing surrogate key
    transaction_key BIGSERIAL PRIMARY KEY,

    -- Natural key from source system (unique to prevent duplicates)
    transaction_id VARCHAR(50) UNIQUE NOT NULL,

    -- Foreign keys to dimension tables
    date_key INTEGER NOT NULL,
    category_key INTEGER NOT NULL,
    merchant_key INTEGER NOT NULL,
    payment_method_key INTEGER NOT NULL,
    user_key INTEGER NOT NULL,

    -- Measure: transaction amount
    amount DECIMAL(10,2) NOT NULL,

    -- Audit columns
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Foreign key constraints for referential integrity
    CONSTRAINT fk_fact_transactions_date
        FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    CONSTRAINT fk_fact_transactions_category
        FOREIGN KEY (category_key) REFERENCES dim_category(category_key),
    CONSTRAINT fk_fact_transactions_merchant
        FOREIGN KEY (merchant_key) REFERENCES dim_merchant(merchant_key),
    CONSTRAINT fk_fact_transactions_payment_method
        FOREIGN KEY (payment_method_key) REFERENCES dim_payment_method(payment_method_key),
    CONSTRAINT fk_fact_transactions_user
        FOREIGN KEY (user_key) REFERENCES dim_user(user_key)
);

-- ============================================================================
-- INDEXES FOR QUERY PERFORMANCE
-- ============================================================================
-- Strategic indexes on foreign keys and common query patterns

-- Indexes on foreign keys for efficient joins
CREATE INDEX idx_fact_transactions_date_key ON fact_transactions(date_key);
CREATE INDEX idx_fact_transactions_category_key ON fact_transactions(category_key);
CREATE INDEX idx_fact_transactions_merchant_key ON fact_transactions(merchant_key);
CREATE INDEX idx_fact_transactions_payment_method_key ON fact_transactions(payment_method_key);
CREATE INDEX idx_fact_transactions_user_key ON fact_transactions(user_key);

-- Composite index for common query pattern: time-series analysis by user
CREATE INDEX idx_fact_transactions_date_user ON fact_transactions(date_key, user_key);

-- Index on transaction_id for ETL lookups and duplicate prevention
CREATE INDEX idx_fact_transactions_transaction_id ON fact_transactions(transaction_id);

-- Index on amount for queries filtering by transaction size
CREATE INDEX idx_fact_transactions_amount ON fact_transactions(amount);

COMMENT ON TABLE fact_transactions IS 'Fact table storing transaction events with measures and dimension keys';
COMMENT ON COLUMN fact_transactions.transaction_key IS 'Surrogate key for the fact table';
COMMENT ON COLUMN fact_transactions.transaction_id IS 'Natural key from source system (unique)';
COMMENT ON COLUMN fact_transactions.amount IS 'Transaction amount in currency (positive or negative)';
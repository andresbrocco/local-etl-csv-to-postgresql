# Local ETL Pipeline: CSV to PostgreSQL

A Python-based ETL pipeline for personal finance transaction data.

## Setup

### 1. Create virtual environment

```bash
python3 -m venv venv
source venv/bin/activate.fish  # Fish shell
# Or: source venv/bin/activate  # Bash/Zsh
# Or: venv\Scripts\activate     # Windows
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Setup PostgreSQL Database using Docker

```bash
# Start PostgreSQL container
docker-compose up -d

# Verify it's running
docker ps
```

The Docker setup creates:
- Database: `finance_etl`
- User: `andresbrocco`
- Password: `senhaforte`

### 4. Configure environment variables

```bash
cp .env.example .env
# Edit .env with your database credentials
```

For Docker setup, use these values in `.env`:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=finance_etl
DB_USER=andresbrocco
DB_PASSWORD=senhaforte
```

### 5. Test database connection

```bash
python scripts/test_connection.py
```

You should see:
```
✅ Successfully connected to PostgreSQL!
PostgreSQL version: PostgreSQL 14.x ...
```

### 6. Generate synthetic transaction data

```bash
venv/bin/python scripts/generate_fake_data.py
```

This generates `data/transactions.csv` with:
- 10,000 synthetic personal finance transactions
- 2-year date range
- 8 spending categories (Groceries, Dining, Transportation, Shopping, Utilities, Entertainment, Healthcare, Travel)
- 4 payment methods (Credit Card, Debit Card, Cash, Digital Wallet)
- 100 unique users
- Realistic amount ranges per category
- Reproducible data (seed=42)

### 7. Create database schema

```bash
# Create the star schema tables
PGPASSWORD=senhaforte psql -h localhost -U andresbrocco -d finance_etl -f sql/schema.sql

# Populate the date dimension
PGPASSWORD=senhaforte psql -h localhost -U andresbrocco -d finance_etl -f sql/populate_dim_date.sql

# Verify schema creation
PGPASSWORD=senhaforte psql -h localhost -U andresbrocco -d finance_etl -f sql/verify_schema.sql
```

## Database Schema

This project uses a **star schema** design optimized for analytical queries on personal finance transaction data.

### Schema Overview

```
                    ┌─────────────────┐
                    │   dim_date      │
                    ├─────────────────┤
                    │ date_key (PK)   │
                    │ date            │
                    │ year            │
                    │ quarter         │
                    │ month           │
                    │ day             │
                    │ is_weekend      │
                    └────────┬────────┘
                             │
                             │
    ┌──────────────┐    ┌───▼──────────────────┐    ┌──────────────────┐
    │dim_category  │    │ fact_transactions    │    │ dim_merchant     │
    ├──────────────┤    ├──────────────────────┤    ├──────────────────┤
    │category_key◄─┼────┤ transaction_key (PK) ├───►│ merchant_key     │
    │category_name │    │ transaction_id       │    │ merchant_name    │
    └──────────────┘    │ date_key (FK)        │    └──────────────────┘
                        │ category_key (FK)    │
                        │ merchant_key (FK)    │
    ┌──────────────┐    │ payment_method_key   │    ┌──────────────────┐
    │ dim_payment_ │    │ user_key (FK)        │    │   dim_user       │
    │   method     │    │ amount               │    ├──────────────────┤
    ├──────────────┤    │ created_at           ├───►│ user_key         │
    │payment_      ◄────┤ updated_at           │    │ user_id          │
    │method_key    │    └──────────────────────┘    └──────────────────┘
    │payment_name  │
    │payment_type  │
    └──────────────┘
```

### Tables

**Fact Table:**
- `fact_transactions` - Central table containing transaction events with foreign keys to all dimensions

**Dimension Tables:**
- `dim_date` - Pre-populated with 1,826 dates (2022-2026) including date attributes
- `dim_category` - Spending categories (Groceries, Dining, etc.)
- `dim_merchant` - Merchant/vendor information
- `dim_payment_method` - Payment methods (Credit Card, Debit Card, Cash, Digital Wallet)
- `dim_user` - User dimension (100 users)

### Key Features

- **Star Schema Design**: Optimized for analytical queries with denormalized dimensions
- **Surrogate Keys**: Auto-incrementing integer keys for all tables
- **Natural Keys Preserved**: Original IDs maintained with unique constraints
- **Foreign Key Constraints**: Enforce referential integrity
- **Strategic Indexing**: 25 indexes for query performance optimization
- **Date Dimension**: Pre-populated with full date hierarchy (year, quarter, month, day, weekend flag)
- **Idempotent Scripts**: All SQL scripts can be safely re-run

### SQL Scripts

- `sql/schema.sql` - Creates all tables, constraints, and indexes
- `sql/populate_dim_date.sql` - Populates date dimension with 5 years of data
- `sql/verify_schema.sql` - Verifies schema structure and constraints
- `sql/drop_schema.sql` - Drops all tables in correct order

## ETL Pipeline

### Extract Module

The Extract module (`src/extract.py`) handles reading CSV transaction data with comprehensive validation and error handling.

**Features:**
- Reads CSV files into pandas DataFrames
- Validates file existence and structure
- Checks for required columns
- Comprehensive error handling (FileNotFoundError, ParserError, etc.)
- Detailed logging to both console and file (`logs/etl_pipeline.log`)
- File metadata collection (size, modified time)
- Data quality checks (null values, empty rows)

**Usage:**
```python
from src.extract import extract_transactions
from src.config import TRANSACTIONS_CSV

# Extract data
df = extract_transactions(str(TRANSACTIONS_CSV))
print(f"Extracted {len(df)} transactions")
```

**Testing:**
```bash
# Run the extract module directly
venv/bin/python3 -m src.extract
```

### Transform Module

The Transform module (`src/transform.py`) cleans, validates, and enriches extracted transaction data, preparing it for loading into the star schema.

**Features:**
- Data cleaning (remove duplicates, trim whitespace, standardize casing)
- Business rule validation (amounts, dates, categories, payment methods)
- Date attribute derivation (year, quarter, month, day, weekday, weekend flag)
- Dimension data extraction (creates DataFrames for each dimension)
- Comprehensive data quality checks with detailed logging
- Invalid record filtering (logs issues without stopping pipeline)

**Data Quality Checks:**
- Amounts: Must be > $0.01 and < $10,000.00
- Dates: Must be between 2020-01-01 and present (not future)
- Categories: Must be in allowed list (8 categories)
- Payment Methods: Must be in allowed list (4 methods)
- Transaction IDs: Must be unique (duplicates removed)

**Returns:**
```python
{
    'fact_data': pd.DataFrame,           # Transformed fact table data
    'dim_date': pd.DataFrame,            # Date dimension with 11 attributes
    'dim_category': pd.DataFrame,        # Category dimension
    'dim_merchant': pd.DataFrame,        # Merchant dimension
    'dim_payment_method': pd.DataFrame,  # Payment method dimension
    'dim_user': pd.DataFrame             # User dimension
}
```

**Usage:**
```python
from src.extract import extract_transactions
from src.transform import transform_transactions
from src.config import TRANSACTIONS_CSV

# Extract and transform
df_raw = extract_transactions(str(TRANSACTIONS_CSV))
transformed = transform_transactions(df_raw)

# Access transformed data
fact_df = transformed['fact_data']
dim_date_df = transformed['dim_date']
print(f"Fact records: {len(fact_df)}")
print(f"Unique dates: {len(dim_date_df)}")
```

**Testing:**
```bash
# Run the transform module directly
venv/bin/python3 -m src.transform
```

### Load Module

The Load module (`src/load.py`) inserts transformed data into the PostgreSQL star schema data warehouse with comprehensive transaction management and duplicate prevention.

**Features:**
- Database connection management with proper cleanup
- Dimension table loading with ON CONFLICT handling (idempotent)
- Dimension key mapping (natural keys → surrogate keys)
- Fact data enrichment with surrogate keys
- Fact table loading with duplicate prevention
- Transaction management (commit/rollback for atomicity)
- Incremental loading (skip existing records)
- Batch loading for performance (1,000 records per batch)
- Comprehensive error handling with descriptive messages

**Loading Sequence:**
1. Establish database connection
2. Begin transaction
3. Load all dimension tables (dim_date, dim_category, dim_merchant, dim_payment_method, dim_user)
4. Retrieve dimension key mappings (natural → surrogate keys)
5. Enrich fact data with surrogate keys
6. Load fact table (with duplicate checking)
7. Commit transaction (or rollback on error)

**Returns:**
```python
{
    'dimensions_inserted': {
        'dim_date': int,
        'dim_category': int,
        'dim_merchant': int,
        'dim_payment_method': int,
        'dim_user': int
    },
    'facts_inserted': int,
    'facts_skipped': int  # Duplicates
}
```

**Usage:**
```python
from src.extract import extract_transactions
from src.transform import transform_transactions
from src.load import load_data_warehouse
from src.config import TRANSACTIONS_CSV

# Complete ETL Pipeline
df_raw = extract_transactions(str(TRANSACTIONS_CSV))
transformed = transform_transactions(df_raw)
results = load_data_warehouse(transformed)

print(f"Dimensions loaded: {results['dimensions_inserted']}")
print(f"Facts inserted: {results['facts_inserted']}")
print(f"Facts skipped: {results['facts_skipped']}")
```

**Testing:**
```bash
# Run the complete ETL pipeline
venv/bin/python3 -m src.load
```

**Key Features:**
- **Idempotent**: Can be run multiple times without duplicating data
- **Atomic**: All-or-nothing loading with transaction management
- **Incremental**: Only loads new records (checks existing transaction_ids)
- **Safe**: Parameterized queries prevent SQL injection
- **Fast**: Batch loading with psycopg2.extras.execute_batch()

**Supporting Modules:**
- `src/config.py` - Centralized configuration (paths, database settings, required columns)
- `src/logger.py` - Logging setup with file and console handlers

## Testing

The project includes comprehensive test suites for all ETL modules using pytest with fixtures, parametrization, and markers.

### Test Organization

```
tests/
├── conftest.py          # Shared fixtures for all tests
├── test_extract.py      # Extract module tests
├── test_transform.py    # Transform module tests
└── test_load.py         # Load module tests
```

### Running Tests

**Run all tests:**
```bash
venv/bin/python3 -m pytest tests/ -v
```

**Run specific module tests:**
```bash
# Extract module tests
venv/bin/python3 -m pytest tests/test_extract.py -v

# Transform module tests
venv/bin/python3 -m pytest tests/test_transform.py -v

# Load module tests
venv/bin/python3 -m pytest tests/test_load.py -v
```

**Run tests by marker:**
```bash
# Run only unit tests (fast, isolated)
venv/bin/python3 -m pytest tests/ -v -m unit

# Run only integration tests (end-to-end scenarios)
venv/bin/python3 -m pytest tests/ -v -m integration

# Run only error handling tests
venv/bin/python3 -m pytest tests/ -v -m error_handling

# Run only validation tests
venv/bin/python3 -m pytest tests/ -v -m validation
```

**Run tests with coverage:**
```bash
venv/bin/python3 -m pytest tests/ --cov=src --cov-report=html
```

### Test Suites Overview

**Extract Module Tests (`test_extract.py`):**
- CSV file reading and parsing
- Column validation
- Error handling (missing files, invalid formats)
- Data quality checks

**Transform Module Tests (`test_transform.py`):**
- Data cleaning (duplicates, whitespace, casing)
- Business rule validation (amounts, dates, categories)
- Date attribute derivation
- Dimension extraction
- Invalid record filtering

**Load Module Tests (`test_load.py` - 20 tests):**
- **Database Connection (3 tests)**:
  - Successful connection
  - Connection failure handling
  - Context manager rollback/cleanup

- **Dimension Loading (4 tests)**:
  - Load new dimension records
  - Skip existing records (idempotency)
  - Empty DataFrame handling
  - Date dimension with all attributes

- **Dimension Key Mapping (2 tests)**:
  - Retrieve individual mappings
  - Retrieve all dimension mappings

- **Fact Enrichment (5 tests)**:
  - Successful enrichment with valid keys
  - Error handling for missing keys (category, merchant, user, date)

- **Fact Loading (3 tests)**:
  - Check existing transactions
  - Load all new transactions
  - Incremental loading with duplicate skipping

- **End-to-End Integration (3 tests)**:
  - Complete successful load pipeline
  - Incremental loading idempotency
  - Transaction rollback on errors

### Test Markers

Tests are organized using pytest markers:
- `@pytest.mark.unit` - Fast, isolated unit tests with mocked dependencies
- `@pytest.mark.integration` - End-to-end tests with multiple components
- `@pytest.mark.error_handling` - Exception and error scenario tests
- `@pytest.mark.validation` - Data validation tests
- `@pytest.mark.file_operations` - File I/O tests

### Test Fixtures

Common fixtures available in `conftest.py`:
- **Data fixtures**: `valid_transaction_data`, `dimension_dataframes`, `enriched_fact_data`
- **File fixtures**: `valid_csv_file`, `empty_csv_file`, `incomplete_csv_file`
- **Mock fixtures**: `mock_db_connection`, `mock_cursor`
- **Configuration fixtures**: `required_columns`, `dimension_mappings`

### Example Test Output

```bash
$ venv/bin/python3 -m pytest tests/test_load.py -v

tests/test_load.py::TestGetDbConnection::test_get_db_connection_success PASSED
tests/test_load.py::TestGetDbConnection::test_get_db_connection_failure PASSED
tests/test_load.py::TestLoadDimension::test_load_dimension_new_records PASSED
tests/test_load.py::TestEnrichFactWithKeys::test_enrich_fact_with_keys_success PASSED
tests/test_load.py::TestLoadDataWarehouse::test_load_data_warehouse_success PASSED
...
======================= 20 passed in 0.17s =======================
```

## Manual Setup Steps Performed

### PostgreSQL Database Setup
- PostgreSQL 14 deployed using Docker container (postgres:14-alpine)
- Configured `.env` file with database credentials
- Verified connectivity with scripts/test_connection.py

## Project Status
🚧 In Development

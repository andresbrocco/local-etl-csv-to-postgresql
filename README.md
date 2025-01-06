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

**Supporting Modules:**
- `src/config.py` - Centralized configuration (paths, database settings, required columns)
- `src/logger.py` - Logging setup with file and console handlers

## Manual Setup Steps Performed

### PostgreSQL Database Setup
- PostgreSQL 14 deployed using Docker container (postgres:14-alpine)
- Configured `.env` file with database credentials
- Verified connectivity with scripts/test_connection.py

## Project Status
🚧 In Development

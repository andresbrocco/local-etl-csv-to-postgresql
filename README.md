# Local ETL Pipeline: CSV to PostgreSQL

> A production-ready Python ETL pipeline demonstrating data engineering fundamentals with star schema modeling, data validation, and incremental loading.

## 🎯 Project Overview

This project implements a complete ETL (Extract, Transform, Load) pipeline that processes personal finance transaction data from CSV files and loads it into a PostgreSQL data warehouse using dimensional modeling (star schema). It showcases core data engineering skills including data validation, error handling, logging, and SQL analytics.

### Key Features

- ✅ **Extract**: CSV file reading with validation and error handling
- ✅ **Transform**: Data cleaning, validation, and enrichment with derived fields
- ✅ **Load**: Incremental loading to PostgreSQL with duplicate prevention
- ✅ **Star Schema**: Proper dimensional modeling with 1 fact and 5 dimension tables
- ✅ **Data Quality**: Comprehensive validation rules and error handling
- ✅ **Monitoring**: Detailed logging and execution statistics
- ✅ **Analytics**: SQL query library demonstrating business insights
- ✅ **Testing**: Comprehensive test suites with pytest (60+ tests)
- ✅ **CLI Interface**: Command-line tool for easy execution

## 📊 Architecture

### Data Flow
```
[CSV File] → [Extract] → [Transform] → [Load] → [PostgreSQL Data Warehouse]
                ↓            ↓           ↓
            [Validate]  [Clean/Enrich] [Dimensions → Facts]
```

### Star Schema Design
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
    └──────────────┘
```

### Technology Stack

- **Python 3.10+** - Core programming language
- **PostgreSQL 14+** - Relational database
- **pandas 2.1+** - Data manipulation
- **psycopg2 2.9+** - PostgreSQL adapter
- **Faker 20.0+** - Synthetic data generation
- **pytest 7.4+** - Testing framework
- **python-dotenv 1.0+** - Environment management

## 🚀 Quick Start

### Prerequisites

- Python 3.10 or higher
- Docker
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/andresbrocco/local-etl-csv-to-postgresql.git
   cd local-etl-csv-to-postgresql
   ```

2. **Create virtual environment**
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Setup PostgreSQL**
   ```bash
   docker-compose up -d
   ```

5. **Configure environment variables**
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
   LOG_LEVEL=INFO
   ```

6. **Create database schema**
   ```bash
   PGPASSWORD=senhaforte psql -h localhost -U andresbrocco -d finance_etl -f sql/schema.sql
   PGPASSWORD=senhaforte psql -h localhost -U andresbrocco -d finance_etl -f sql/populate_dim_date.sql
   ```

7. **Generate sample data**
   ```bash
   venv/bin/python3 scripts/generate_fake_data.py
   ```

8. **Run the ETL pipeline**
   ```bash
   venv/bin/python3 -m src.etl_pipeline
   ```

You should see output like:
```
================================================================================
ETL PIPELINE SUMMARY
================================================================================
✓ Status: SUCCESS
✓ Execution Time: 2.45s
✓ Records Extracted: 10,000
✓ Records Transformed: 9,987
✓ Records Loaded: 9,987
================================================================================
```

## 📖 Usage

### Running the ETL Pipeline

**Basic execution:**
```bash
venv/bin/python3 -m src.etl_pipeline
```

**With options:**
```bash
# Specify custom CSV file
venv/bin/python3 -m src.etl_pipeline --file data/custom_transactions.csv

# Dry run (extract and transform only, no load)
venv/bin/python3 -m src.etl_pipeline --dry-run

# Validate prerequisites only
venv/bin/python3 -m src.etl_pipeline --validate-only

# Verbose logging
venv/bin/python3 -m src.etl_pipeline --verbose
```

**Show all options:**
```bash
venv/bin/python3 -m src.etl_pipeline --help
```

### Running Analytics Queries

**Execute validation queries:**
```bash
venv/bin/python3 -m src.run_queries --validation
```

**Execute sample queries:**
```bash
venv/bin/python3 -m src.run_queries
```

**Execute all queries:**
```bash
venv/bin/python3 -m src.run_queries --all
```

**Or run queries directly in psql:**
```bash
PGPASSWORD=senhaforte psql -h localhost -U andresbrocco -d finance_etl -f sql/queries.sql
```

### Regenerating Data

Generate new synthetic transactions:
```bash
venv/bin/python3 scripts/generate_fake_data.py
```

This creates `data/transactions.csv` with:
- 10,000 synthetic transactions
- 2-year date range (2023-2025)
- 8 spending categories
- 4 payment methods
- 100 unique users
- Reproducible data (seed=42)

## 📁 Project Structure

```
local-etl-csv-to-postgresql/
├── data/                     # CSV data files
│   └── transactions.csv      # Generated transaction data
├── docs/                     # Documentation
│   ├── ARCHITECTURE.md       # System architecture
│   └── LESSONS_LEARNED.md    # Project insights
├── logs/                     # Execution logs
│   └── etl_pipeline.log     # Pipeline execution logs
├── scripts/                  # Utility scripts
│   ├── generate_fake_data.py # Data generator
│   └── test_connection.py    # DB connection test
├── sql/                      # SQL scripts
│   ├── schema.sql           # Star schema DDL
│   ├── populate_dim_date.sql # Date dimension data
│   ├── drop_schema.sql      # Cleanup script
│   ├── verify_schema.sql    # Verification queries
│   └── queries.sql          # Analytics queries
├── src/                      # Source code
│   ├── __init__.py
│   ├── config.py            # Configuration management
│   ├── logger.py            # Logging setup
│   ├── extract.py           # Extract module
│   ├── transform.py         # Transform module
│   ├── load.py              # Load module
│   ├── etl_pipeline.py      # Main orchestration
│   └── run_queries.py       # Query executor
├── tests/                    # Test suites
│   ├── conftest.py          # Shared fixtures
│   ├── test_extract.py      # Extract tests
│   ├── test_transform.py    # Transform tests
│   └── test_load.py         # Load tests
├── .env.example             # Environment template
├── .gitignore               # Git ignore patterns
├── docker-compose.yml       # PostgreSQL container
├── pytest.ini               # Pytest configuration
├── requirements.txt         # Python dependencies
├── README.md                # This file
```

## 🗄️ Database Schema

### Star Schema Tables

**Fact Table:**
- `fact_transactions` - Central table containing transaction events with measures (amount) and foreign keys to all dimensions

**Dimension Tables:**
- `dim_date` - Pre-populated with 1,826 dates (2022-2026) including date attributes (year, quarter, month, day, weekend flag)
- `dim_category` - Spending categories (8 categories: Groceries, Dining, Transportation, Shopping, Utilities, Entertainment, Healthcare, Travel)
- `dim_merchant` - Merchants/vendors (unique merchants from transaction data)
- `dim_payment_method` - Payment methods (4 types: Credit Card, Debit Card, Cash, Digital Wallet)
- `dim_user` - User dimension (100 unique users)

### Key Features

- **Star Schema Design**: Optimized for analytical queries with denormalized dimensions
- **Surrogate Keys**: Auto-incrementing integer keys for all tables
- **Natural Keys Preserved**: Original IDs maintained with unique constraints
- **Foreign Key Constraints**: Enforce referential integrity
- **Strategic Indexing**: 25 indexes for query performance optimization
- **Date Dimension**: Pre-populated with full date hierarchy (year, quarter, month, day, weekend flag)
- **Idempotent Scripts**: All SQL scripts can be safely re-run

See [sql/schema.sql](sql/schema.sql) for complete DDL.

## 📊 Sample Analytics

The project includes a comprehensive SQL query library with 30+ analytical queries demonstrating:

- **Data Validation**: Integrity checks, duplicate detection, orphaned records
- **Time Analysis**: Monthly trends, quarterly comparisons, day-of-week patterns, weekend vs weekday
- **Category Analysis**: Top categories, spending distribution, category trends over time
- **Merchant Analysis**: Top merchants by spending, visit frequency
- **Payment Analysis**: Payment method preferences, usage distribution
- **User Analysis**: Top spenders, user behavior patterns, per-user category breakdown
- **Advanced Analytics**: Month-over-month growth (window functions), running totals, anomaly detection

### Sample Query Results

**Validation Summary:**
```
================================================================================
VALIDATION SUMMARY
================================================================================
✓ All validation checks PASSED
✓ No orphaned records: 0
✓ No duplicate transaction IDs: 0

Record Counts:
  • fact_transactions: 10,003
  • dim_date: 1,826
  • dim_category: 8
  • dim_merchant: 8,613
  • dim_payment_method: 4
  • dim_user: 100

Data Quality:
  • Min amount: $5.07
  • Max amount: $1,996.60
  • Average: $164.01
  • Median: $104.93
```

**Top 5 Spending Categories:**
```
category_name | transaction_count | total_spending | percentage_of_total
-----------------------------------------------------------------------------
Shopping      | 1,483             | $388,974.03    | 23.71%
Travel        | 304               | $313,895.01    | 19.13%
Groceries     | 2,536             | $264,326.97    | 16.11%
Utilities     | 1,008             | $180,763.73    | 11.02%
Healthcare    | 402               | $169,488.91    | 10.33%
```

**Weekend vs Weekday Spending:**
```
day_type | transaction_count | total_spending | avg_transaction
----------------------------------------------------------------
Weekday  | 7,259             | $1,196,615.77  | $164.85
Weekend  | 2,744             | $443,967.13    | $161.80
```

**Payment Method Distribution:**
```
payment_method  | transaction_count | total_spending | percentage
------------------------------------------------------------------
Credit Card     | 7,035 (70.33%)    | $1,163,779.92  | 70.33%
Debit Card      | 1,498 (14.98%)    | $229,944.00    | 14.98%
Cash            | 983 (9.83%)       | $165,492.65    | 9.83%
Digital Wallet  | 487 (4.87%)       | $81,366.33     | 4.87%
```

**Top 10 Merchants by Spending:**
```
merchant_name              | category_name | total_spent
----------------------------------------------------------
Barnes Plc                 | Travel        | $2,187.39
Murillo-Becker             | Travel        | $1,996.60
Scott-Adkins               | Travel        | $1,996.28
Lutz-Fleming               | Travel        | $1,985.62
Foley-Martinez             | Travel        | $1,983.50
```

## 🧪 Testing

The project includes comprehensive test suites for all ETL modules using pytest with fixtures, parametrization, and markers.

### Test Organization

```
tests/
├── conftest.py          # Shared fixtures for all tests
├── test_extract.py      # Extract module tests (15 tests)
├── test_transform.py    # Transform module tests (25 tests)
└── test_load.py         # Load module tests (20 tests)
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

### Test Coverage

- **Extract Module (15 tests)**: CSV reading, validation, error handling, data quality checks
- **Transform Module (25 tests)**: Data cleaning, business rule validation, date derivation, dimension extraction
- **Load Module (20 tests)**: Database operations, dimension loading, fact enrichment, incremental loading, transaction management

## 🔍 Troubleshooting

### Common Issues

**Issue**: Database connection fails
```
Solution: Verify PostgreSQL is running and .env credentials are correct
$ docker ps  # Check if container is running
$ PGPASSWORD=senhaforte psql -h localhost -U andresbrocco -d finance_etl  # Test connection
```

**Issue**: CSV file not found
```
Solution: Ensure you've generated the data
$ venv/bin/python3 scripts/generate_fake_data.py
```

**Issue**: Import errors
```
Solution: Ensure virtual environment is activated and dependencies installed
$ source venv/bin/activate
$ pip install -r requirements.txt
```

**Issue**: ModuleNotFoundError when running scripts
```
Solution: Use the module syntax instead of direct script execution
$ venv/bin/python3 -m src.etl_pipeline  # Correct
$ python src/etl_pipeline.py            # May fail due to import paths
```

**Issue**: Permission denied when accessing database
```
Solution: Verify database user credentials in .env file
$ cat .env  # Check credentials
$ docker-compose down && docker-compose up -d  # Restart container
```

**Issue**: Schema already exists error
```
Solution: Drop existing schema before recreating
$ PGPASSWORD=senhaforte psql -h localhost -U andresbrocco -d finance_etl -f sql/drop_schema.sql
$ PGPASSWORD=senhaforte psql -h localhost -U andresbrocco -d finance_etl -f sql/schema.sql
```

## 📈 Performance

With 10,000 transactions on a standard laptop (M1 MacBook):
- **Extract**: ~0.1 seconds
- **Transform**: ~0.5 seconds
- **Load**: ~1.8 seconds
- **Total Pipeline**: ~2.5 seconds

Performance scales linearly for larger datasets. The pipeline uses batch loading (1,000 records per batch) for optimal database write performance.

## 🎓 Learning Outcomes

This project demonstrates:

1. ✅ **ETL Pipeline Design** - Modular architecture with separate Extract-Transform-Load components
2. ✅ **Dimensional Modeling** - Star schema with fact and dimension tables following Kimball methodology
3. ✅ **SQL Proficiency** - DDL, DML, complex analytical queries with window functions and CTEs
4. ✅ **Data Validation** - Business rules, data quality checks, error detection
5. ✅ **Error Handling** - Comprehensive exception handling, logging, and graceful failure
6. ✅ **Database Operations** - Connection management, transactions, incremental loading, duplicate prevention
7. ✅ **Python Best Practices** - Type hints, docstrings, modular code, configuration management
8. ✅ **Testing** - Unit tests, integration tests, fixtures, parametrization, markers
9. ✅ **Version Control** - Git workflow with meaningful commits and conventional commit messages
10. ✅ **Documentation** - Comprehensive README, architecture docs, inline comments

## 🔮 Future Enhancements

Potential improvements:

- [ ] Add data profiling and quality reports (pandas-profiling)
- [ ] Add support for multiple data sources (JSON, APIs, Parquet)
- [ ] Create data visualization dashboard (Streamlit or Dash)
- [ ] Implement slowly changing dimensions (SCD Type 2) for historical tracking
- [ ] Add Apache Airflow for scheduling and orchestration
- [ ] Deploy to cloud (AWS RDS, Lambda, S3)
- [ ] Add data lineage tracking (Apache Atlas)
- [ ] Implement parallel processing for large files (Dask, multiprocessing)
- [ ] Create alerting and monitoring system (Prometheus, Grafana)
- [ ] Add data catalog and metadata management
- [ ] Implement data quality framework (Great Expectations)
- [ ] Add CDC (Change Data Capture) support
- [ ] Create data pipeline observability dashboard
- [ ] Add machine learning models for transaction categorization
- [ ] Implement data versioning and time travel queries

## 📝 License

This project is part of a data engineering portfolio and is available for educational purposes.

## 👤 Author

**André Sbrocco**
- GitHub: [@andresbrocco](https://github.com/andresbrocco)
- LinkedIn: [linkedin.com/in/andresbrocco](https://www.linkedin.com/in/andresbrocco)
- Portfolio: [www.andresbrocco.com](https://www.andresbrocco.com)

## 🙏 Acknowledgments

- Project inspired by real-world data engineering challenges in financial analytics
- Star schema design follows Kimball dimensional modeling methodology
- Built as part of data engineering skill development and portfolio creation
- Thanks to the Python and PostgreSQL communities for excellent tools and documentation
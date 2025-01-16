# Architecture Documentation

## System Overview

This document describes the technical architecture of the Local ETL Pipeline project, including design decisions, implementation patterns, and the rationale behind key technology choices.

## System Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         ETL Pipeline                             │
│                                                                  │
│  ┌─────────┐      ┌───────────┐      ┌──────────┐              │
│  │ Extract │─────►│ Transform │─────►│   Load   │              │
│  └─────────┘      └───────────┘      └──────────┘              │
│       │                 │                   │                   │
│       ▼                 ▼                   ▼                   │
│  [Validate]       [Clean/Enrich]    [Dimensions → Facts]        │
│       │                 │                   │                   │
└───────┼─────────────────┼───────────────────┼───────────────────┘
        │                 │                   │
        ▼                 ▼                   ▼
   [CSV File]      [DataFrames]      [PostgreSQL Database]
```

### ETL Pipeline Flow

#### 1. Extract Phase (`src/extract.py`)

**Purpose**: Read CSV transaction data and validate file structure

**Process**:
1. Validate file existence and accessibility
2. Read CSV file using pandas (optimized for performance)
3. Validate required columns are present
4. Check for empty data or malformed records
5. Collect file metadata (size, modified time)
6. Log extraction statistics
7. Return validated DataFrame

**Key Features**:
- Comprehensive error handling (FileNotFoundError, ParserError, etc.)
- File metadata collection for monitoring
- Data quality checks (null values, empty rows)
- Detailed logging at INFO, WARNING, and ERROR levels

**Error Handling**:
- Missing file: Raises FileNotFoundError with clear message
- Invalid CSV format: Raises ParserError with line number
- Missing columns: Raises ValueError with list of missing columns
- Empty data: Raises ValueError

#### 2. Transform Phase (`src/transform.py`)

**Purpose**: Clean, validate, and enrich extracted data for loading

**Process**:
1. **Data Cleaning**:
   - Remove duplicate transaction IDs
   - Trim whitespace from string fields
   - Standardize casing (proper case for categories/merchants)

2. **Business Rule Validation**:
   - Amounts must be > $0.01 and < $10,000.00
   - Dates must be between 2020-01-01 and present (not future)
   - Categories must be in allowed list (8 categories)
   - Payment methods must be in allowed list (4 methods)

3. **Date Attribute Derivation**:
   - Parse transaction_date to datetime
   - Extract year, quarter, month, day
   - Calculate day_of_week (0=Monday, 6=Sunday)
   - Calculate month_name, day_name
   - Determine is_weekend (True/False)

4. **Dimension Extraction**:
   - Extract unique dates with all attributes
   - Extract unique categories
   - Extract unique merchants
   - Extract unique payment methods
   - Extract unique user IDs

5. **Fact Data Preparation**:
   - Keep valid records only
   - Prepare data with natural keys for foreign key mapping

**Data Quality Checks**:
- Invalid records are filtered (not rejected entirely)
- Comprehensive logging of all validation issues
- Statistics reported for each validation check

**Returns**:
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

#### 3. Load Phase (`src/load.py`)

**Purpose**: Load transformed data into PostgreSQL star schema

**Process**:
1. **Establish Database Connection**:
   - Connect using credentials from environment variables
   - Use context manager for proper cleanup
   - Log connection details (host, port, database)

2. **Begin Transaction**:
   - All loading happens within a single database transaction
   - Ensures atomicity (all-or-nothing)

3. **Load Dimension Tables**:
   - Load in dependency order (no dependencies, so any order works)
   - Use `ON CONFLICT DO NOTHING` for idempotency
   - Dimensions: dim_date, dim_category, dim_merchant, dim_payment_method, dim_user

4. **Retrieve Surrogate Keys**:
   - Query each dimension table to get natural key → surrogate key mapping
   - Store mappings in dictionaries for lookup

5. **Enrich Fact Data**:
   - Map natural keys (category_name, merchant_name, etc.) to surrogate keys
   - Add surrogate key columns (category_key, merchant_key, etc.)
   - Raise error if any natural key cannot be mapped

6. **Load Fact Table**:
   - Check for existing transaction IDs (for incremental loading)
   - Filter out duplicates
   - Insert new records using batch loading (1,000 per batch)
   - Log statistics (inserted, skipped)

7. **Commit Transaction**:
   - Commit if all steps succeed
   - Rollback on any error

**Key Features**:
- **Idempotent**: Can be run multiple times without duplicating data
- **Atomic**: All-or-nothing loading with transaction management
- **Incremental**: Only loads new records (checks existing transaction_ids)
- **Safe**: Parameterized queries prevent SQL injection
- **Fast**: Batch loading with `psycopg2.extras.execute_batch()`

**Returns**:
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

### ETL Orchestration (`src/etl_pipeline.py`)

**Purpose**: Coordinate all ETL phases with comprehensive error handling

**Features**:
- Complete pipeline orchestration (Extract → Transform → Load)
- Pre-flight validation (database connection, tables, file)
- Multiple execution modes (full, dry-run, validate-only)
- CLI interface with argparse
- Execution time tracking
- Graceful error handling and recovery
- Proper exit codes for scripting

**Execution Modes**:
1. **Full Mode** (default): Extract → Transform → Load
2. **Dry Run** (`--dry-run`): Extract → Transform (no database writes)
3. **Validate Only** (`--validate-only`): Check prerequisites only

**Pre-flight Validation**:
- Database connectivity test
- Verify all required tables exist
- Verify source CSV file exists
- Log table row counts

**Error Handling**:
- Custom exception hierarchy (ETLError, ExtractError, TransformError, LoadError, ValidationError)
- Phase-specific error messages
- Keyboard interrupt handling (Ctrl+C)
- Proper cleanup on errors

**Exit Codes**:
- `0`: Success
- `1`: Failure
- `130`: Interrupted by user

## Star Schema Design

### Design Rationale

**Why Star Schema?**

1. **Query Performance**:
   - Denormalized structure minimizes joins
   - Direct joins from fact to dimensions (no multi-hop joins)
   - Query execution plans are simple and predictable

2. **Business Clarity**:
   - Dimensions provide clear business context
   - Easy for analysts to understand
   - Intuitive for business intelligence tools

3. **Scalability**:
   - Fact table can grow to millions/billions of rows
   - Dimension tables remain relatively small
   - Indexing strategy optimizes common queries

4. **Flexibility**:
   - Easy to add new dimensions without schema changes
   - Supports dimensional analysis (slice and dice)
   - Enables OLAP operations (drill-down, roll-up)

### Schema Components

#### Fact Table: `fact_transactions`

**Grain**: One row per transaction event

**Measures**:
- `amount` (DECIMAL): Transaction amount (additive measure)

**Dimensions** (Foreign Keys):
- `date_key` → dim_date
- `category_key` → dim_category
- `merchant_key` → dim_merchant
- `payment_method_key` → dim_payment_method
- `user_key` → dim_user

**Natural Key**:
- `transaction_id` (VARCHAR): Original transaction identifier (unique)

**Audit Fields**:
- `created_at` (TIMESTAMP): Record creation time
- `updated_at` (TIMESTAMP): Record update time

**Design Decisions**:
- Surrogate key (`transaction_key`) for technical stability
- Natural key preserved with unique constraint
- Foreign key constraints enforce referential integrity
- Indexes on all foreign keys for join performance

#### Dimension Table: `dim_date`

**Purpose**: Pre-populated date dimension with full hierarchy

**Attributes**:
- `date` (DATE): The actual date (natural key)
- `year` (INTEGER): 2022-2026
- `quarter` (INTEGER): 1-4
- `month` (INTEGER): 1-12
- `month_name` (VARCHAR): January-December
- `day` (INTEGER): 1-31
- `day_of_week` (INTEGER): 0=Monday, 6=Sunday
- `day_name` (VARCHAR): Monday-Sunday
- `week_of_year` (INTEGER): 1-53
- `is_weekend` (BOOLEAN): True for Saturday/Sunday

**Design Decisions**:
- Pre-populated for 5 years (2022-2026) for performance
- Includes all common date attributes to avoid date calculations in queries
- Natural date as unique constraint
- Enables efficient time-based analysis

#### Dimension Tables: `dim_category`, `dim_merchant`, `dim_payment_method`, `dim_user`

**Purpose**: Provide context for transactions

**Pattern**:
- Surrogate key (auto-incrementing integer)
- Natural key (name/ID) with unique constraint
- Descriptive attributes
- Audit timestamp (created_at)

**Design Decisions**:
- Surrogate keys allow natural keys to change without breaking relationships
- Unique constraints on natural keys prevent duplicates
- Indexes on natural keys for lookup performance
- Minimal attributes (no over-engineering)

### Indexing Strategy

**25 Strategic Indexes**:

1. **Primary Keys** (6 indexes): All tables have PK index
2. **Foreign Keys** (5 indexes): All fact table FKs indexed for joins
3. **Natural Keys** (5 indexes): All dimension natural keys indexed for lookups
4. **Unique Constraints** (6 indexes): Ensure data integrity
5. **Composite Indexes** (3 indexes): Common query patterns (e.g., date + category)

**Index Benefits**:
- Fast joins between fact and dimensions
- Fast lookups during loading (natural key → surrogate key mapping)
- Fast unique constraint checking
- Query optimizer can use covering indexes

## Technology Choices

### Python 3.10+

**Why Python?**
- Industry standard for data engineering
- Rich ecosystem of data tools (pandas, SQLAlchemy, etc.)
- Easy to read and maintain
- Strong community support

**Why 3.10+?**
- Modern type hints syntax (`list[str]` instead of `List[str]`)
- Structural pattern matching (not used yet, but available)
- Performance improvements
- Long-term support

### PostgreSQL 14+

**Why PostgreSQL?**
- Open-source RDBMS with excellent SQL support
- ACID compliance for data integrity
- Rich feature set (window functions, CTEs, JSON support)
- Strong performance for analytical workloads
- Industry-standard for data warehousing

**Why 14+?**
- Performance improvements (parallel query, JIT compilation)
- Enhanced indexing capabilities
- Better monitoring and observability
- Security improvements

### pandas 2.1+

**Why pandas?**
- De facto standard for data manipulation in Python
- Optimized for in-memory operations
- Rich API for data transformation
- Integration with CSV, SQL, and other formats

**Alternatives Considered**:
- **Polars**: Faster but less mature ecosystem
- **Dask**: Better for larger-than-memory data, but adds complexity
- **Raw SQL**: Less expressive for complex transformations

### psycopg2 2.9+

**Why psycopg2?**
- Most popular PostgreSQL adapter for Python
- Mature and well-tested
- Efficient connection pooling
- Support for batch operations

**Alternatives Considered**:
- **psycopg3**: Newer but less mature
- **asyncpg**: Async support, but not needed for this use case
- **SQLAlchemy**: ORM overhead not needed for this project

### Faker 20.0+

**Why Faker?**
- Standard library for generating synthetic data
- Realistic data generation
- Reproducible with seeds
- Wide variety of data types

### pytest 7.4+

**Why pytest?**
- Industry standard for Python testing
- Rich fixture system
- Parametrization support
- Plugin ecosystem (coverage, markers, etc.)

## Design Patterns

### 1. Configuration Management

**Pattern**: Centralized configuration module

**Implementation**: `src/config.py`

**Benefits**:
- Single source of truth for configuration
- Easy to modify without code changes
- Environment-specific settings via `.env` file
- Type-safe constants

**Example**:
```python
from src.config import DB_HOST, DB_PORT, TRANSACTIONS_CSV
```

### 2. Logging

**Pattern**: Centralized logger factory

**Implementation**: `src/logger.py`

**Benefits**:
- Consistent logging format across modules
- Dual output (file + console)
- Prevents duplicate handlers
- Easy to configure log levels

**Example**:
```python
from src.logger import setup_logger
logger = setup_logger(__name__)
```

### 3. Database Connection Management

**Pattern**: Context manager for database connections

**Implementation**: `get_db_connection()` in `src/load.py`

**Benefits**:
- Automatic connection cleanup (even on errors)
- Transaction management
- Resource leak prevention
- Idiomatic Python

**Example**:
```python
with get_db_connection() as conn:
    cursor = conn.cursor()
    # Use connection
    # Automatic cleanup on exit
```

### 4. Error Handling

**Pattern**: Custom exception hierarchy

**Implementation**:
```python
class ETLError(Exception): pass
class ExtractError(ETLError): pass
class TransformError(ETLError): pass
class LoadError(ETLError): pass
class ValidationError(ETLError): pass
```

**Benefits**:
- Specific error types for each phase
- Easy to catch specific errors
- Clear error messages
- Proper error propagation

### 5. Incremental Loading

**Pattern**: Check-and-skip pattern for duplicates

**Implementation**:
1. Query existing transaction IDs
2. Filter out existing records
3. Load only new records

**Benefits**:
- Idempotent pipeline (can re-run safely)
- Efficient (only loads new data)
- Simple and reliable

### 6. Batch Loading

**Pattern**: Batch inserts using `execute_batch()`

**Implementation**: Load 1,000 records per batch

**Benefits**:
- Better performance than row-by-row
- Reduces network round trips
- Balances memory usage and performance

### 7. Separation of Concerns

**Pattern**: Modular architecture

**Implementation**: Separate modules for Extract, Transform, Load

**Benefits**:
- Easy to test in isolation
- Easy to modify without affecting other phases
- Clear responsibilities
- Reusable components

## Data Flow and State Management

### State Transitions

```
CSV File (Static)
    ↓
Extracted DataFrame (In-Memory)
    ↓
Transformed DataFrames (In-Memory)
    ↓
Database Tables (Persistent)
```

**Key Points**:
- State is immutable at each stage (no in-place modifications)
- Each stage validates input before processing
- Errors halt pipeline and preserve original state
- Database writes are atomic (transaction-based)

### Data Validation

**Multi-Layer Validation**:

1. **Extract Phase**:
   - File existence
   - File format (valid CSV)
   - Required columns present
   - Non-empty data

2. **Transform Phase**:
   - Business rules (amounts, dates, categories)
   - Data types (numeric amounts, valid dates)
   - Referential integrity (valid categories/payment methods)

3. **Load Phase**:
   - Foreign key constraints (database-enforced)
   - Unique constraints (database-enforced)
   - NOT NULL constraints (database-enforced)

## Security Considerations

### 1. SQL Injection Prevention

**Approach**: Always use parameterized queries

**Implementation**:
```python
cursor.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
```

**Never**:
```python
cursor.execute(f"SELECT * FROM users WHERE user_id = '{user_id}'")
```

### 2. Credential Management

**Approach**: Environment variables via `.env` file

**Implementation**:
- Credentials never hardcoded
- `.env` file in `.gitignore`
- `.env.example` provides template

### 3. Connection Security

**Approach**: Use PostgreSQL authentication

**Implementation**:
- User credentials required
- No anonymous access
- Connection timeouts configured

### 4. Data Validation

**Approach**: Validate all external input

**Implementation**:
- Amount ranges validated
- Date ranges validated
- Categorical values validated
- SQL injection prevented via parameterization

## Performance Optimization

### 1. Extract Performance

- Use pandas CSV reader (optimized C parser)
- Read entire file at once (assumes file fits in memory)
- Validate columns early (fail fast)

### 2. Transform Performance

- Use pandas vectorized operations (no row iteration)
- Remove duplicates efficiently with `drop_duplicates()`
- Use pandas datetime parsing (optimized)
- Filter invalid records once (no repeated validation)

### 3. Load Performance

- Use batch loading (1,000 records per batch)
- Use `execute_batch()` instead of `executemany()`
- Load dimensions before facts (satisfies foreign keys)
- Use `ON CONFLICT DO NOTHING` for idempotency
- Commit once at end (not per record)

### 4. Database Performance

- Indexes on all foreign keys
- Indexes on all natural keys
- Indexes on frequently queried columns
- Analyze tables after loading (for query planning)

## Scalability Considerations

### Current Limitations

- In-memory processing (file must fit in RAM)
- Single-threaded execution
- Single database connection
- No distributed processing

### Future Scalability Improvements

1. **Larger Files**:
   - Chunk processing for large CSV files
   - Streaming transformations
   - Spill to disk if needed

2. **Parallel Processing**:
   - Multiprocessing for transformation
   - Connection pooling for loading
   - Parallel batch loading

3. **Distributed Processing**:
   - Apache Spark for large-scale processing
   - Partitioned loading by date ranges
   - Distributed database (Redshift, BigQuery)

4. **Cloud Deployment**:
   - AWS Lambda for serverless execution
   - S3 for data storage
   - RDS for database
   - CloudWatch for monitoring

## Testing Strategy

### Test Organization

**Three-Layer Approach**:

1. **Unit Tests**: Test individual functions with mocked dependencies
2. **Integration Tests**: Test complete workflows with real components
3. **Error Handling Tests**: Test exception scenarios

### Test Fixtures

**Shared Fixtures** (`conftest.py`):
- Data fixtures (valid/invalid data)
- File fixtures (CSV files)
- Mock fixtures (database connections)
- Configuration fixtures

**Benefits**:
- Reusable test data
- Consistent test setup
- Easy to maintain

### Test Markers

**Custom Markers**:
- `@pytest.mark.unit`: Fast, isolated tests
- `@pytest.mark.integration`: End-to-end tests
- `@pytest.mark.error_handling`: Exception tests
- `@pytest.mark.validation`: Data validation tests
- `@pytest.mark.file_operations`: File I/O tests

**Benefits**:
- Run specific test categories
- Separate fast/slow tests
- Organize test execution

## Monitoring and Observability

### Logging

**Log Levels**:
- **INFO**: Successful operations, key milestones, statistics
- **WARNING**: Data quality issues, skipped records
- **ERROR**: Failures, exceptions

**Log Output**:
- File: `logs/etl_pipeline.log` (persistent)
- Console: stdout (real-time feedback)

**Log Format**:
```
2025-11-10 00:37:00 - module_name - LEVEL - Message
```

### Metrics

**Pipeline Metrics**:
- Execution time (total and per phase)
- Records processed (extracted, transformed, loaded)
- Records skipped (duplicates, invalid)
- Dimension statistics (records per dimension)

**Data Quality Metrics**:
- Duplicate count
- Invalid record count
- Null value count
- Orphaned records (should be 0)

## Conclusion

This architecture demonstrates industry best practices for building production-ready ETL pipelines:

- **Modular Design**: Separate concerns for maintainability
- **Data Quality**: Multi-layer validation
- **Performance**: Optimized for common use cases
- **Reliability**: Error handling and transaction management
- **Testability**: Comprehensive test coverage
- **Observability**: Detailed logging and metrics
- **Security**: Parameterized queries and credential management
- **Scalability**: Designed for future growth

The architecture balances simplicity with production-readiness, making it suitable for both learning and real-world applications.

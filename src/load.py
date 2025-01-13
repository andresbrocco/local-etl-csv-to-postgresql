"""
Load module for the ETL pipeline.

This module handles the load phase of the ETL process:
- Establishes PostgreSQL database connections
- Loads dimension tables with surrogate key management
- Retrieves dimension key mappings
- Enriches fact data with surrogate keys
- Loads fact table with duplicate prevention
- Implements transaction management for data integrity
- Provides incremental loading capabilities
- Uses batch inserts for performance optimization
"""

from contextlib import contextmanager
from typing import Any
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch

from src.logger import setup_logger
from src.config import DB_CONFIG, BATCH_SIZE

# Set up logger for this module
logger = setup_logger(__name__)


# ============================================================================
# Custom Exceptions
# ============================================================================

class DatabaseConnectionError(Exception):
    """Raised when database connection fails"""
    pass


class DimensionLoadError(Exception):
    """Raised when dimension loading fails"""
    pass


class FactLoadError(Exception):
    """Raised when fact loading fails"""
    pass


# ============================================================================
# Database Connection Management
# ============================================================================

def get_db_connection():
    """
    Create and return a PostgreSQL connection.

    Uses configuration from src.config.DB_CONFIG.
    Implements proper connection handling with error reporting.

    Returns:
        psycopg2 connection object

    Raises:
        DatabaseConnectionError: If connection cannot be established

    Example:
        >>> conn = get_db_connection()
        >>> cursor = conn.cursor()
        >>> cursor.execute("SELECT version();")
        >>> conn.close()
    """
    try:
        logger.info("Establishing database connection...")
        logger.info(f"Connecting to {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")

        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )

        logger.info("Database connection established successfully")
        return conn

    except psycopg2.Error as e:
        error_msg = f"Failed to connect to database: {str(e)}"
        logger.error(error_msg)
        raise DatabaseConnectionError(error_msg) from e
    except Exception as e:
        error_msg = f"Unexpected error connecting to database: {str(e)}"
        logger.error(error_msg)
        raise DatabaseConnectionError(error_msg) from e


@contextmanager
def database_connection():
    """
    Context manager for database connections.

    Ensures connections are properly closed and handles rollback on errors.
    Use this for transaction-safe database operations.

    Yields:
        psycopg2 connection object

    Raises:
        DatabaseConnectionError: If connection cannot be established

    Example:
        >>> with database_connection() as conn:
        >>>     cursor = conn.cursor()
        >>>     cursor.execute("SELECT * FROM dim_category")
    """
    conn = None
    try:
        conn = get_db_connection()
        yield conn
    except Exception as e:
        logger.error(f"Database operation error: {e}")
        if conn and not conn.closed:
            conn.rollback()
            logger.info("Transaction rolled back due to error")
        raise
    finally:
        if conn and not conn.closed:
            conn.close()
            logger.info("Database connection closed")


# ============================================================================
# Dimension Loading Functions
# ============================================================================

def load_dimension(
    conn,
    df: pd.DataFrame,
    table_name: str,
    natural_key_column: str,
    additional_columns: list[str] = None
) -> int:
    """
    Load dimension table with duplicate prevention.

    Uses INSERT ... ON CONFLICT DO NOTHING for idempotency.
    Only inserts new records; existing records are skipped.

    Args:
        conn: Active database connection
        df: DataFrame with dimension data
        table_name: Name of dimension table (e.g., 'dim_category')
        natural_key_column: Column name for natural key (e.g., 'category_name')
        additional_columns: List of additional columns to insert (optional)

    Returns:
        Number of new rows inserted

    Raises:
        DimensionLoadError: If dimension loading fails

    Example:
        >>> df = pd.DataFrame({'category_name': ['Groceries', 'Dining']})
        >>> inserted = load_dimension(conn, df, 'dim_category', 'category_name')
        >>> print(f"Inserted {inserted} new categories")
    """
    try:
        if df.empty:
            logger.warning(f"No data to load for {table_name}")
            return 0

        logger.info(f"Loading {table_name}...")
        logger.info(f"  Records to process: {len(df)}")

        cursor = conn.cursor()

        # Build column list
        columns = [natural_key_column]
        if additional_columns:
            columns.extend(additional_columns)

        # Build INSERT query with ON CONFLICT
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))

        insert_query = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT ({natural_key_column}) DO NOTHING
        """

        # Prepare data for insertion (convert numpy types to native Python types)
        records = []
        for _, row in df.iterrows():
            values = tuple(row[col].item() if hasattr(row[col], 'item') else row[col] for col in columns)
            records.append(values)

        # Get count before insert
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count_before = cursor.fetchone()[0]

        # Execute batch insert
        execute_batch(cursor, insert_query, records, page_size=BATCH_SIZE)

        # Get count after insert
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count_after = cursor.fetchone()[0]

        inserted = count_after - count_before

        logger.info(f"  Inserted {inserted} new records (skipped {len(df) - inserted} existing)")
        logger.info(f"  Total records in {table_name}: {count_after}")

        cursor.close()
        return inserted

    except psycopg2.Error as e:
        error_msg = f"Failed to load dimension {table_name}: {str(e)}"
        logger.error(error_msg)
        raise DimensionLoadError(error_msg) from e
    except Exception as e:
        error_msg = f"Unexpected error loading dimension {table_name}: {str(e)}"
        logger.error(error_msg)
        raise DimensionLoadError(error_msg) from e


def load_dim_date(conn, df: pd.DataFrame) -> int:
    """
    Load date dimension with all attributes.

    Args:
        conn: Active database connection
        df: DataFrame with date dimension data

    Returns:
        Number of new rows inserted

    Example:
        >>> date_df = transformed_data['dim_date']
        >>> inserted = load_dim_date(conn, date_df)
    """
    logger.info("Loading dim_date...")

    if df.empty:
        logger.warning("No date dimension data to load")
        return 0

    try:
        cursor = conn.cursor()

        # Build INSERT query with all date attributes
        insert_query = """
            INSERT INTO dim_date (
                date_key, date, year, quarter, month, day,
                month_name, day_name, day_of_week, week_of_year, is_weekend
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_key) DO NOTHING
        """

        # Prepare records
        records = []
        for _, row in df.iterrows():
            values = (
                int(row['date_key']),
                row['date'],
                int(row['year']),
                int(row['quarter']),
                int(row['month']),
                int(row['day']),
                row['month_name'],
                row['day_name'],
                int(row['day_of_week']),
                int(row['week_of_year']),
                bool(row['is_weekend'])
            )
            records.append(values)

        # Get count before insert
        cursor.execute("SELECT COUNT(*) FROM dim_date")
        count_before = cursor.fetchone()[0]

        # Execute batch insert
        execute_batch(cursor, insert_query, records, page_size=BATCH_SIZE)

        # Get count after insert
        cursor.execute("SELECT COUNT(*) FROM dim_date")
        count_after = cursor.fetchone()[0]

        inserted = count_after - count_before

        logger.info(f"  Inserted {inserted} new date records (skipped {len(df) - inserted} existing)")
        logger.info(f"  Total records in dim_date: {count_after}")

        cursor.close()
        return inserted

    except psycopg2.Error as e:
        error_msg = f"Failed to load dim_date: {str(e)}"
        logger.error(error_msg)
        raise DimensionLoadError(error_msg) from e
    except Exception as e:
        error_msg = f"Unexpected error loading dim_date: {str(e)}"
        logger.error(error_msg)
        raise DimensionLoadError(error_msg) from e


# ============================================================================
# Dimension Key Mapping Functions
# ============================================================================

def get_dimension_key_mapping(
    conn,
    table_name: str,
    natural_key_column: str,
    surrogate_key_column: str
) -> dict[Any, int]:
    """
    Retrieve mapping of natural keys to surrogate keys.

    Queries the dimension table to create a lookup dictionary
    for enriching fact data with surrogate keys.

    Args:
        conn: Active database connection
        table_name: Name of dimension table
        natural_key_column: Natural key column name
        surrogate_key_column: Surrogate key column name

    Returns:
        Dictionary mapping natural keys to surrogate keys

    Example:
        >>> mapping = get_dimension_key_mapping(
        ...     conn, 'dim_category', 'category_name', 'category_key'
        ... )
        >>> print(mapping)  # {'Groceries': 1, 'Dining': 2, ...}
    """
    try:
        logger.info(f"Retrieving key mapping for {table_name}...")

        cursor = conn.cursor()

        query = f"""
            SELECT {natural_key_column}, {surrogate_key_column}
            FROM {table_name}
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        mapping = {row[0]: row[1] for row in rows}

        logger.info(f"  Retrieved {len(mapping)} key mappings from {table_name}")

        cursor.close()
        return mapping

    except psycopg2.Error as e:
        error_msg = f"Failed to retrieve key mapping from {table_name}: {str(e)}"
        logger.error(error_msg)
        raise DimensionLoadError(error_msg) from e
    except Exception as e:
        error_msg = f"Unexpected error retrieving key mapping from {table_name}: {str(e)}"
        logger.error(error_msg)
        raise DimensionLoadError(error_msg) from e


def get_all_dimension_mappings(conn) -> dict[str, dict]:
    """
    Retrieve all dimension key mappings at once.

    Args:
        conn: Active database connection

    Returns:
        Dictionary of mappings for each dimension:
        {
            'category': {natural_key: surrogate_key, ...},
            'merchant': {natural_key: surrogate_key, ...},
            'payment_method': {natural_key: surrogate_key, ...},
            'user': {natural_key: surrogate_key, ...},
            'date': {natural_key: surrogate_key, ...}
        }

    Example:
        >>> mappings = get_all_dimension_mappings(conn)
        >>> category_key = mappings['category']['Groceries']
    """
    logger.info("Retrieving all dimension key mappings...")

    mappings = {}

    # Category mapping
    mappings['category'] = get_dimension_key_mapping(
        conn, 'dim_category', 'category_name', 'category_key'
    )

    # Merchant mapping
    mappings['merchant'] = get_dimension_key_mapping(
        conn, 'dim_merchant', 'merchant_name', 'merchant_key'
    )

    # Payment method mapping
    mappings['payment_method'] = get_dimension_key_mapping(
        conn, 'dim_payment_method', 'payment_method_name', 'payment_method_key'
    )

    # User mapping
    mappings['user'] = get_dimension_key_mapping(
        conn, 'dim_user', 'user_id', 'user_key'
    )

    # Date mapping (using date_key which is already an integer)
    mappings['date'] = get_dimension_key_mapping(
        conn, 'dim_date', 'date_key', 'date_key'
    )

    logger.info("All dimension key mappings retrieved successfully")

    return mappings


# ============================================================================
# Fact Data Enrichment
# ============================================================================

def enrich_fact_with_keys(
    fact_df: pd.DataFrame,
    dimension_mappings: dict[str, dict]
) -> pd.DataFrame:
    """
    Replace natural keys with surrogate keys from dimensions.

    Adds surrogate key columns to the fact DataFrame by looking up
    the natural keys in the dimension mappings.

    Args:
        fact_df: Fact data with natural keys (category, merchant, etc.)
        dimension_mappings: Dict of dicts for each dimension mapping

    Returns:
        DataFrame with surrogate keys (_key columns) added

    Raises:
        FactLoadError: If any natural keys cannot be mapped to surrogate keys

    Example:
        >>> enriched_df = enrich_fact_with_keys(fact_df, mappings)
        >>> print(enriched_df[['category', 'category_key']].head())
    """
    logger.info("Enriching fact data with surrogate keys...")

    try:
        # Make a copy to avoid modifying the original
        enriched_df = fact_df.copy()

        initial_count = len(enriched_df)

        # Map category to category_key
        enriched_df['category_key'] = enriched_df['category'].map(dimension_mappings['category'])
        missing_categories = enriched_df['category_key'].isnull().sum()
        if missing_categories > 0:
            missing_values = enriched_df[enriched_df['category_key'].isnull()]['category'].unique()
            error_msg = f"Found {missing_categories} transactions with unmapped categories: {missing_values[:5]}"
            logger.error(error_msg)
            raise FactLoadError(error_msg)

        # Map merchant to merchant_key
        enriched_df['merchant_key'] = enriched_df['merchant'].map(dimension_mappings['merchant'])
        missing_merchants = enriched_df['merchant_key'].isnull().sum()
        if missing_merchants > 0:
            missing_values = enriched_df[enriched_df['merchant_key'].isnull()]['merchant'].unique()
            error_msg = f"Found {missing_merchants} transactions with unmapped merchants: {missing_values[:5]}"
            logger.error(error_msg)
            raise FactLoadError(error_msg)

        # Map payment_method to payment_method_key
        enriched_df['payment_method_key'] = enriched_df['payment_method'].map(
            dimension_mappings['payment_method']
        )
        missing_payment = enriched_df['payment_method_key'].isnull().sum()
        if missing_payment > 0:
            missing_values = enriched_df[enriched_df['payment_method_key'].isnull()]['payment_method'].unique()
            error_msg = f"Found {missing_payment} transactions with unmapped payment methods: {missing_values}"
            logger.error(error_msg)
            raise FactLoadError(error_msg)

        # Map user_id to user_key
        enriched_df['user_key'] = enriched_df['user_id'].map(dimension_mappings['user'])
        missing_users = enriched_df['user_key'].isnull().sum()
        if missing_users > 0:
            missing_values = enriched_df[enriched_df['user_key'].isnull()]['user_id'].unique()
            error_msg = f"Found {missing_users} transactions with unmapped user_ids: {missing_values[:5]}"
            logger.error(error_msg)
            raise FactLoadError(error_msg)

        # Date_key is already in the correct format (YYYYMMDD integer)
        # Just verify it exists in the date dimension
        enriched_df['date_key_exists'] = enriched_df['date_key'].isin(dimension_mappings['date'].keys())
        missing_dates = (~enriched_df['date_key_exists']).sum()
        if missing_dates > 0:
            missing_values = enriched_df[~enriched_df['date_key_exists']]['date_key'].unique()
            error_msg = f"Found {missing_dates} transactions with unmapped date_keys: {missing_values[:5]}"
            logger.error(error_msg)
            raise FactLoadError(error_msg)

        # Drop the temporary validation column
        enriched_df = enriched_df.drop(columns=['date_key_exists'])

        # Convert surrogate keys to integers
        enriched_df['category_key'] = enriched_df['category_key'].astype(int)
        enriched_df['merchant_key'] = enriched_df['merchant_key'].astype(int)
        enriched_df['payment_method_key'] = enriched_df['payment_method_key'].astype(int)
        enriched_df['user_key'] = enriched_df['user_key'].astype(int)

        logger.info(f"Successfully enriched {len(enriched_df)} fact records with surrogate keys")
        logger.info(f"  Added columns: category_key, merchant_key, payment_method_key, user_key")

        return enriched_df

    except FactLoadError:
        # Re-raise FactLoadError as-is
        raise
    except Exception as e:
        error_msg = f"Unexpected error enriching fact data: {str(e)}"
        logger.error(error_msg)
        raise FactLoadError(error_msg) from e


# ============================================================================
# Fact Table Loading Functions
# ============================================================================

def check_existing_transactions(conn, transaction_ids: list) -> set:
    """
    Query database to find which transactions already exist.

    Enables incremental loading by identifying duplicate transactions.

    Args:
        conn: Active database connection
        transaction_ids: List of transaction IDs to check

    Returns:
        Set of transaction IDs that already exist in the database

    Example:
        >>> transaction_ids = ['txn_001', 'txn_002', 'txn_003']
        >>> existing = check_existing_transactions(conn, transaction_ids)
        >>> print(f"{len(existing)} transactions already exist")
    """
    if not transaction_ids:
        return set()

    try:
        logger.info(f"Checking for existing transactions (checking {len(transaction_ids)} IDs)...")

        cursor = conn.cursor()

        # Use ANY to check multiple values efficiently
        query = """
            SELECT transaction_id
            FROM fact_transactions
            WHERE transaction_id = ANY(%s)
        """

        cursor.execute(query, (transaction_ids,))
        existing_ids = {row[0] for row in cursor.fetchall()}

        logger.info(f"  Found {len(existing_ids)} existing transactions")

        cursor.close()
        return existing_ids

    except psycopg2.Error as e:
        error_msg = f"Failed to check existing transactions: {str(e)}"
        logger.error(error_msg)
        raise FactLoadError(error_msg) from e
    except Exception as e:
        error_msg = f"Unexpected error checking existing transactions: {str(e)}"
        logger.error(error_msg)
        raise FactLoadError(error_msg) from e


def load_fact_table(
    conn,
    fact_df: pd.DataFrame,
    table_name: str = 'fact_transactions'
) -> tuple[int, int]:
    """
    Load fact table with duplicate prevention.

    Uses transaction_id to avoid inserting duplicate records.
    Supports incremental loading by checking for existing transactions.

    Args:
        conn: Active database connection
        fact_df: Enriched fact DataFrame with surrogate keys
        table_name: Name of fact table (default: 'fact_transactions')

    Returns:
        Tuple of (inserted_count, skipped_count)

    Raises:
        FactLoadError: If fact loading fails

    Example:
        >>> inserted, skipped = load_fact_table(conn, enriched_df)
        >>> print(f"Inserted: {inserted}, Skipped: {skipped}")
    """
    try:
        if fact_df.empty:
            logger.warning(f"No data to load for {table_name}")
            return 0, 0

        logger.info(f"Loading {table_name}...")
        logger.info(f"  Records to process: {len(fact_df)}")

        cursor = conn.cursor()

        # Check for existing transactions
        transaction_ids = fact_df['transaction_id'].tolist()
        existing_ids = check_existing_transactions(conn, transaction_ids)

        # Filter out existing transactions
        if existing_ids:
            logger.info(f"  Filtering out {len(existing_ids)} existing transactions")
            new_transactions_df = fact_df[~fact_df['transaction_id'].isin(existing_ids)].copy()
        else:
            new_transactions_df = fact_df.copy()

        skipped_count = len(fact_df) - len(new_transactions_df)

        if new_transactions_df.empty:
            logger.info(f"  No new transactions to insert (all {skipped_count} already exist)")
            cursor.close()
            return 0, skipped_count

        # Build INSERT query
        insert_query = """
            INSERT INTO fact_transactions (
                transaction_id,
                date_key,
                category_key,
                merchant_key,
                payment_method_key,
                user_key,
                amount
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING
        """

        # Prepare records for insertion
        records = []
        for _, row in new_transactions_df.iterrows():
            values = (
                str(row['transaction_id']),
                int(row['date_key']),
                int(row['category_key']),
                int(row['merchant_key']),
                int(row['payment_method_key']),
                int(row['user_key']),
                float(row['amount'])
            )
            records.append(values)

        # Get count before insert
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count_before = cursor.fetchone()[0]

        # Execute batch insert
        logger.info(f"  Inserting {len(records)} new transactions in batches of {BATCH_SIZE}...")
        execute_batch(cursor, insert_query, records, page_size=BATCH_SIZE)

        # Get count after insert
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count_after = cursor.fetchone()[0]

        inserted_count = count_after - count_before

        logger.info(f"  Successfully inserted {inserted_count} new transactions")
        logger.info(f"  Skipped {skipped_count} existing transactions")
        logger.info(f"  Total records in {table_name}: {count_after}")

        cursor.close()
        return inserted_count, skipped_count

    except psycopg2.Error as e:
        error_msg = f"Failed to load fact table {table_name}: {str(e)}"
        logger.error(error_msg)
        raise FactLoadError(error_msg) from e
    except Exception as e:
        error_msg = f"Unexpected error loading fact table {table_name}: {str(e)}"
        logger.error(error_msg)
        raise FactLoadError(error_msg) from e


# ============================================================================
# Main Load Function
# ============================================================================

def load_data_warehouse(transformed_data: dict) -> dict:
    """
    Load transformed data into the PostgreSQL data warehouse.

    This is the main entry point for the load phase. It performs:
    1. Establishes database connection
    2. Begins transaction
    3. Loads all dimension tables
    4. Retrieves dimension key mappings
    5. Enriches fact data with surrogate keys
    6. Loads fact table
    7. Commits transaction (or rolls back on error)

    All operations are performed within a single database transaction
    to ensure data consistency (atomicity).

    Args:
        transformed_data: Dictionary with keys:
            - 'fact_data': Transaction fact data
            - 'dim_date': Date dimension
            - 'dim_category': Category dimension
            - 'dim_merchant': Merchant dimension
            - 'dim_payment_method': Payment method dimension
            - 'dim_user': User dimension

    Returns:
        Dictionary with loading statistics:
        {
            'dimensions_inserted': {
                'dim_date': int,
                'dim_category': int,
                'dim_merchant': int,
                'dim_payment_method': int,
                'dim_user': int
            },
            'facts_inserted': int,
            'facts_skipped': int
        }

    Raises:
        DatabaseConnectionError: If connection fails
        DimensionLoadError: If dimension loading fails
        FactLoadError: If fact loading fails
        ValueError: If input data is invalid

    Example:
        >>> from src.extract import extract_transactions
        >>> from src.transform import transform_transactions
        >>> from src.load import load_data_warehouse
        >>>
        >>> df_raw = extract_transactions("data/transactions.csv")
        >>> transformed = transform_transactions(df_raw)
        >>> results = load_data_warehouse(transformed)
        >>>
        >>> print(f"Dimensions inserted: {results['dimensions_inserted']}")
        >>> print(f"Facts inserted: {results['facts_inserted']}")
    """
    logger.info("=" * 80)
    logger.info("Starting load phase")
    logger.info("=" * 80)

    # Validate input
    required_keys = ['fact_data', 'dim_date', 'dim_category', 'dim_merchant', 'dim_payment_method', 'dim_user']
    missing_keys = [key for key in required_keys if key not in transformed_data]

    if missing_keys:
        error_msg = f"Missing required keys in transformed_data: {', '.join(missing_keys)}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    conn = None

    try:
        # Establish database connection
        conn = get_db_connection()
        conn.autocommit = False  # Manual transaction control
        logger.info("Transaction started")

        # Initialize statistics
        stats = {
            'dimensions_inserted': {},
            'facts_inserted': 0,
            'facts_skipped': 0
        }

        # ====================================================================
        # STEP 1: Load Dimension Tables
        # ====================================================================
        logger.info("")
        logger.info("STEP 1: Loading dimension tables...")
        logger.info("-" * 80)

        # Load dim_date (with all attributes)
        stats['dimensions_inserted']['dim_date'] = load_dim_date(
            conn, transformed_data['dim_date']
        )

        # Load dim_category
        stats['dimensions_inserted']['dim_category'] = load_dimension(
            conn,
            transformed_data['dim_category'],
            'dim_category',
            'category_name'
        )

        # Load dim_merchant
        stats['dimensions_inserted']['dim_merchant'] = load_dimension(
            conn,
            transformed_data['dim_merchant'],
            'dim_merchant',
            'merchant_name'
        )

        # Load dim_payment_method
        stats['dimensions_inserted']['dim_payment_method'] = load_dimension(
            conn,
            transformed_data['dim_payment_method'],
            'dim_payment_method',
            'payment_method_name'
        )

        # Load dim_user
        stats['dimensions_inserted']['dim_user'] = load_dimension(
            conn,
            transformed_data['dim_user'],
            'dim_user',
            'user_id'
        )

        logger.info("All dimension tables loaded successfully")

        # ====================================================================
        # STEP 2: Retrieve Dimension Key Mappings
        # ====================================================================
        logger.info("")
        logger.info("STEP 2: Retrieving dimension key mappings...")
        logger.info("-" * 80)

        dimension_mappings = get_all_dimension_mappings(conn)

        # ====================================================================
        # STEP 3: Enrich Fact Data with Surrogate Keys
        # ====================================================================
        logger.info("")
        logger.info("STEP 3: Enriching fact data with surrogate keys...")
        logger.info("-" * 80)

        enriched_fact_df = enrich_fact_with_keys(
            transformed_data['fact_data'],
            dimension_mappings
        )

        # ====================================================================
        # STEP 4: Load Fact Table
        # ====================================================================
        logger.info("")
        logger.info("STEP 4: Loading fact table...")
        logger.info("-" * 80)

        stats['facts_inserted'], stats['facts_skipped'] = load_fact_table(
            conn,
            enriched_fact_df
        )

        # ====================================================================
        # STEP 5: Commit Transaction
        # ====================================================================
        logger.info("")
        logger.info("Committing transaction...")
        conn.commit()
        logger.info("Transaction committed successfully")

        # ====================================================================
        # Log Summary
        # ====================================================================
        logger.info("")
        logger.info("=" * 80)
        logger.info("LOAD PHASE SUMMARY")
        logger.info("=" * 80)

        logger.info("Dimension Records Inserted:")
        for dim_name, count in stats['dimensions_inserted'].items():
            logger.info(f"  {dim_name}: {count}")

        total_dim_inserts = sum(stats['dimensions_inserted'].values())
        logger.info(f"  TOTAL DIMENSIONS: {total_dim_inserts}")

        logger.info("")
        logger.info("Fact Table Statistics:")
        logger.info(f"  New transactions inserted: {stats['facts_inserted']}")
        logger.info(f"  Existing transactions skipped: {stats['facts_skipped']}")
        logger.info(f"  Total transactions processed: {stats['facts_inserted'] + stats['facts_skipped']}")

        logger.info("=" * 80)
        logger.info("Load phase completed successfully")
        logger.info("=" * 80)

        return stats

    except (DatabaseConnectionError, DimensionLoadError, FactLoadError, ValueError):
        # Re-raise known exceptions as-is
        if conn and not conn.closed:
            conn.rollback()
            logger.error("Transaction rolled back due to error")
        raise

    except Exception as e:
        # Catch any unexpected errors
        if conn and not conn.closed:
            conn.rollback()
            logger.error("Transaction rolled back due to error")

        error_msg = f"Unexpected error during load phase: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Error type: {type(e).__name__}")
        raise Exception(error_msg) from e

    finally:
        # Ensure connection is closed
        if conn and not conn.closed:
            conn.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    """
    Test the load module directly with full ETL pipeline.

    This allows running the module standalone for testing:
    $ source venv/bin/activate.fish
    $ python -m src.load
    """
    from src.config import TRANSACTIONS_CSV
    from src.extract import extract_transactions
    from src.transform import transform_transactions

    print("\n" + "=" * 80)
    print("Testing Load Module (Full ETL Pipeline)")
    print("=" * 80 + "\n")

    try:
        # ====================================================================
        # Step 1: Extract
        # ====================================================================
        print("Step 1: Extracting data...")
        df_raw = extract_transactions(str(TRANSACTIONS_CSV))
        print(f"✓ Extracted {len(df_raw):,} transactions\n")

        # ====================================================================
        # Step 2: Transform
        # ====================================================================
        print("Step 2: Transforming data...")
        transformed_data = transform_transactions(df_raw)
        print(f"✓ Transformation completed\n")

        # ====================================================================
        # Step 3: Load
        # ====================================================================
        print("Step 3: Loading data into warehouse...")
        results = load_data_warehouse(transformed_data)
        print(f"✓ Load completed\n")

        # ====================================================================
        # Display Results
        # ====================================================================
        print("=" * 80)
        print("ETL PIPELINE RESULTS")
        print("=" * 80 + "\n")

        print("DIMENSIONS INSERTED:")
        for dim_name, count in results['dimensions_inserted'].items():
            print(f"  {dim_name}: {count}")

        print("\nFACT TABLE:")
        print(f"  Transactions inserted: {results['facts_inserted']}")
        print(f"  Transactions skipped (duplicates): {results['facts_skipped']}")
        print(f"  Total processed: {results['facts_inserted'] + results['facts_skipped']}")

        print("\n" + "=" * 80)
        print("✓ ETL Pipeline completed successfully")
        print("=" * 80 + "\n")

        # ====================================================================
        # Verify Data in Database
        # ====================================================================
        print("Verifying data in database...")
        with database_connection() as conn:
            cursor = conn.cursor()

            # Check record counts
            tables = [
                'dim_date', 'dim_category', 'dim_merchant',
                'dim_payment_method', 'dim_user', 'fact_transactions'
            ]

            print("\nTable Record Counts:")
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  {table}: {count:,}")

            # Sample fact records
            print("\nSample Fact Records (with dimension lookups):")
            cursor.execute("""
                SELECT
                    f.transaction_id,
                    d.date,
                    c.category_name,
                    m.merchant_name,
                    p.payment_method_name,
                    u.user_id,
                    f.amount
                FROM fact_transactions f
                JOIN dim_date d ON f.date_key = d.date_key
                JOIN dim_category c ON f.category_key = c.category_key
                JOIN dim_merchant m ON f.merchant_key = m.merchant_key
                JOIN dim_payment_method p ON f.payment_method_key = p.payment_method_key
                JOIN dim_user u ON f.user_key = u.user_key
                ORDER BY f.transaction_key
                LIMIT 5
            """)

            print("\n{:<38} {:<12} {:<15} {:<30} {:<18} {:<8} {:<10}".format(
                "Transaction ID", "Date", "Category", "Merchant", "Payment", "User", "Amount"
            ))
            print("-" * 140)

            for row in cursor.fetchall():
                print("{:<38} {:<12} {:<15} {:<30} {:<18} {:<8} ${:<9.2f}".format(
                    row[0], str(row[1]), row[2], row[3][:28], row[4], row[5], row[6]
                ))

            cursor.close()

        print("\n" + "=" * 80)
        print("✓ Load module test completed successfully")
        print("=" * 80 + "\n")

    except Exception as e:
        print(f"\n✗ Load failed: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

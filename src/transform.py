"""
Transform module for the ETL pipeline.

This module handles the transformation phase of the ETL process:
- Cleans and standardizes transaction data
- Validates business rules and data quality
- Derives date dimension attributes
- Creates dimension and fact DataFrames for star schema
- Provides comprehensive error handling and logging
"""

from datetime import datetime
from typing import Any
import pandas as pd

from src.logger import setup_logger

# Set up logger for this module
logger = setup_logger(__name__)

# ============================================================================
# Data Quality Configuration
# ============================================================================

ALLOWED_CATEGORIES = [
    'Groceries', 'Dining', 'Transportation', 'Entertainment',
    'Utilities', 'Shopping', 'Healthcare', 'Travel'
]

ALLOWED_PAYMENT_METHODS = [
    'Credit Card', 'Debit Card', 'Cash', 'Digital Wallet'
]

MIN_VALID_DATE = datetime(2020, 1, 1)  # Not before 2020
MAX_VALID_DATE = datetime.now()  # Not in future

MIN_AMOUNT = 0.01  # At least 1 cent
MAX_AMOUNT = 10000.00  # Max reasonable transaction


# ============================================================================
# Helper Functions for Data Standardization
# ============================================================================

def standardize_category(category: str) -> str:
    """
    Standardize category names (title case, trim).

    Args:
        category: Raw category string

    Returns:
        Standardized category name

    Example:
        >>> standardize_category("  groceries  ")
        'Groceries'
        >>> standardize_category("DINING")
        'Dining'
    """
    if pd.isna(category):
        return category
    return str(category).strip().title()


def standardize_merchant(merchant: str) -> str:
    """
    Standardize merchant names (title case, trim, remove extra spaces).

    Args:
        merchant: Raw merchant string

    Returns:
        Standardized merchant name

    Example:
        >>> standardize_merchant("  walmart   SUPERCENTER  ")
        'Walmart Supercenter'
    """
    if pd.isna(merchant):
        return merchant

    # Convert to string, strip, and remove extra spaces
    merchant = str(merchant).strip()
    merchant = ' '.join(merchant.split())  # Remove extra spaces

    return merchant.title()


def standardize_payment_method(payment_method: str) -> str:
    """
    Standardize payment method names (title case, trim).

    Args:
        payment_method: Raw payment method string

    Returns:
        Standardized payment method name

    Example:
        >>> standardize_payment_method("credit card")
        'Credit Card'
    """
    if pd.isna(payment_method):
        return payment_method
    return str(payment_method).strip().title()


# ============================================================================
# Data Cleaning Functions
# ============================================================================

def clean_transaction_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize transaction data.

    Performs the following operations:
    - Removes duplicate transactions (by transaction_id)
    - Trims whitespace from string columns
    - Standardizes text casing
    - Logs cleaning statistics

    Args:
        df: Raw transaction DataFrame

    Returns:
        Cleaned DataFrame

    Example:
        >>> df_clean = clean_transaction_data(df_raw)
        >>> print(f"Removed {len(df_raw) - len(df_clean)} duplicates")
    """
    logger.info("Starting data cleaning...")
    initial_count = len(df)

    # Make a copy to avoid modifying the original
    df = df.copy()

    # Remove duplicate transaction_ids (keep first occurrence)
    duplicates_before = df.duplicated(subset=['transaction_id']).sum()
    if duplicates_before > 0:
        logger.warning(f"Found {duplicates_before} duplicate transaction_ids")
        df = df.drop_duplicates(subset=['transaction_id'], keep='first')
        logger.info(f"Removed {duplicates_before} duplicate transactions (kept first occurrence)")

    # Trim whitespace from string columns
    string_columns = df.select_dtypes(include=['object']).columns
    for col in string_columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)

    # Standardize text casing
    if 'category' in df.columns:
        df['category'] = df['category'].apply(standardize_category)
        logger.info("Standardized category names to title case")

    if 'merchant' in df.columns:
        df['merchant'] = df['merchant'].apply(standardize_merchant)
        logger.info("Standardized merchant names to title case")

    if 'payment_method' in df.columns:
        df['payment_method'] = df['payment_method'].apply(standardize_payment_method)
        logger.info("Standardized payment method names to title case")

    final_count = len(df)
    logger.info(f"Data cleaning completed: {initial_count} → {final_count} rows")

    return df


# ============================================================================
# Data Validation Functions
# ============================================================================

def validate_transaction_data(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """
    Validate business rules and data quality.

    Validates:
    - Amount > 0 and within reasonable range
    - Valid dates (not future, not too old)
    - Category in allowed list
    - Payment method in allowed list
    - No nulls in required fields
    - User ID is valid integer

    Args:
        df: Cleaned transaction DataFrame

    Returns:
        Tuple of (valid_df, list_of_issues)
        - valid_df: DataFrame with only valid records
        - list_of_issues: List of validation issue descriptions

    Example:
        >>> valid_df, issues = validate_transaction_data(df_clean)
        >>> for issue in issues:
        >>>     print(f"Issue: {issue}")
    """
    logger.info("Starting data validation...")
    initial_count = len(df)
    issues = []

    # Make a copy to track validation
    df = df.copy()
    df['is_valid'] = True

    # Check for null values in required fields
    required_fields = ['transaction_id', 'date', 'category', 'amount', 'merchant', 'payment_method', 'user_id']
    for field in required_fields:
        null_count = df[field].isnull().sum()
        if null_count > 0:
            issue = f"Found {null_count} null values in '{field}' column"
            issues.append(issue)
            logger.warning(issue)
            df.loc[df[field].isnull(), 'is_valid'] = False

    # Validate amount
    try:
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

        # Check for amounts <= 0
        invalid_amounts = (df['amount'] <= 0) | (df['amount'].isnull())
        invalid_count = invalid_amounts.sum()
        if invalid_count > 0:
            issue = f"Found {invalid_count} transactions with invalid amounts (≤ 0 or non-numeric)"
            issues.append(issue)
            logger.warning(issue)
            df.loc[invalid_amounts, 'is_valid'] = False

        # Check for amounts above maximum
        too_large = df['amount'] > MAX_AMOUNT
        too_large_count = too_large.sum()
        if too_large_count > 0:
            issue = f"Found {too_large_count} transactions with amounts > ${MAX_AMOUNT:,.2f}"
            issues.append(issue)
            logger.warning(issue)
            df.loc[too_large, 'is_valid'] = False

        # Round amounts to 2 decimal places
        df['amount'] = df['amount'].round(2)

    except Exception as e:
        issue = f"Error validating amounts: {str(e)}"
        issues.append(issue)
        logger.error(issue)

    # Validate dates
    try:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

        # Check for invalid date parsing
        invalid_dates = df['date'].isnull()
        invalid_date_count = invalid_dates.sum()
        if invalid_date_count > 0:
            issue = f"Found {invalid_date_count} transactions with invalid date format"
            issues.append(issue)
            logger.warning(issue)
            df.loc[invalid_dates, 'is_valid'] = False

        # Check for dates too old
        valid_dates = ~invalid_dates
        too_old = valid_dates & (df['date'] < MIN_VALID_DATE)
        too_old_count = too_old.sum()
        if too_old_count > 0:
            issue = f"Found {too_old_count} transactions with dates before {MIN_VALID_DATE.strftime('%Y-%m-%d')}"
            issues.append(issue)
            logger.warning(issue)
            df.loc[too_old, 'is_valid'] = False

        # Check for future dates
        in_future = valid_dates & (df['date'] > MAX_VALID_DATE)
        future_count = in_future.sum()
        if future_count > 0:
            issue = f"Found {future_count} transactions with future dates"
            issues.append(issue)
            logger.warning(issue)
            df.loc[in_future, 'is_valid'] = False

    except Exception as e:
        issue = f"Error validating dates: {str(e)}"
        issues.append(issue)
        logger.error(issue)

    # Validate category
    invalid_categories = ~df['category'].isin(ALLOWED_CATEGORIES)
    invalid_cat_count = invalid_categories.sum()
    if invalid_cat_count > 0:
        unique_invalid = df.loc[invalid_categories, 'category'].unique()
        issue = f"Found {invalid_cat_count} transactions with invalid categories: {', '.join(map(str, unique_invalid[:5]))}"
        if len(unique_invalid) > 5:
            issue += f" ... and {len(unique_invalid) - 5} more"
        issues.append(issue)
        logger.warning(issue)
        df.loc[invalid_categories, 'is_valid'] = False

    # Validate payment method
    invalid_payment = ~df['payment_method'].isin(ALLOWED_PAYMENT_METHODS)
    invalid_payment_count = invalid_payment.sum()
    if invalid_payment_count > 0:
        unique_invalid = df.loc[invalid_payment, 'payment_method'].unique()
        issue = f"Found {invalid_payment_count} transactions with invalid payment methods: {', '.join(map(str, unique_invalid))}"
        issues.append(issue)
        logger.warning(issue)
        df.loc[invalid_payment, 'is_valid'] = False

    # Validate user_id is integer
    try:
        df['user_id'] = pd.to_numeric(df['user_id'], errors='coerce')
        invalid_user_ids = df['user_id'].isnull()
        invalid_user_count = invalid_user_ids.sum()
        if invalid_user_count > 0:
            issue = f"Found {invalid_user_count} transactions with invalid user_id (non-integer)"
            issues.append(issue)
            logger.warning(issue)
            df.loc[invalid_user_ids, 'is_valid'] = False

        # Convert to int (for valid ones) - use Int64 to handle potential nulls gracefully
        if not invalid_user_ids.all():
            df['user_id'] = df['user_id'].astype('Int64')

    except Exception as e:
        issue = f"Error validating user_id: {str(e)}"
        issues.append(issue)
        logger.error(issue)

    # Filter to only valid records
    valid_df = df[df['is_valid']].drop(columns=['is_valid']).copy()
    invalid_count = initial_count - len(valid_df)

    if invalid_count > 0:
        logger.warning(f"Filtered out {invalid_count} invalid transactions")
    else:
        logger.info("All transactions passed validation")

    logger.info(f"Data validation completed: {initial_count} → {len(valid_df)} valid rows")

    return valid_df, issues


# ============================================================================
# Date Dimension Functions
# ============================================================================

def derive_date_attributes(date_series: pd.Series) -> pd.DataFrame:
    """
    Calculate all date dimension attributes.

    Derives comprehensive date attributes for the date dimension table:
    - Date components (year, quarter, month, day)
    - Date names (month_name, day_name)
    - Calendar positions (day_of_week, week_of_year)
    - Date key (YYYYMMDD integer format)
    - Weekend flag

    Args:
        date_series: pandas Series of datetime objects

    Returns:
        DataFrame with columns:
        - date_key (YYYYMMDD integer)
        - date (datetime)
        - year, quarter, month, day (integers)
        - month_name, day_name (strings)
        - day_of_week (1=Mon, 7=Sun)
        - week_of_year (integer)
        - is_weekend (boolean)

    Example:
        >>> dates = pd.Series(pd.date_range('2023-01-01', periods=3))
        >>> date_dim = derive_date_attributes(dates)
        >>> print(date_dim[['date_key', 'day_name', 'is_weekend']])
    """
    logger.info("Deriving date dimension attributes...")

    # Create DataFrame from unique dates
    unique_dates = pd.DataFrame({'date': date_series.unique()})
    unique_dates = unique_dates.sort_values('date').reset_index(drop=True)

    # Generate date_key as YYYYMMDD integer
    unique_dates['date_key'] = unique_dates['date'].dt.strftime('%Y%m%d').astype(int)

    # Extract date components
    unique_dates['year'] = unique_dates['date'].dt.year
    unique_dates['quarter'] = unique_dates['date'].dt.quarter
    unique_dates['month'] = unique_dates['date'].dt.month
    unique_dates['day'] = unique_dates['date'].dt.day

    # Get date names
    unique_dates['month_name'] = unique_dates['date'].dt.month_name()
    unique_dates['day_name'] = unique_dates['date'].dt.day_name()

    # Get calendar positions
    # ISO weekday: Monday=1, Sunday=7
    unique_dates['day_of_week'] = unique_dates['date'].dt.isocalendar().day
    unique_dates['week_of_year'] = unique_dates['date'].dt.isocalendar().week

    # Calculate is_weekend (Saturday=6, Sunday=7 in ISO format)
    unique_dates['is_weekend'] = unique_dates['day_of_week'].isin([6, 7])

    logger.info(f"Derived attributes for {len(unique_dates)} unique dates")
    logger.info(f"Date range: {unique_dates['date'].min().strftime('%Y-%m-%d')} to {unique_dates['date'].max().strftime('%Y-%m-%d')}")

    return unique_dates


# ============================================================================
# Dimension Creation Functions
# ============================================================================

def create_dimension_data(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Extract unique dimension values from fact data.

    Creates dimension DataFrames for:
    - dim_date: Unique dates with all derived attributes
    - dim_category: Unique spending categories
    - dim_merchant: Unique merchant names
    - dim_payment_method: Unique payment methods
    - dim_user: Unique user IDs

    Args:
        df: Validated transaction DataFrame

    Returns:
        Dictionary with keys: 'dim_date', 'dim_category', 'dim_merchant',
        'dim_payment_method', 'dim_user', each containing a DataFrame

    Example:
        >>> dimensions = create_dimension_data(df_valid)
        >>> print(f"Categories: {len(dimensions['dim_category'])}")
    """
    logger.info("Creating dimension DataFrames...")

    dimensions = {}

    # Date dimension (with all attributes)
    dimensions['dim_date'] = derive_date_attributes(df['date'])
    logger.info(f"Created dim_date with {len(dimensions['dim_date'])} unique dates")

    # Category dimension
    unique_categories = df['category'].unique()
    dimensions['dim_category'] = pd.DataFrame({
        'category_name': sorted(unique_categories)
    })
    logger.info(f"Created dim_category with {len(dimensions['dim_category'])} categories")

    # Merchant dimension
    unique_merchants = df['merchant'].unique()
    dimensions['dim_merchant'] = pd.DataFrame({
        'merchant_name': sorted(unique_merchants)
    })
    logger.info(f"Created dim_merchant with {len(dimensions['dim_merchant'])} merchants")

    # Payment method dimension
    unique_payment_methods = df['payment_method'].unique()
    dimensions['dim_payment_method'] = pd.DataFrame({
        'payment_method_name': sorted(unique_payment_methods)
    })
    logger.info(f"Created dim_payment_method with {len(dimensions['dim_payment_method'])} payment methods")

    # User dimension
    unique_users = df['user_id'].unique()
    dimensions['dim_user'] = pd.DataFrame({
        'user_id': sorted(unique_users)
    })
    logger.info(f"Created dim_user with {len(dimensions['dim_user'])} users")

    return dimensions


# ============================================================================
# Logging and Reporting Functions
# ============================================================================

def log_transformation_summary(
    original_count: int,
    final_count: int,
    duplicates_removed: int,
    invalid_removed: int,
    issues: list[str]
) -> None:
    """
    Log comprehensive transformation statistics.

    Args:
        original_count: Initial number of records
        final_count: Final number of valid records
        duplicates_removed: Number of duplicate records removed
        invalid_removed: Number of invalid records filtered out
        issues: List of data quality issues found

    Example:
        >>> log_transformation_summary(10000, 9950, 30, 20, ["Issue 1", "Issue 2"])
    """
    logger.info("=" * 80)
    logger.info("TRANSFORMATION SUMMARY")
    logger.info("=" * 80)

    logger.info(f"Original record count: {original_count:,}")
    logger.info(f"Duplicates removed: {duplicates_removed:,}")
    logger.info(f"Invalid records filtered: {invalid_removed:,}")
    logger.info(f"Final valid record count: {final_count:,}")

    if original_count > 0:
        success_rate = (final_count / original_count) * 100
        logger.info(f"Success rate: {success_rate:.2f}%")

    if issues:
        logger.warning(f"Total data quality issues found: {len(issues)}")
        for idx, issue in enumerate(issues, 1):
            logger.warning(f"  {idx}. {issue}")
    else:
        logger.info("No data quality issues found")

    logger.info("=" * 80)


# ============================================================================
# Main Transformation Function
# ============================================================================

def transform_transactions(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Transform transaction data for star schema loading.

    This is the main entry point for the transformation phase. It performs:
    1. Data cleaning (duplicates, whitespace, standardization)
    2. Data type conversion (dates, amounts, user_id)
    3. Data validation (business rules, quality checks)
    4. Date attribute derivation
    5. Dimension extraction
    6. Fact data preparation

    Args:
        df: Raw transaction DataFrame from extract phase

    Returns:
        Dictionary containing:
        - 'fact_data': Transformed fact table data
        - 'dim_date': Date dimension with attributes
        - 'dim_category': Category dimension
        - 'dim_merchant': Merchant dimension
        - 'dim_payment_method': Payment method dimension
        - 'dim_user': User dimension

    Raises:
        ValueError: If the input DataFrame is empty or invalid
        Exception: For unexpected errors during transformation

    Example:
        >>> from src.extract import extract_transactions
        >>> from src.transform import transform_transactions
        >>>
        >>> df_raw = extract_transactions("data/transactions.csv")
        >>> transformed = transform_transactions(df_raw)
        >>>
        >>> fact_df = transformed['fact_data']
        >>> print(f"Fact records: {len(fact_df)}")
    """
    logger.info("=" * 80)
    logger.info("Starting transformation phase")
    logger.info("=" * 80)

    try:
        # Validate input
        if df is None or df.empty:
            error_msg = "Input DataFrame is empty or None"
            logger.error(error_msg)
            raise ValueError(error_msg)

        original_count = len(df)
        logger.info(f"Input record count: {original_count:,}")

        # Step 1: Clean data
        df_clean = clean_transaction_data(df)
        duplicates_removed = original_count - len(df_clean)

        # Step 2: Validate data
        df_valid, issues = validate_transaction_data(df_clean)
        invalid_removed = len(df_clean) - len(df_valid)

        if df_valid.empty:
            error_msg = "No valid records remaining after transformation"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Step 3: Create dimensions
        dimensions = create_dimension_data(df_valid)

        # Step 4: Prepare fact data
        logger.info("Preparing fact table data...")

        # Create fact DataFrame with necessary columns
        fact_data = df_valid[[
            'transaction_id',
            'date',
            'category',
            'merchant',
            'payment_method',
            'user_id',
            'amount'
        ]].copy()

        # Add date_key for dimension matching
        fact_data['date_key'] = fact_data['date'].dt.strftime('%Y%m%d').astype(int)

        logger.info(f"Prepared fact table with {len(fact_data)} records")

        # Step 5: Log transformation summary
        log_transformation_summary(
            original_count=original_count,
            final_count=len(df_valid),
            duplicates_removed=duplicates_removed,
            invalid_removed=invalid_removed,
            issues=issues
        )

        # Step 6: Return all transformed data
        result = {
            'fact_data': fact_data,
            'dim_date': dimensions['dim_date'],
            'dim_category': dimensions['dim_category'],
            'dim_merchant': dimensions['dim_merchant'],
            'dim_payment_method': dimensions['dim_payment_method'],
            'dim_user': dimensions['dim_user']
        }

        logger.info("=" * 80)
        logger.info("Transformation completed successfully")
        logger.info("=" * 80)

        return result

    except ValueError:
        # Re-raise ValueError as-is
        raise

    except Exception as e:
        # Catch any unexpected errors
        error_msg = f"Unexpected error during transformation: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Error type: {type(e).__name__}")
        raise Exception(error_msg) from e


if __name__ == "__main__":
    """
    Test the transformation module directly.

    This allows running the module standalone for testing:
    $ source venv/bin/activate.fish
    $ python -m src.transform
    """
    from src.config import TRANSACTIONS_CSV
    from src.extract import extract_transactions

    print("\n" + "=" * 80)
    print("Testing Transform Module")
    print("=" * 80 + "\n")

    try:
        # Extract data
        print("Step 1: Extracting data...")
        df_raw = extract_transactions(str(TRANSACTIONS_CSV))
        print(f"✓ Extracted {len(df_raw):,} transactions\n")

        # Transform data
        print("Step 2: Transforming data...")
        transformed_data = transform_transactions(df_raw)
        print(f"✓ Transformation completed\n")

        # Display results
        print("=" * 80)
        print("TRANSFORMATION RESULTS")
        print("=" * 80 + "\n")

        fact_df = transformed_data['fact_data']
        print(f"Fact Table Records: {len(fact_df):,}")
        print(f"Columns: {', '.join(fact_df.columns.tolist())}")
        print(f"\nSample fact data:")
        print(fact_df.head(3))

        print("\n" + "-" * 80 + "\n")

        dim_date = transformed_data['dim_date']
        print(f"Date Dimension: {len(dim_date):,} unique dates")
        print(f"Date range: {dim_date['date'].min().strftime('%Y-%m-%d')} to {dim_date['date'].max().strftime('%Y-%m-%d')}")
        print(f"\nSample date dimension:")
        print(dim_date[['date_key', 'date', 'year', 'month', 'day_name', 'is_weekend']].head(3))

        print("\n" + "-" * 80 + "\n")

        dim_category = transformed_data['dim_category']
        print(f"Category Dimension: {len(dim_category)} categories")
        print(f"Categories: {', '.join(dim_category['category_name'].tolist())}")

        print("\n" + "-" * 80 + "\n")

        dim_merchant = transformed_data['dim_merchant']
        print(f"Merchant Dimension: {len(dim_merchant):,} merchants")
        print(f"Sample merchants:")
        print(dim_merchant.head(5))

        print("\n" + "-" * 80 + "\n")

        dim_payment = transformed_data['dim_payment_method']
        print(f"Payment Method Dimension: {len(dim_payment)} payment methods")
        print(f"Payment methods: {', '.join(dim_payment['payment_method_name'].tolist())}")

        print("\n" + "-" * 80 + "\n")

        dim_user = transformed_data['dim_user']
        print(f"User Dimension: {len(dim_user)} users")
        print(f"User ID range: {dim_user['user_id'].min()} to {dim_user['user_id'].max()}")

        print("\n" + "=" * 80)
        print("✓ Transform module test completed successfully")
        print("=" * 80 + "\n")

    except Exception as e:
        print(f"\n✗ Transformation failed: {str(e)}")
        raise

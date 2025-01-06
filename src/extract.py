"""
Extract module for the ETL pipeline.

This module handles the extraction phase of the ETL process:
- Reads CSV transaction data
- Performs basic validation
- Returns pandas DataFrame for transformation
- Provides comprehensive error handling and logging
"""

import os
from pathlib import Path
from datetime import datetime
import pandas as pd

from src.logger import setup_logger
from src.config import REQUIRED_CSV_COLUMNS

# Set up logger for this module
logger = setup_logger(__name__)


def get_file_info(file_path: str) -> dict:
    """
    Get metadata about the CSV file.

    Args:
        file_path: Path to the CSV file

    Returns:
        Dictionary containing file metadata:
        - file_size: Size in bytes
        - file_size_mb: Size in megabytes (formatted)
        - modified_time: Last modified timestamp
        - exists: Whether file exists

    Example:
        >>> info = get_file_info("data/transactions.csv")
        >>> print(f"File size: {info['file_size_mb']}")
    """
    file_path_obj = Path(file_path)

    if not file_path_obj.exists():
        return {
            "exists": False,
            "file_size": 0,
            "file_size_mb": "0.00 MB",
            "modified_time": None
        }

    file_size = os.path.getsize(file_path)
    file_size_mb = file_size / (1024 * 1024)
    modified_timestamp = os.path.getmtime(file_path)
    modified_time = datetime.fromtimestamp(modified_timestamp)

    return {
        "exists": True,
        "file_size": file_size,
        "file_size_mb": f"{file_size_mb:.2f} MB",
        "modified_time": modified_time.strftime("%Y-%m-%d %H:%M:%S")
    }


def validate_csv_structure(df: pd.DataFrame, required_columns: list) -> tuple[bool, str]:
    """
    Validate DataFrame has required structure.

    Performs the following checks:
    1. DataFrame is not empty
    2. All required columns are present
    3. No completely empty rows exist

    Args:
        df: pandas DataFrame to validate
        required_columns: List of required column names

    Returns:
        Tuple of (is_valid: bool, error_message: str)
        - If valid: (True, "")
        - If invalid: (False, "description of error")

    Example:
        >>> is_valid, error = validate_csv_structure(df, ["id", "name"])
        >>> if not is_valid:
        >>>     print(f"Validation failed: {error}")
    """
    # Check if DataFrame is empty
    if df.empty:
        return False, "DataFrame is empty (0 rows)"

    if len(df) == 0:
        return False, "DataFrame has 0 rows"

    # Check for required columns
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        return False, f"Missing required columns: {', '.join(sorted(missing_columns))}"

    # Check for completely empty rows (all values are NaN)
    empty_rows = df.isnull().all(axis=1).sum()
    if empty_rows > 0:
        logger.warning(f"Found {empty_rows} completely empty rows in the data")

    return True, ""


def extract_transactions(file_path: str) -> pd.DataFrame:
    """
    Extract transaction data from CSV file.

    This function is the main entry point for the extraction phase. It:
    1. Validates the file exists and is readable
    2. Reads the CSV file into a pandas DataFrame
    3. Performs structural validation
    4. Logs extraction statistics
    5. Returns the DataFrame for transformation

    Args:
        file_path: Path to the CSV file containing transaction data

    Returns:
        pandas DataFrame containing the extracted transaction data

    Raises:
        FileNotFoundError: If the file doesn't exist at the specified path
        pd.errors.EmptyDataError: If the CSV file is empty
        pd.errors.ParserError: If the CSV file is malformed
        ValueError: If required columns are missing
        Exception: For any other unexpected errors

    Example:
        >>> from src.extract import extract_transactions
        >>> from src.config import TRANSACTIONS_CSV
        >>>
        >>> df = extract_transactions(str(TRANSACTIONS_CSV))
        >>> print(f"Extracted {len(df)} transactions")
        >>> print(df.head())
    """
    logger.info("=" * 80)
    logger.info("Starting extraction phase")
    logger.info(f"File path: {file_path}")

    try:
        # Get file information
        file_info = get_file_info(file_path)

        # Check if file exists
        if not file_info["exists"]:
            error_msg = f"File not found: {file_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        logger.info(f"File size: {file_info['file_size_mb']}")
        logger.info(f"Last modified: {file_info['modified_time']}")

        # Read CSV file
        logger.info("Reading CSV file...")
        try:
            df = pd.read_csv(file_path)
        except pd.errors.EmptyDataError as e:
            error_msg = f"CSV file is empty: {file_path}"
            logger.error(error_msg)
            raise pd.errors.EmptyDataError(error_msg) from e
        except pd.errors.ParserError as e:
            error_msg = f"CSV file is malformed: {file_path}. Error: {str(e)}"
            logger.error(error_msg)
            raise pd.errors.ParserError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error reading CSV file: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg) from e

        logger.info(f"Successfully read CSV file")
        logger.info(f"Initial row count: {len(df)}")
        logger.info(f"Column count: {len(df.columns)}")
        logger.info(f"Columns found: {', '.join(df.columns.tolist())}")

        # Validate CSV structure
        logger.info("Validating CSV structure...")
        is_valid, error_message = validate_csv_structure(df, REQUIRED_CSV_COLUMNS)

        if not is_valid:
            error_msg = f"CSV validation failed: {error_message}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        logger.info("CSV structure validation passed")

        # Log data quality statistics
        null_counts = df.isnull().sum()
        columns_with_nulls = null_counts[null_counts > 0]

        if len(columns_with_nulls) > 0:
            logger.warning("Found null values in the following columns:")
            for column, count in columns_with_nulls.items():
                percentage = (count / len(df)) * 100
                logger.warning(f"  - {column}: {count} nulls ({percentage:.2f}%)")
        else:
            logger.info("No null values found in the data")

        # Log memory usage
        memory_usage_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
        logger.info(f"DataFrame memory usage: {memory_usage_mb:.2f} MB")

        # Extraction complete
        logger.info("=" * 80)
        logger.info(f"Extraction completed successfully")
        logger.info(f"Total rows extracted: {len(df):,}")
        logger.info(f"Total columns: {len(df.columns)}")
        logger.info("=" * 80)

        return df

    except FileNotFoundError:
        # Re-raise FileNotFoundError as-is
        raise

    except (pd.errors.EmptyDataError, pd.errors.ParserError, ValueError):
        # Re-raise pandas and validation errors as-is
        raise

    except Exception as e:
        # Catch any other unexpected errors
        error_msg = f"Unexpected error during extraction: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Error type: {type(e).__name__}")
        raise Exception(error_msg) from e


if __name__ == "__main__":
    """
    Test the extraction module directly.

    This allows running the module standalone for testing:
    $ source venv/bin/activate.fish
    $ python -m src.extract
    """
    from src.config import TRANSACTIONS_CSV

    print("\n" + "=" * 80)
    print("Testing Extract Module")
    print("=" * 80 + "\n")

    try:
        df = extract_transactions(str(TRANSACTIONS_CSV))
        print(f"\n✓ Successfully extracted {len(df):,} transactions")
        print(f"\nFirst 5 rows:")
        print(df.head())
        print(f"\nDataFrame info:")
        print(df.info())

    except Exception as e:
        print(f"\n✗ Extraction failed: {str(e)}")
        raise

"""
Shared pytest fixtures and configuration for ETL pipeline tests.

This module provides reusable fixtures for testing the extract and transform modules,
including test data, file creation, and configuration.
"""

import pytest
import pandas as pd
import logging
from datetime import datetime, timedelta

from src.config import REQUIRED_CSV_COLUMNS


# ============================================================================
# Configuration Fixtures
# ============================================================================

@pytest.fixture(scope="module")
def required_columns():
    """
    Provides required CSV columns for validation testing.

    Returns:
        list[str]: List of required column names
    """
    return REQUIRED_CSV_COLUMNS


# ============================================================================
# Data Fixtures
# ============================================================================

@pytest.fixture
def valid_transaction_data():
    """
    Provides a valid transaction DataFrame for testing.

    Returns:
        pd.DataFrame: DataFrame with all required columns and valid data
    """
    return pd.DataFrame({
        "transaction_id": ["TXN001", "TXN002", "TXN003"],
        "date": ["2023-01-01", "2023-01-02", "2023-01-03"],
        "category": ["Food", "Transport", "Entertainment"],
        "amount": [10.50, 20.75, 30.00],
        "merchant": ["Store A", "Store B", "Store C"],
        "payment_method": ["Credit Card", "Debit Card", "Cash"],
        "user_id": [1, 2, 3]
    })


@pytest.fixture
def empty_dataframe():
    """
    Provides an empty DataFrame for testing edge cases.

    Returns:
        pd.DataFrame: Empty DataFrame with no columns or rows
    """
    return pd.DataFrame()


@pytest.fixture
def incomplete_dataframe():
    """
    Provides a DataFrame missing required columns for testing validation.

    Returns:
        pd.DataFrame: DataFrame with only transaction_id and amount columns
    """
    return pd.DataFrame({
        "transaction_id": ["TXN001", "TXN002"],
        "amount": [10.0, 20.0]
    })


# ============================================================================
# File Fixtures
# ============================================================================

@pytest.fixture
def valid_csv_file(tmp_path, valid_transaction_data):
    """
    Creates a temporary valid CSV file for testing.

    Args:
        tmp_path: pytest's temporary path fixture
        valid_transaction_data: Fixture providing valid DataFrame

    Returns:
        str: Path to the created CSV file
    """
    file_path = tmp_path / "valid_transactions.csv"
    valid_transaction_data.to_csv(file_path, index=False)
    return str(file_path)


@pytest.fixture
def empty_csv_file(tmp_path):
    """
    Creates a temporary empty CSV file for testing.

    Args:
        tmp_path: pytest's temporary path fixture

    Returns:
        str: Path to the created empty CSV file
    """
    file_path = tmp_path / "empty.csv"
    file_path.touch()
    return str(file_path)


@pytest.fixture
def incomplete_csv_file(tmp_path):
    """
    Creates a temporary CSV file with missing required columns.

    Args:
        tmp_path: pytest's temporary path fixture

    Returns:
        str: Path to the created incomplete CSV file
    """
    file_path = tmp_path / "incomplete.csv"
    file_path.write_text(
        "transaction_id,amount\n"
        "TXN001,100.0\n"
        "TXN002,200.0\n"
    )
    return str(file_path)


@pytest.fixture
def nonexistent_file_path(tmp_path):
    """
    Provides a path to a file that does not exist.

    Args:
        tmp_path: pytest's temporary path fixture

    Returns:
        str: Path to a nonexistent CSV file
    """
    return str(tmp_path / "nonexistent.csv")


# ============================================================================
# Logging Fixtures
# ============================================================================

@pytest.fixture
def disable_logging():
    """
    Temporarily disables logging for cleaner test output.

    Yields:
        None

    Note:
        Automatically re-enables logging after test completes
    """
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


# ============================================================================
# Transform Module Fixtures
# ============================================================================

@pytest.fixture
def clean_transform_data():
    """
    Provides clean transaction data for transform testing.

    Returns:
        pd.DataFrame: Clean transaction data with valid values
    """
    return pd.DataFrame({
        "transaction_id": ["TXN001", "TXN002", "TXN003", "TXN004"],
        "date": ["2023-06-15", "2023-06-16", "2023-06-17", "2023-06-18"],
        "category": ["Groceries", "Dining", "Transportation", "Entertainment"],
        "amount": [50.00, 35.50, 15.75, 100.00],
        "merchant": ["Whole Foods", "Starbucks", "Uber", "Netflix"],
        "payment_method": ["Credit Card", "Debit Card", "Digital Wallet", "Credit Card"],
        "user_id": [1, 2, 1, 3]
    })


@pytest.fixture
def dirty_transform_data():
    """
    Provides dirty transaction data with various issues for transform testing.

    Returns:
        pd.DataFrame: Transaction data with whitespace, mixed case, and duplicates
    """
    return pd.DataFrame({
        "transaction_id": ["TXN001", "TXN002", "TXN001", "TXN003"],  # Duplicate
        "date": ["2023-06-15", "2023-06-16", "2023-06-15", "2023-06-17"],
        "category": ["  groceries  ", "DINING", "groceries", "entertainment  "],
        "amount": [50.00, 35.50, 50.00, 100.00],
        "merchant": ["  whole   foods  ", "STARBUCKS", "whole foods", "  netflix  "],
        "payment_method": ["credit card", "DEBIT CARD", "credit card", "Credit Card"],
        "user_id": [1, 2, 1, 3]
    })


@pytest.fixture
def invalid_transform_data():
    """
    Provides transaction data with validation issues.

    Returns:
        pd.DataFrame: Transaction data with invalid amounts, dates, categories, etc.
    """
    future_date = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
    old_date = "2019-01-01"  # Before MIN_VALID_DATE (2020-01-01)

    return pd.DataFrame({
        "transaction_id": ["TXN001", "TXN002", "TXN003", "TXN004", "TXN005", "TXN006"],
        "date": ["2023-06-15", future_date, old_date, "2023-06-18", "2023-06-19", "2023-06-20"],
        "category": ["Groceries", "Dining", "InvalidCategory", "Entertainment", "Shopping", "Groceries"],
        "amount": [50.00, -10.00, 100.00, 15000.00, 0.00, 25.00],  # Negative, too large, zero
        "merchant": ["Whole Foods", "Starbucks", "Target", "Netflix", "Amazon", "Trader Joes"],
        "payment_method": ["Credit Card", "InvalidPayment", "Cash", "Digital Wallet", "Credit Card", "Debit Card"],
        "user_id": [1, 2, 3, 4, "invalid", 6]  # Invalid user_id
    })


@pytest.fixture
def sample_date_series():
    """
    Provides a sample date series for date dimension testing.

    Returns:
        pd.Series: Series of datetime objects
    """
    dates = pd.date_range(start="2023-06-15", end="2023-06-20", freq="D")
    return pd.Series(dates)


@pytest.fixture
def weekend_date_series():
    """
    Provides dates that include weekends for testing weekend detection.

    Returns:
        pd.Series: Series including Saturday and Sunday
    """
    # June 17, 2023 is Saturday, June 18 is Sunday
    dates = pd.to_datetime(["2023-06-16", "2023-06-17", "2023-06-18", "2023-06-19"])
    return pd.Series(dates)


@pytest.fixture
def transform_constants():
    """
    Provides transform module constants for testing.

    Returns:
        dict: Dictionary with transform constants
    """
    from src.transform import (
        ALLOWED_CATEGORIES,
        ALLOWED_PAYMENT_METHODS,
        MIN_VALID_DATE,
        MIN_AMOUNT,
        MAX_AMOUNT
    )

    return {
        "allowed_categories": ALLOWED_CATEGORIES,
        "allowed_payment_methods": ALLOWED_PAYMENT_METHODS,
        "min_valid_date": MIN_VALID_DATE,
        "min_amount": MIN_AMOUNT,
        "max_amount": MAX_AMOUNT
    }


@pytest.fixture
def validated_transform_data():
    """
    Provides validated transaction data with datetime conversion for dimension testing.

    Returns:
        pd.DataFrame: Transaction data with datetime dates, ready for dimension creation
    """
    return pd.DataFrame({
        "transaction_id": ["TXN001", "TXN002", "TXN003", "TXN004"],
        "date": pd.to_datetime(["2023-06-15", "2023-06-16", "2023-06-17", "2023-06-18"]),
        "category": ["Groceries", "Dining", "Transportation", "Entertainment"],
        "amount": [50.00, 35.50, 15.75, 100.00],
        "merchant": ["Whole Foods", "Starbucks", "Uber", "Netflix"],
        "payment_method": ["Credit Card", "Debit Card", "Digital Wallet", "Credit Card"],
        "user_id": [1, 2, 1, 3]
    })

"""
Shared pytest fixtures and configuration for ETL pipeline tests.

This module provides reusable fixtures for testing the extract module,
including test data, file creation, and configuration.
"""

import pytest
import pandas as pd
import logging

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

"""
Tests for the extract module.

This test suite validates the extraction module's functionality including:
- Successful extraction of valid CSV files
- Error handling for missing files
- Error handling for malformed CSV files
- Validation of required columns
- Logging functionality
"""

import pandas as pd
from pathlib import Path
import tempfile
import os

from src.extract import extract_transactions, validate_csv_structure, get_file_info
from src.config import REQUIRED_CSV_COLUMNS, TRANSACTIONS_CSV


def test_get_file_info_existing_file():
    """Test getting info for an existing file."""
    print("  - Testing get_file_info with existing file...")
    info = get_file_info(str(TRANSACTIONS_CSV))

    assert info["exists"] is True, "File should exist"
    assert info["file_size"] > 0, "File size should be greater than 0"
    assert "MB" in info["file_size_mb"], "File size should contain MB"
    assert info["modified_time"] is not None, "Modified time should not be None"
    print("    ✓ Passed")


def test_get_file_info_nonexistent_file():
    """Test getting info for a nonexistent file."""
    print("  - Testing get_file_info with nonexistent file...")
    info = get_file_info("nonexistent_file.csv")

    assert info["exists"] is False, "File should not exist"
    assert info["file_size"] == 0, "File size should be 0"
    assert info["file_size_mb"] == "0.00 MB", "File size should be 0.00 MB"
    assert info["modified_time"] is None, "Modified time should be None"
    print("    ✓ Passed")


def test_validate_csv_structure_valid():
    """Test validation of a valid DataFrame."""
    print("  - Testing validate_csv_structure with valid DataFrame...")
    df = pd.DataFrame({
        "transaction_id": ["1", "2"],
        "date": ["2023-01-01", "2023-01-02"],
        "category": ["Food", "Transport"],
        "amount": [10.0, 20.0],
        "merchant": ["Store A", "Store B"],
        "payment_method": ["Credit", "Debit"],
        "user_id": [1, 2]
    })

    is_valid, error_msg = validate_csv_structure(df, REQUIRED_CSV_COLUMNS)

    assert is_valid is True, f"DataFrame should be valid but got error: {error_msg}"
    assert error_msg == "", "Error message should be empty"
    print("    ✓ Passed")


def test_validate_csv_structure_empty():
    """Test validation of an empty DataFrame."""
    print("  - Testing validate_csv_structure with empty DataFrame...")
    df = pd.DataFrame()

    is_valid, error_msg = validate_csv_structure(df, REQUIRED_CSV_COLUMNS)

    assert is_valid is False, "Empty DataFrame should not be valid"
    assert "empty" in error_msg.lower(), f"Error message should mention 'empty' but got: {error_msg}"
    print("    ✓ Passed")


def test_validate_csv_structure_missing_columns():
    """Test validation when required columns are missing."""
    print("  - Testing validate_csv_structure with missing columns...")
    df = pd.DataFrame({
        "transaction_id": ["1", "2"],
        "amount": [10.0, 20.0]
    })

    is_valid, error_msg = validate_csv_structure(df, REQUIRED_CSV_COLUMNS)

    assert is_valid is False, "DataFrame with missing columns should not be valid"
    assert "Missing required columns" in error_msg, f"Error should mention missing columns but got: {error_msg}"
    print("    ✓ Passed")


def test_extract_transactions_success():
    """Test successful extraction of transactions from valid CSV."""
    print("  - Testing extract_transactions with valid CSV...")
    df = extract_transactions(str(TRANSACTIONS_CSV))

    # Verify DataFrame is not empty
    assert len(df) > 0, "DataFrame should not be empty"

    # Verify all required columns are present
    for col in REQUIRED_CSV_COLUMNS:
        assert col in df.columns, f"Required column '{col}' is missing"

    # Verify data types are reasonable
    assert df["amount"].dtype in [float, int], "Amount should be numeric"
    assert df["user_id"].dtype in [int, float], "User ID should be numeric"
    print("    ✓ Passed")


def test_extract_transactions_file_not_found():
    """Test error handling when file doesn't exist."""
    print("  - Testing extract_transactions with nonexistent file...")
    try:
        extract_transactions("nonexistent_file.csv")
        assert False, "Should have raised FileNotFoundError"
    except FileNotFoundError:
        print("    ✓ Passed - FileNotFoundError raised as expected")


def test_extract_transactions_empty_csv():
    """Test error handling for empty CSV file."""
    print("  - Testing extract_transactions with empty CSV...")
    # Create a temporary empty CSV file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        temp_file = f.name

    try:
        extract_transactions(temp_file)
        assert False, "Should have raised EmptyDataError"
    except pd.errors.EmptyDataError:
        print("    ✓ Passed - EmptyDataError raised as expected")
    finally:
        os.unlink(temp_file)


def test_extract_transactions_missing_columns():
    """Test error handling when CSV is missing required columns."""
    print("  - Testing extract_transactions with missing required columns...")
    # Create a temporary CSV with missing columns
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("transaction_id,amount\n")
        f.write("1,100.0\n")
        f.write("2,200.0\n")
        temp_file = f.name

    try:
        extract_transactions(temp_file)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Missing required columns" in str(e), f"Expected 'Missing required columns' error but got: {e}"
        print("    ✓ Passed - ValueError raised as expected")
    finally:
        os.unlink(temp_file)


if __name__ == "__main__":
    """
    Run tests directly without pytest.
    """
    print("\n" + "=" * 80)
    print("Running Extract Module Tests")
    print("=" * 80 + "\n")

    # Test get_file_info
    print("1. Testing get_file_info function:")
    test_get_file_info_existing_file()
    test_get_file_info_nonexistent_file()
    print()

    # Test validate_csv_structure
    print("2. Testing validate_csv_structure function:")
    test_validate_csv_structure_valid()
    test_validate_csv_structure_empty()
    test_validate_csv_structure_missing_columns()
    print()

    # Test extract_transactions
    print("3. Testing extract_transactions function:")
    test_extract_transactions_success()
    test_extract_transactions_file_not_found()
    test_extract_transactions_empty_csv()
    test_extract_transactions_missing_columns()
    print()

    print("=" * 80)
    print("All tests passed!")
    print("=" * 80 + "\n")

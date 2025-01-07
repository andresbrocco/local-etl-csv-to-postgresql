"""
Tests for the extract module.

This test suite validates the extraction module's functionality including:
- File information retrieval
- CSV structure validation
- Successful extraction of valid CSV files
- Error handling for missing files and malformed CSV files
- Data type validation
"""

import pytest
import pandas as pd
from pathlib import Path

from src.extract import extract_transactions, validate_csv_structure, get_file_info
from src.config import REQUIRED_CSV_COLUMNS, TRANSACTIONS_CSV


# ============================================================================
# Tests for get_file_info Function
# ============================================================================

class TestGetFileInfo:
    """Tests for the get_file_info function."""

    @pytest.mark.unit
    @pytest.mark.file_operations
    def test_existing_file(self, valid_csv_file):
        """Test getting info for an existing file."""
        info = get_file_info(valid_csv_file)

        assert info["exists"] is True
        assert info["file_size"] > 0
        assert "MB" in info["file_size_mb"]
        assert info["modified_time"] is not None

    @pytest.mark.unit
    @pytest.mark.file_operations
    @pytest.mark.parametrize("file_path", [
        "nonexistent_file.csv",
        "/fake/path/to/file.csv",
        "../relative/nonexistent.csv",
    ], ids=["simple_name", "absolute_path", "relative_path"])
    def test_nonexistent_files(self, file_path):
        """Test getting info for nonexistent files with various path formats."""
        info = get_file_info(file_path)

        assert info["exists"] is False
        assert info["file_size"] == 0
        assert info["file_size_mb"] == "0.00 MB"
        assert info["modified_time"] is None

    @pytest.mark.integration
    @pytest.mark.file_operations
    def test_real_transactions_file(self):
        """Test getting info for the actual transactions.csv file."""
        if not TRANSACTIONS_CSV.exists():
            pytest.skip("Real transaction data not available")

        info = get_file_info(str(TRANSACTIONS_CSV))

        assert info["exists"] is True
        assert info["file_size"] > 0
        assert info["modified_time"] is not None


# ============================================================================
# Tests for validate_csv_structure Function
# ============================================================================

class TestValidateCsvStructure:
    """Tests for the validate_csv_structure function."""

    @pytest.mark.unit
    @pytest.mark.validation
    def test_valid_structure(self, valid_transaction_data, required_columns):
        """Test validation of a valid DataFrame."""
        is_valid, error_msg = validate_csv_structure(valid_transaction_data, required_columns)

        assert is_valid is True
        assert error_msg == ""

    @pytest.mark.unit
    @pytest.mark.validation
    @pytest.mark.parametrize("df_fixture,expected_error_substring", [
        ("empty_dataframe", "empty"),
        ("incomplete_dataframe", "Missing required columns"),
    ], ids=["empty_dataframe", "missing_columns"])
    def test_invalid_structures(self, df_fixture, expected_error_substring, required_columns, request):
        """Test validation of invalid DataFrame structures."""
        df = request.getfixturevalue(df_fixture)
        is_valid, error_msg = validate_csv_structure(df, required_columns)

        assert is_valid is False
        assert expected_error_substring in error_msg

    @pytest.mark.unit
    @pytest.mark.validation
    def test_dataframe_with_extra_columns(self, valid_transaction_data, required_columns):
        """Test that extra columns don't cause validation to fail."""
        df_with_extra = valid_transaction_data.copy()
        df_with_extra["extra_column"] = ["A", "B", "C"]

        is_valid, error_msg = validate_csv_structure(df_with_extra, required_columns)

        assert is_valid is True
        assert error_msg == ""


# ============================================================================
# Tests for extract_transactions Function
# ============================================================================

class TestExtractTransactions:
    """Tests for the extract_transactions function."""

    @pytest.mark.integration
    @pytest.mark.file_operations
    def test_successful_extraction(self, valid_csv_file, required_columns):
        """Test successful extraction of transactions from valid CSV."""
        df = extract_transactions(valid_csv_file)

        # Verify DataFrame is not empty
        assert len(df) > 0

        # Verify all required columns are present
        for col in required_columns:
            assert col in df.columns, f"Required column '{col}' is missing"

        # Verify data types are reasonable
        assert df["amount"].dtype in [float, int]
        assert df["user_id"].dtype in [int, float, object]

    @pytest.mark.integration
    @pytest.mark.file_operations
    def test_extraction_with_real_data(self, required_columns):
        """Test extraction with the actual transactions.csv file."""
        if not TRANSACTIONS_CSV.exists():
            pytest.skip("Real transaction data not available")

        df = extract_transactions(str(TRANSACTIONS_CSV))

        assert len(df) > 0
        for col in required_columns:
            assert col in df.columns

    @pytest.mark.unit
    @pytest.mark.error_handling
    @pytest.mark.parametrize("csv_fixture,expected_exception,error_message_contains", [
        ("nonexistent_file_path", FileNotFoundError, "File not found"),
        ("empty_csv_file", pd.errors.EmptyDataError, ""),
        ("incomplete_csv_file", ValueError, "Missing required columns"),
    ], ids=["file_not_found", "empty_csv", "missing_columns"])
    def test_error_handling(self, csv_fixture, expected_exception, error_message_contains, request):
        """Test error handling for various invalid input scenarios."""
        file_path = request.getfixturevalue(csv_fixture)

        with pytest.raises(expected_exception) as exc_info:
            extract_transactions(file_path)

        if error_message_contains:
            assert error_message_contains in str(exc_info.value)

    @pytest.mark.integration
    @pytest.mark.file_operations
    def test_data_integrity(self, tmp_path, valid_transaction_data):
        """Test that extracted data matches the source CSV exactly."""
        # Create CSV file
        csv_file = tmp_path / "test_transactions.csv"
        valid_transaction_data.to_csv(csv_file, index=False)

        # Extract and compare
        df_extracted = extract_transactions(str(csv_file))

        # Compare shape
        assert df_extracted.shape == valid_transaction_data.shape

        # Compare column names
        assert list(df_extracted.columns) == list(valid_transaction_data.columns)

        # Compare values (convert to strings for comparison to avoid type issues)
        for col in valid_transaction_data.columns:
            assert list(df_extracted[col].astype(str)) == list(valid_transaction_data[col].astype(str))

    @pytest.mark.integration
    @pytest.mark.file_operations
    def test_large_file_handling(self, tmp_path):
        """Test extraction of a larger CSV file."""
        # Create a larger dataset
        large_data = pd.DataFrame({
            "transaction_id": [f"TXN{i:06d}" for i in range(1000)],
            "date": ["2023-01-01"] * 1000,
            "category": ["Food"] * 1000,
            "amount": [10.0 + i * 0.1 for i in range(1000)],
            "merchant": ["Store A"] * 1000,
            "payment_method": ["Credit Card"] * 1000,
            "user_id": [i % 100 for i in range(1000)]
        })

        csv_file = tmp_path / "large_transactions.csv"
        large_data.to_csv(csv_file, index=False)

        df_extracted = extract_transactions(str(csv_file))

        assert len(df_extracted) == 1000
        assert df_extracted["transaction_id"].nunique() == 1000

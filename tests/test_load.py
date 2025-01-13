"""
Tests for the load module of the ETL pipeline.

This module tests database connection management, dimension loading, key mapping,
fact enrichment, fact loading, and end-to-end integration.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
import psycopg2

from src.load import (
    get_db_connection,
    database_connection,
    load_dimension,
    load_dim_date,
    get_dimension_key_mapping,
    get_all_dimension_mappings,
    enrich_fact_with_keys,
    check_existing_transactions,
    load_fact_table,
    load_data_warehouse,
    DatabaseConnectionError,
    DimensionLoadError,
    FactLoadError
)


# ============================================================================
# Database Connection Tests (3 tests)
# ============================================================================

class TestGetDbConnection:
    """Tests for database connection creation."""

    @pytest.mark.unit
    def test_get_db_connection_success(self, mock_db_connection):
        """Test successful database connection."""
        with patch('src.load.psycopg2.connect', return_value=mock_db_connection):
            conn = get_db_connection()
            assert conn is not None
            assert conn == mock_db_connection

    @pytest.mark.unit
    @pytest.mark.error_handling
    def test_get_db_connection_failure(self):
        """Test DatabaseConnectionError raised on connection failure."""
        with patch('src.load.psycopg2.connect', side_effect=psycopg2.OperationalError("Connection failed")):
            with pytest.raises(DatabaseConnectionError) as exc_info:
                get_db_connection()
            assert "Failed to connect" in str(exc_info.value)


class TestDatabaseConnectionContextManager:
    """Tests for database connection context manager."""

    @pytest.mark.unit
    @pytest.mark.error_handling
    def test_database_context_manager_rollback_on_error(self, mock_db_connection):
        """Test connection auto-rollback and cleanup on error."""
        with patch('src.load.get_db_connection', return_value=mock_db_connection):
            try:
                with database_connection() as conn:
                    assert conn == mock_db_connection
                    raise ValueError("Simulated error")
            except ValueError:
                pass

            # Verify rollback was called
            mock_db_connection.rollback.assert_called_once()
            # Verify connection was closed
            mock_db_connection.close.assert_called_once()


# ============================================================================
# Dimension Loading Tests (4 tests)
# ============================================================================

class TestLoadDimension:
    """Tests for generic dimension loading."""

    @pytest.mark.unit
    def test_load_dimension_new_records(self, mock_db_connection, mock_cursor, dimension_dataframes):
        """Test loading new dimension records successfully."""
        # Setup: cursor returns count before (0) and after (3) insertion
        mock_cursor.fetchone.side_effect = [(0,), (3,)]

        with patch('src.load.execute_batch'):
            count = load_dimension(
                mock_db_connection,
                dimension_dataframes["category"],
                "dim_category",
                "category_name"
            )

        assert count == 3
        assert mock_cursor.execute.called

    @pytest.mark.unit
    def test_load_dimension_skip_existing(self, mock_db_connection, mock_cursor, dimension_dataframes):
        """Test ON CONFLICT behavior skips existing records."""
        # Setup: cursor returns same count before and after (no new records)
        mock_cursor.fetchone.side_effect = [(3,), (3,)]

        with patch('src.load.execute_batch'):
            count = load_dimension(
                mock_db_connection,
                dimension_dataframes["category"],
                "dim_category",
                "category_name"
            )

        assert count == 0

    @pytest.mark.unit
    def test_load_dimension_empty_dataframe(self, mock_db_connection, empty_dataframe):
        """Test loading empty DataFrame returns 0."""
        count = load_dimension(
            mock_db_connection,
            empty_dataframe,
            "dim_category",
            "category_name"
        )

        assert count == 0


class TestLoadDimDate:
    """Tests for date dimension loading."""

    @pytest.mark.unit
    def test_load_dim_date_with_all_attributes(self, mock_db_connection, mock_cursor, dimension_dataframes):
        """Test loading date dimension with all 11 attributes."""
        # Setup: cursor returns count before (0) and after (3) insertion
        mock_cursor.fetchone.side_effect = [(0,), (3,)]

        with patch('src.load.execute_batch'):
            count = load_dim_date(
                mock_db_connection,
                dimension_dataframes["date"]
            )

        assert count == 3
        assert mock_cursor.execute.called


# ============================================================================
# Dimension Key Mapping Tests (2 tests)
# ============================================================================

class TestGetDimensionKeyMapping:
    """Tests for dimension key mapping retrieval."""

    @pytest.mark.unit
    def test_get_dimension_key_mapping_success(self, mock_db_connection, mock_cursor):
        """Test retrieving dimension key mapping successfully."""
        # Setup: cursor returns mapping data
        mock_cursor.fetchall.return_value = [
            ("Groceries", 1),
            ("Dining", 2),
            ("Transportation", 3)
        ]

        mapping = get_dimension_key_mapping(
            mock_db_connection,
            "dim_category",
            "category_name",
            "category_key"
        )

        assert mapping == {"Groceries": 1, "Dining": 2, "Transportation": 3}
        assert mock_cursor.execute.called


class TestGetAllDimensionMappings:
    """Tests for retrieving all dimension mappings."""

    @pytest.mark.unit
    def test_get_all_dimension_mappings(self, mock_db_connection):
        """Test retrieving all 5 dimension mappings."""
        with patch('src.load.get_dimension_key_mapping') as mock_get_mapping:
            # Setup: return different mappings for each dimension
            mock_get_mapping.side_effect = [
                {"Groceries": 1},  # category
                {"Whole Foods": 1},  # merchant
                {"Credit Card": 1},  # payment_method
                {1: 1},  # user
                {20230615: 20230615}  # date
            ]

            mappings = get_all_dimension_mappings(mock_db_connection)

            assert "category" in mappings
            assert "merchant" in mappings
            assert "payment_method" in mappings
            assert "user" in mappings
            assert "date" in mappings
            assert len(mappings) == 5


# ============================================================================
# Fact Enrichment Tests (5 tests)
# ============================================================================

class TestEnrichFactWithKeys:
    """Tests for fact data enrichment with surrogate keys."""

    @pytest.mark.unit
    def test_enrich_fact_with_keys_success(self, dimension_mappings):
        """Test successful fact enrichment with all valid keys."""
        fact_df = pd.DataFrame({
            "transaction_id": ["TXN001", "TXN002"],
            "date_key": [20230615, 20230616],
            "category": ["Groceries", "Dining"],
            "merchant": ["Whole Foods", "Starbucks"],
            "payment_method": ["Credit Card", "Debit Card"],
            "user_id": [1, 2],
            "amount": [50.00, 35.50]
        })

        enriched = enrich_fact_with_keys(fact_df, dimension_mappings)

        # Verify surrogate keys added
        assert "category_key" in enriched.columns
        assert "merchant_key" in enriched.columns
        assert "payment_method_key" in enriched.columns
        assert "user_key" in enriched.columns

        # Verify correct values
        assert enriched["category_key"].tolist() == [1, 2]
        assert enriched["merchant_key"].tolist() == [1, 2]

    @pytest.mark.unit
    @pytest.mark.error_handling
    @pytest.mark.validation
    def test_enrich_fact_missing_category_key(self, dimension_mappings):
        """Test FactLoadError raised when category key missing."""
        fact_df = pd.DataFrame({
            "transaction_id": ["TXN001"],
            "date_key": [20230615],
            "category": ["InvalidCategory"],  # Not in mappings
            "merchant": ["Whole Foods"],
            "payment_method": ["Credit Card"],
            "user_id": [1],
            "amount": [50.00]
        })

        with pytest.raises(FactLoadError) as exc_info:
            enrich_fact_with_keys(fact_df, dimension_mappings)
        assert "category" in str(exc_info.value).lower()

    @pytest.mark.unit
    @pytest.mark.error_handling
    @pytest.mark.validation
    def test_enrich_fact_missing_merchant_key(self, dimension_mappings):
        """Test FactLoadError raised when merchant key missing."""
        fact_df = pd.DataFrame({
            "transaction_id": ["TXN001"],
            "date_key": [20230615],
            "category": ["Groceries"],
            "merchant": ["InvalidMerchant"],  # Not in mappings
            "payment_method": ["Credit Card"],
            "user_id": [1],
            "amount": [50.00]
        })

        with pytest.raises(FactLoadError) as exc_info:
            enrich_fact_with_keys(fact_df, dimension_mappings)
        assert "merchant" in str(exc_info.value).lower()

    @pytest.mark.unit
    @pytest.mark.error_handling
    @pytest.mark.validation
    def test_enrich_fact_missing_user_key(self, dimension_mappings):
        """Test FactLoadError raised when user key missing."""
        fact_df = pd.DataFrame({
            "transaction_id": ["TXN001"],
            "date_key": [20230615],
            "category": ["Groceries"],
            "merchant": ["Whole Foods"],
            "payment_method": ["Credit Card"],
            "user_id": [999],  # Not in mappings
            "amount": [50.00]
        })

        with pytest.raises(FactLoadError) as exc_info:
            enrich_fact_with_keys(fact_df, dimension_mappings)
        assert "user" in str(exc_info.value).lower()

    @pytest.mark.unit
    @pytest.mark.error_handling
    @pytest.mark.validation
    def test_enrich_fact_missing_date_key(self, dimension_mappings):
        """Test FactLoadError raised when date key missing."""
        fact_df = pd.DataFrame({
            "transaction_id": ["TXN001"],
            "date_key": [99999999],  # Not in mappings
            "category": ["Groceries"],
            "merchant": ["Whole Foods"],
            "payment_method": ["Credit Card"],
            "user_id": [1],
            "amount": [50.00]
        })

        with pytest.raises(FactLoadError) as exc_info:
            enrich_fact_with_keys(fact_df, dimension_mappings)
        assert "date" in str(exc_info.value).lower()


# ============================================================================
# Fact Loading Tests (3 tests)
# ============================================================================

class TestCheckExistingTransactions:
    """Tests for checking existing transactions."""

    @pytest.mark.unit
    def test_check_existing_transactions(self, mock_db_connection, mock_cursor):
        """Test returns set of existing transaction IDs."""
        # Setup: cursor returns existing transaction IDs
        mock_cursor.fetchall.return_value = [
            ("TXN001",),
            ("TXN002",)
        ]

        existing = check_existing_transactions(
            mock_db_connection,
            ["TXN001", "TXN002", "TXN003"]
        )

        assert existing == {"TXN001", "TXN002"}
        assert "TXN003" not in existing


class TestLoadFactTable:
    """Tests for fact table loading."""

    @pytest.mark.unit
    def test_load_fact_table_all_new(self, mock_db_connection, mock_cursor, enriched_fact_data):
        """Test loading all new transactions."""
        # Setup: no existing transactions, cursor returns count before (0) and after (3)
        mock_cursor.fetchone.side_effect = [(0,), (3,)]

        with patch('src.load.check_existing_transactions', return_value=set()):
            with patch('src.load.execute_batch'):
                inserted, skipped = load_fact_table(
                    mock_db_connection,
                    enriched_fact_data
                )

        assert inserted == 3
        assert skipped == 0

    @pytest.mark.unit
    def test_load_fact_table_skip_existing(self, mock_db_connection, mock_cursor, enriched_fact_data, existing_transaction_ids):
        """Test incremental load skips existing transactions."""
        # Setup: TXN001 and TXN002 already exist, only 1 new record inserted
        mock_cursor.fetchone.side_effect = [(0,), (1,)]

        with patch('src.load.check_existing_transactions', return_value=existing_transaction_ids):
            with patch('src.load.execute_batch'):
                inserted, skipped = load_fact_table(
                    mock_db_connection,
                    enriched_fact_data
                )

        assert inserted == 1
        assert skipped == 2


# ============================================================================
# End-to-End Integration Tests (3 tests)
# ============================================================================

class TestLoadDataWarehouse:
    """Tests for end-to-end data warehouse loading."""

    @pytest.mark.integration
    def test_load_data_warehouse_success(self, mock_db_connection, mock_cursor, dimension_dataframes):
        """Test complete successful load of all dimensions and fact table."""
        transformed_data = {
            "dim_category": dimension_dataframes["category"],
            "dim_merchant": dimension_dataframes["merchant"],
            "dim_payment_method": dimension_dataframes["payment_method"],
            "dim_user": dimension_dataframes["user"],
            "dim_date": dimension_dataframes["date"],
            "fact_data": pd.DataFrame({
                "transaction_id": ["TXN001"],
                "date_key": [20230615],
                "category": ["Groceries"],
                "merchant": ["Whole Foods"],
                "payment_method": ["Credit Card"],
                "user_id": [1],
                "amount": [50.00]
            })
        }

        with patch('src.load.get_db_connection', return_value=mock_db_connection):
            with patch('src.load.load_dimension', return_value=3):
                with patch('src.load.load_dim_date', return_value=3):
                    with patch('src.load.get_all_dimension_mappings', return_value={
                        "category": {"Groceries": 1},
                        "merchant": {"Whole Foods": 1},
                        "payment_method": {"Credit Card": 1},
                        "user": {1: 1},
                        "date": {20230615: 20230615}
                    }):
                        with patch('src.load.load_fact_table', return_value=(1, 0)):
                            stats = load_data_warehouse(transformed_data)

        assert "dimensions_inserted" in stats
        assert "facts_inserted" in stats
        assert "facts_skipped" in stats
        assert stats["facts_inserted"] == 1
        mock_db_connection.commit.assert_called_once()

    @pytest.mark.integration
    def test_load_data_warehouse_incremental(self, mock_db_connection, dimension_dataframes):
        """Test incremental loading - run twice, second run skips duplicates."""
        transformed_data = {
            "dim_category": dimension_dataframes["category"],
            "dim_merchant": dimension_dataframes["merchant"],
            "dim_payment_method": dimension_dataframes["payment_method"],
            "dim_user": dimension_dataframes["user"],
            "dim_date": dimension_dataframes["date"],
            "fact_data": pd.DataFrame({
                "transaction_id": ["TXN001"],
                "date_key": [20230615],
                "category": ["Groceries"],
                "merchant": ["Whole Foods"],
                "payment_method": ["Credit Card"],
                "user_id": [1],
                "amount": [50.00]
            })
        }

        with patch('src.load.get_db_connection', return_value=mock_db_connection):
            # load_dimension is called 4 times per run (category, merchant, payment_method, user)
            # First run: 3 new records each, Second run: 0 new records each
            with patch('src.load.load_dimension', side_effect=[3, 3, 3, 3, 0, 0, 0, 0]):
                with patch('src.load.load_dim_date', side_effect=[3, 0]):
                    with patch('src.load.get_all_dimension_mappings', return_value={
                        "category": {"Groceries": 1},
                        "merchant": {"Whole Foods": 1},
                        "payment_method": {"Credit Card": 1},
                        "user": {1: 1},
                        "date": {20230615: 20230615}
                    }):
                        # First load: 1 inserted, 0 skipped
                        with patch('src.load.load_fact_table', return_value=(1, 0)):
                            stats1 = load_data_warehouse(transformed_data)

                        # Second load: 0 inserted, 1 skipped (duplicate)
                        with patch('src.load.load_fact_table', return_value=(0, 1)):
                            stats2 = load_data_warehouse(transformed_data)

        # First load should insert 1
        assert stats1["facts_inserted"] == 1
        assert stats1["facts_skipped"] == 0

        # Second load should skip 1
        assert stats2["facts_inserted"] == 0
        assert stats2["facts_skipped"] == 1

    @pytest.mark.integration
    @pytest.mark.error_handling
    def test_load_data_warehouse_rollback_on_error(self, mock_db_connection, dimension_dataframes):
        """Test transaction atomicity - rollback on error."""
        transformed_data = {
            "dim_category": dimension_dataframes["category"],
            "dim_merchant": dimension_dataframes["merchant"],
            "dim_payment_method": dimension_dataframes["payment_method"],
            "dim_user": dimension_dataframes["user"],
            "dim_date": dimension_dataframes["date"],
            "fact_data": pd.DataFrame({
                "transaction_id": ["TXN001"],
                "date_key": [20230615],
                "category": ["Groceries"],
                "merchant": ["Whole Foods"],
                "payment_method": ["Credit Card"],
                "user_id": [1],
                "amount": [50.00]
            })
        }

        with patch('src.load.get_db_connection', return_value=mock_db_connection):
            with patch('src.load.load_dimension', return_value=3):
                with patch('src.load.load_dim_date', return_value=3):
                    with patch('src.load.get_all_dimension_mappings', side_effect=DimensionLoadError("Mapping failed")):
                        with pytest.raises(DimensionLoadError):
                            load_data_warehouse(transformed_data)

        # Verify rollback was called
        mock_db_connection.rollback.assert_called()
        # Verify connection was closed
        mock_db_connection.close.assert_called()

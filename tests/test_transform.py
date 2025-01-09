"""
Tests for the transform module.

This test suite validates the transformation module's functionality including:
- Data standardization (category, merchant, payment_method)
- Data cleaning (duplicates, whitespace, standardization)
- Data validation (amounts, dates, categories, payment methods, user_ids)
- Date dimension derivation
- Dimension creation
- Full transformation pipeline
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from src.transform import (
    standardize_category,
    standardize_merchant,
    standardize_payment_method,
    clean_transaction_data,
    validate_transaction_data,
    derive_date_attributes,
    create_dimension_data,
    log_transformation_summary,
    transform_transactions,
    ALLOWED_CATEGORIES,
    ALLOWED_PAYMENT_METHODS,
    MIN_VALID_DATE,
    MAX_VALID_DATE,
    MIN_AMOUNT,
    MAX_AMOUNT
)


# ============================================================================
# Tests for Standardization Functions
# ============================================================================

class TestStandardizationFunctions:
    """Tests for text standardization helper functions."""

    @pytest.mark.unit
    @pytest.mark.validation
    @pytest.mark.parametrize("input_value,expected_output", [
        ("groceries", "Groceries"),
        ("GROCERIES", "Groceries"),
        ("GrOcErIeS", "Groceries"),
        ("  groceries  ", "Groceries"),
        ("dining", "Dining"),
        ("already standardized", "Already Standardized"),
    ], ids=["lowercase", "uppercase", "mixed_case", "with_whitespace", "simple", "multi_word"])
    def test_standardize_category(self, input_value, expected_output):
        """Test category standardization with various inputs."""
        result = standardize_category(input_value)
        assert result == expected_output

    @pytest.mark.unit
    @pytest.mark.validation
    def test_standardize_category_null(self):
        """Test that null values are preserved."""
        result = standardize_category(np.nan)
        assert pd.isna(result)

    @pytest.mark.unit
    @pytest.mark.validation
    @pytest.mark.parametrize("input_value,expected_output", [
        ("walmart", "Walmart"),
        ("WALMART SUPERCENTER", "Walmart Supercenter"),
        ("  walmart   supercenter  ", "Walmart Supercenter"),
        ("target  store", "Target Store"),
        ("whole   foods   market", "Whole Foods Market"),
    ], ids=["single_word", "two_words_caps", "extra_spaces", "double_space", "multiple_spaces"])
    def test_standardize_merchant(self, input_value, expected_output):
        """Test merchant standardization with various inputs."""
        result = standardize_merchant(input_value)
        assert result == expected_output

    @pytest.mark.unit
    @pytest.mark.validation
    def test_standardize_merchant_null(self):
        """Test that null values are preserved in merchant standardization."""
        result = standardize_merchant(np.nan)
        assert pd.isna(result)

    @pytest.mark.unit
    @pytest.mark.validation
    @pytest.mark.parametrize("input_value,expected_output", [
        ("credit card", "Credit Card"),
        ("DEBIT CARD", "Debit Card"),
        ("  cash  ", "Cash"),
        ("digital wallet", "Digital Wallet"),
    ], ids=["lowercase", "uppercase", "with_whitespace", "multi_word"])
    def test_standardize_payment_method(self, input_value, expected_output):
        """Test payment method standardization."""
        result = standardize_payment_method(input_value)
        assert result == expected_output

    @pytest.mark.unit
    @pytest.mark.validation
    def test_standardize_payment_method_null(self):
        """Test that null values are preserved in payment method standardization."""
        result = standardize_payment_method(np.nan)
        assert pd.isna(result)


# ============================================================================
# Tests for clean_transaction_data Function
# ============================================================================

class TestCleanTransactionData:
    """Tests for the clean_transaction_data function."""

    @pytest.mark.unit
    @pytest.mark.validation
    def test_removes_duplicates(self, dirty_transform_data):
        """Test that duplicate transaction_ids are removed."""
        df_clean = clean_transaction_data(dirty_transform_data)

        # Should remove 1 duplicate (TXN001 appears twice)
        assert len(df_clean) == 3
        assert df_clean['transaction_id'].nunique() == 3

    @pytest.mark.unit
    @pytest.mark.validation
    def test_keeps_first_occurrence(self, dirty_transform_data):
        """Test that first occurrence is kept when removing duplicates."""
        df_clean = clean_transaction_data(dirty_transform_data)

        # First TXN001 has category "  groceries  "
        txn001 = df_clean[df_clean['transaction_id'] == 'TXN001']
        assert len(txn001) == 1

    @pytest.mark.unit
    @pytest.mark.validation
    def test_standardizes_category(self, dirty_transform_data):
        """Test that categories are standardized to title case."""
        df_clean = clean_transaction_data(dirty_transform_data)

        # All categories should be title case
        for category in df_clean['category']:
            assert category == category.strip().title()

    @pytest.mark.unit
    @pytest.mark.validation
    def test_standardizes_merchant(self, dirty_transform_data):
        """Test that merchants are standardized."""
        df_clean = clean_transaction_data(dirty_transform_data)

        # Check specific merchant standardization
        assert "Whole Foods" in df_clean['merchant'].values
        assert "Starbucks" in df_clean['merchant'].values

        # No extra spaces
        for merchant in df_clean['merchant']:
            assert '  ' not in merchant

    @pytest.mark.unit
    @pytest.mark.validation
    def test_standardizes_payment_method(self, dirty_transform_data):
        """Test that payment methods are standardized."""
        df_clean = clean_transaction_data(dirty_transform_data)

        # All payment methods should be title case
        for payment in df_clean['payment_method']:
            assert payment == payment.strip().title()

    @pytest.mark.unit
    @pytest.mark.validation
    def test_trims_whitespace(self, dirty_transform_data):
        """Test that whitespace is trimmed from all string columns."""
        df_clean = clean_transaction_data(dirty_transform_data)

        # No leading/trailing whitespace
        for col in df_clean.select_dtypes(include=['object']).columns:
            for value in df_clean[col]:
                if isinstance(value, str):
                    assert value == value.strip()

    @pytest.mark.unit
    @pytest.mark.validation
    def test_preserves_clean_data(self, clean_transform_data):
        """Test that already clean data is not corrupted."""
        df_clean = clean_transaction_data(clean_transform_data)

        # Should have same number of rows (no duplicates)
        assert len(df_clean) == len(clean_transform_data)

        # Should preserve data integrity
        assert list(df_clean.columns) == list(clean_transform_data.columns)

    @pytest.mark.unit
    @pytest.mark.validation
    def test_does_not_modify_original(self, dirty_transform_data):
        """Test that original DataFrame is not modified."""
        original_len = len(dirty_transform_data)
        df_clean = clean_transaction_data(dirty_transform_data)

        # Original should be unchanged
        assert len(dirty_transform_data) == original_len


# ============================================================================
# Tests for validate_transaction_data Function
# ============================================================================

class TestValidateTransactionData:
    """Tests for the validate_transaction_data function."""

    @pytest.mark.unit
    @pytest.mark.validation
    def test_all_valid_data(self, clean_transform_data):
        """Test validation of completely valid data."""
        valid_df, issues = validate_transaction_data(clean_transform_data)

        assert len(valid_df) == len(clean_transform_data)
        assert len(issues) == 0

    @pytest.mark.unit
    @pytest.mark.validation
    def test_filters_invalid_amounts(self):
        """Test that invalid amounts are filtered out."""
        df = pd.DataFrame({
            "transaction_id": ["TXN001", "TXN002", "TXN003", "TXN004"],
            "date": ["2023-06-15"] * 4,
            "category": ["Groceries"] * 4,
            "amount": [50.00, -10.00, 0.00, 15000.00],  # Negative, zero, too large
            "merchant": ["Store A"] * 4,
            "payment_method": ["Credit Card"] * 4,
            "user_id": [1, 2, 3, 4]
        })

        valid_df, issues = validate_transaction_data(df)

        # Only first transaction should be valid
        assert len(valid_df) == 1
        assert valid_df.iloc[0]['amount'] == 50.00

        # Should have issues reported
        assert len(issues) > 0

    @pytest.mark.unit
    @pytest.mark.validation
    def test_filters_future_dates(self):
        """Test that future dates are filtered out."""
        future_date = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")

        df = pd.DataFrame({
            "transaction_id": ["TXN001", "TXN002"],
            "date": ["2023-06-15", future_date],
            "category": ["Groceries", "Dining"],
            "amount": [50.00, 30.00],
            "merchant": ["Store A", "Store B"],
            "payment_method": ["Credit Card", "Cash"],
            "user_id": [1, 2]
        })

        valid_df, issues = validate_transaction_data(df)

        # Only first transaction should be valid
        assert len(valid_df) == 1
        assert any("future" in issue.lower() for issue in issues)

    @pytest.mark.unit
    @pytest.mark.validation
    def test_filters_old_dates(self):
        """Test that dates before MIN_VALID_DATE are filtered out."""
        old_date = "2019-01-01"  # Before 2020-01-01

        df = pd.DataFrame({
            "transaction_id": ["TXN001", "TXN002"],
            "date": ["2023-06-15", old_date],
            "category": ["Groceries", "Dining"],
            "amount": [50.00, 30.00],
            "merchant": ["Store A", "Store B"],
            "payment_method": ["Credit Card", "Cash"],
            "user_id": [1, 2]
        })

        valid_df, issues = validate_transaction_data(df)

        assert len(valid_df) == 1
        assert any("2020" in issue for issue in issues)

    @pytest.mark.unit
    @pytest.mark.validation
    def test_filters_invalid_categories(self):
        """Test that invalid categories are filtered out."""
        df = pd.DataFrame({
            "transaction_id": ["TXN001", "TXN002"],
            "date": ["2023-06-15", "2023-06-16"],
            "category": ["Groceries", "InvalidCategory"],
            "amount": [50.00, 30.00],
            "merchant": ["Store A", "Store B"],
            "payment_method": ["Credit Card", "Cash"],
            "user_id": [1, 2]
        })

        valid_df, issues = validate_transaction_data(df)

        assert len(valid_df) == 1
        assert valid_df.iloc[0]['category'] == "Groceries"
        assert any("invalid categories" in issue.lower() for issue in issues)

    @pytest.mark.unit
    @pytest.mark.validation
    def test_filters_invalid_payment_methods(self):
        """Test that invalid payment methods are filtered out."""
        df = pd.DataFrame({
            "transaction_id": ["TXN001", "TXN002"],
            "date": ["2023-06-15", "2023-06-16"],
            "category": ["Groceries", "Dining"],
            "amount": [50.00, 30.00],
            "merchant": ["Store A", "Store B"],
            "payment_method": ["Credit Card", "Bitcoin"],
            "user_id": [1, 2]
        })

        valid_df, issues = validate_transaction_data(df)

        assert len(valid_df) == 1
        assert any("invalid payment methods" in issue.lower() for issue in issues)

    @pytest.mark.unit
    @pytest.mark.validation
    def test_filters_invalid_user_ids(self):
        """Test that non-integer user_ids are filtered out."""
        df = pd.DataFrame({
            "transaction_id": ["TXN001", "TXN002", "TXN003"],
            "date": ["2023-06-15", "2023-06-16", "2023-06-17"],
            "category": ["Groceries", "Dining", "Shopping"],
            "amount": [50.00, 30.00, 40.00],
            "merchant": ["Store A", "Store B", "Store C"],
            "payment_method": ["Credit Card", "Cash", "Debit Card"],
            "user_id": [1, "invalid", 3]
        })

        valid_df, issues = validate_transaction_data(df)

        assert len(valid_df) == 2
        assert any("invalid user_id" in issue.lower() for issue in issues)

    @pytest.mark.unit
    @pytest.mark.validation
    def test_handles_null_values(self):
        """Test that null values in required fields are filtered out."""
        df = pd.DataFrame({
            "transaction_id": ["TXN001", "TXN002", "TXN003"],
            "date": ["2023-06-15", None, "2023-06-17"],
            "category": ["Groceries", "Dining", None],
            "amount": [50.00, 30.00, None],
            "merchant": ["Store A", "Store B", "Store C"],
            "payment_method": ["Credit Card", "Cash", "Debit Card"],
            "user_id": [1, 2, 3]
        })

        valid_df, issues = validate_transaction_data(df)

        # Only first transaction should be valid
        assert len(valid_df) == 1
        assert len(issues) > 0

    @pytest.mark.unit
    @pytest.mark.validation
    def test_rounds_amounts(self, clean_transform_data):
        """Test that amounts are rounded to 2 decimal places."""
        df = clean_transform_data.copy()
        df['amount'] = [10.999, 20.111, 30.555, 40.123]

        valid_df, issues = validate_transaction_data(df)

        # Check rounding
        assert valid_df.iloc[0]['amount'] == 11.00
        assert valid_df.iloc[1]['amount'] == 20.11
        assert valid_df.iloc[2]['amount'] == 30.56
        assert valid_df.iloc[3]['amount'] == 40.12

    @pytest.mark.unit
    @pytest.mark.validation
    def test_returns_tuple(self, clean_transform_data):
        """Test that function returns tuple of (DataFrame, list)."""
        result = validate_transaction_data(clean_transform_data)

        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], pd.DataFrame)
        assert isinstance(result[1], list)

    @pytest.mark.unit
    @pytest.mark.validation
    def test_comprehensive_validation(self, invalid_transform_data):
        """Test validation with multiple types of invalid data."""
        valid_df, issues = validate_transaction_data(invalid_transform_data)

        # Should filter out multiple invalid records
        assert len(valid_df) < len(invalid_transform_data)

        # Should have multiple issues
        assert len(issues) > 0


# ============================================================================
# Tests for derive_date_attributes Function
# ============================================================================

class TestDeriveDateAttributes:
    """Tests for the derive_date_attributes function."""

    @pytest.mark.unit
    @pytest.mark.validation
    def test_creates_date_key(self, sample_date_series):
        """Test that date_key is created in YYYYMMDD format."""
        date_dim = derive_date_attributes(sample_date_series)

        # Check date_key format
        assert 'date_key' in date_dim.columns
        assert date_dim.iloc[0]['date_key'] == 20230615  # June 15, 2023

    @pytest.mark.unit
    @pytest.mark.validation
    def test_extracts_date_components(self, sample_date_series):
        """Test that year, quarter, month, day are extracted."""
        date_dim = derive_date_attributes(sample_date_series)

        # Check all components exist
        assert 'year' in date_dim.columns
        assert 'quarter' in date_dim.columns
        assert 'month' in date_dim.columns
        assert 'day' in date_dim.columns

        # Check values for first date (2023-06-15)
        assert date_dim.iloc[0]['year'] == 2023
        assert date_dim.iloc[0]['quarter'] == 2  # Q2
        assert date_dim.iloc[0]['month'] == 6
        assert date_dim.iloc[0]['day'] == 15

    @pytest.mark.unit
    @pytest.mark.validation
    def test_includes_date_names(self, sample_date_series):
        """Test that month_name and day_name are included."""
        date_dim = derive_date_attributes(sample_date_series)

        assert 'month_name' in date_dim.columns
        assert 'day_name' in date_dim.columns

        # Check first date (2023-06-15 is a Thursday)
        assert date_dim.iloc[0]['month_name'] == 'June'
        assert date_dim.iloc[0]['day_name'] == 'Thursday'

    @pytest.mark.unit
    @pytest.mark.validation
    def test_weekend_detection(self, weekend_date_series):
        """Test that weekends are correctly identified."""
        date_dim = derive_date_attributes(weekend_date_series)

        # June 17, 2023 is Saturday (index 1)
        # June 18, 2023 is Sunday (index 2)
        assert date_dim.iloc[1]['is_weekend'] == True
        assert date_dim.iloc[2]['is_weekend'] == True

        # June 16, 2023 is Friday (index 0)
        # June 19, 2023 is Monday (index 3)
        assert date_dim.iloc[0]['is_weekend'] == False
        assert date_dim.iloc[3]['is_weekend'] == False

    @pytest.mark.unit
    @pytest.mark.validation
    def test_day_of_week(self, sample_date_series):
        """Test that day_of_week follows ISO format (1=Mon, 7=Sun)."""
        date_dim = derive_date_attributes(sample_date_series)

        # June 15, 2023 is Thursday (should be 4)
        assert date_dim.iloc[0]['day_of_week'] == 4

    @pytest.mark.unit
    @pytest.mark.validation
    def test_week_of_year(self, sample_date_series):
        """Test that week_of_year is calculated."""
        date_dim = derive_date_attributes(sample_date_series)

        assert 'week_of_year' in date_dim.columns
        # June 15, 2023 is in week 24
        assert date_dim.iloc[0]['week_of_year'] == 24

    @pytest.mark.unit
    @pytest.mark.validation
    def test_handles_duplicate_dates(self):
        """Test that duplicate dates are deduplicated."""
        dates = pd.Series([
            pd.Timestamp("2023-06-15"),
            pd.Timestamp("2023-06-15"),
            pd.Timestamp("2023-06-16")
        ])

        date_dim = derive_date_attributes(dates)

        # Should only have 2 unique dates
        assert len(date_dim) == 2

    @pytest.mark.unit
    @pytest.mark.validation
    def test_dates_are_sorted(self, sample_date_series):
        """Test that dates are sorted in ascending order."""
        date_dim = derive_date_attributes(sample_date_series)

        # Verify sorted
        dates = date_dim['date'].tolist()
        assert dates == sorted(dates)


# ============================================================================
# Tests for create_dimension_data Function
# ============================================================================

class TestCreateDimensionData:
    """Tests for the create_dimension_data function."""

    @pytest.mark.unit
    @pytest.mark.validation
    def test_returns_all_dimensions(self, validated_transform_data):
        """Test that all 5 dimensions are created."""
        dimensions = create_dimension_data(validated_transform_data)

        assert isinstance(dimensions, dict)
        assert 'dim_date' in dimensions
        assert 'dim_category' in dimensions
        assert 'dim_merchant' in dimensions
        assert 'dim_payment_method' in dimensions
        assert 'dim_user' in dimensions

    @pytest.mark.unit
    @pytest.mark.validation
    def test_dim_date_structure(self, validated_transform_data):
        """Test that dim_date has correct structure."""
        dimensions = create_dimension_data(validated_transform_data)
        dim_date = dimensions['dim_date']

        # Should have date dimension columns
        assert 'date_key' in dim_date.columns
        assert 'date' in dim_date.columns
        assert 'year' in dim_date.columns
        assert 'month' in dim_date.columns
        assert 'day' in dim_date.columns
        assert 'is_weekend' in dim_date.columns

    @pytest.mark.unit
    @pytest.mark.validation
    def test_dim_category_structure(self, validated_transform_data):
        """Test that dim_category has correct structure."""
        dimensions = create_dimension_data(validated_transform_data)
        dim_category = dimensions['dim_category']

        assert 'category_name' in dim_category.columns
        assert len(dim_category) == validated_transform_data['category'].nunique()

    @pytest.mark.unit
    @pytest.mark.validation
    def test_dim_merchant_structure(self, validated_transform_data):
        """Test that dim_merchant has correct structure."""
        dimensions = create_dimension_data(validated_transform_data)
        dim_merchant = dimensions['dim_merchant']

        assert 'merchant_name' in dim_merchant.columns
        assert len(dim_merchant) == validated_transform_data['merchant'].nunique()

    @pytest.mark.unit
    @pytest.mark.validation
    def test_dim_payment_method_structure(self, validated_transform_data):
        """Test that dim_payment_method has correct structure."""
        dimensions = create_dimension_data(validated_transform_data)
        dim_payment = dimensions['dim_payment_method']

        assert 'payment_method_name' in dim_payment.columns
        assert len(dim_payment) == validated_transform_data['payment_method'].nunique()

    @pytest.mark.unit
    @pytest.mark.validation
    def test_dim_user_structure(self, validated_transform_data):
        """Test that dim_user has correct structure."""
        dimensions = create_dimension_data(validated_transform_data)
        dim_user = dimensions['dim_user']

        assert 'user_id' in dim_user.columns
        assert len(dim_user) == validated_transform_data['user_id'].nunique()

    @pytest.mark.unit
    @pytest.mark.validation
    def test_dimensions_are_sorted(self, validated_transform_data):
        """Test that dimension values are sorted."""
        dimensions = create_dimension_data(validated_transform_data)

        # Check category is sorted
        categories = dimensions['dim_category']['category_name'].tolist()
        assert categories == sorted(categories)

        # Check merchants are sorted
        merchants = dimensions['dim_merchant']['merchant_name'].tolist()
        assert merchants == sorted(merchants)

        # Check payment methods are sorted
        payments = dimensions['dim_payment_method']['payment_method_name'].tolist()
        assert payments == sorted(payments)

        # Check users are sorted
        users = dimensions['dim_user']['user_id'].tolist()
        assert users == sorted(users)

    @pytest.mark.unit
    @pytest.mark.validation
    def test_dimensions_are_unique(self, validated_transform_data):
        """Test that all dimension values are unique."""
        dimensions = create_dimension_data(validated_transform_data)

        for dim_name, dim_df in dimensions.items():
            # Check no duplicates
            assert len(dim_df) == dim_df.drop_duplicates().shape[0]


# ============================================================================
# Tests for log_transformation_summary Function
# ============================================================================

class TestLogTransformationSummary:
    """Tests for the log_transformation_summary function."""

    @pytest.mark.unit
    def test_does_not_crash_with_valid_inputs(self):
        """Test that function completes without errors."""
        # Should not raise any exceptions
        log_transformation_summary(
            original_count=1000,
            final_count=950,
            duplicates_removed=30,
            invalid_removed=20,
            issues=["Issue 1", "Issue 2"]
        )

    @pytest.mark.unit
    def test_handles_zero_counts(self):
        """Test with zero counts."""
        log_transformation_summary(
            original_count=0,
            final_count=0,
            duplicates_removed=0,
            invalid_removed=0,
            issues=[]
        )

    @pytest.mark.unit
    def test_handles_empty_issues_list(self):
        """Test with no issues."""
        log_transformation_summary(
            original_count=1000,
            final_count=1000,
            duplicates_removed=0,
            invalid_removed=0,
            issues=[]
        )

    @pytest.mark.unit
    def test_handles_many_issues(self):
        """Test with many issues."""
        issues = [f"Issue {i}" for i in range(100)]
        log_transformation_summary(
            original_count=1000,
            final_count=500,
            duplicates_removed=200,
            invalid_removed=300,
            issues=issues
        )


# ============================================================================
# Tests for transform_transactions Function (Main Pipeline)
# ============================================================================

class TestTransformTransactions:
    """Tests for the main transform_transactions function."""

    @pytest.mark.integration
    def test_successful_transformation(self, clean_transform_data):
        """Test successful transformation of valid data."""
        result = transform_transactions(clean_transform_data)

        assert isinstance(result, dict)
        assert 'fact_data' in result
        assert 'dim_date' in result
        assert 'dim_category' in result
        assert 'dim_merchant' in result
        assert 'dim_payment_method' in result
        assert 'dim_user' in result

    @pytest.mark.integration
    def test_fact_data_structure(self, clean_transform_data):
        """Test that fact_data has correct structure."""
        result = transform_transactions(clean_transform_data)
        fact_data = result['fact_data']

        # Check required columns
        required_cols = ['transaction_id', 'date', 'category', 'merchant',
                        'payment_method', 'user_id', 'amount', 'date_key']
        for col in required_cols:
            assert col in fact_data.columns

    @pytest.mark.integration
    def test_date_key_in_fact_data(self, clean_transform_data):
        """Test that date_key is added to fact data."""
        result = transform_transactions(clean_transform_data)
        fact_data = result['fact_data']

        assert 'date_key' in fact_data.columns
        # Should be integer in YYYYMMDD format
        assert fact_data['date_key'].dtype in [int, 'int64', 'Int64']

    @pytest.mark.integration
    @pytest.mark.error_handling
    def test_raises_error_on_empty_input(self, empty_dataframe):
        """Test that ValueError is raised for empty input."""
        with pytest.raises(ValueError, match=r"empty"):
            transform_transactions(empty_dataframe)

    @pytest.mark.integration
    @pytest.mark.error_handling
    def test_raises_error_on_none_input(self):
        """Test that ValueError is raised for None input."""
        with pytest.raises(ValueError, match=r"empty|None"):
            transform_transactions(None)

    @pytest.mark.integration
    @pytest.mark.error_handling
    def test_raises_error_when_all_invalid(self):
        """Test that ValueError is raised when all records are invalid."""
        # Create DataFrame where all records will be invalid
        df = pd.DataFrame({
            "transaction_id": ["TXN001", "TXN002"],
            "date": ["2019-01-01", "2019-01-02"],  # Too old
            "category": ["InvalidCat1", "InvalidCat2"],  # Invalid categories
            "amount": [-10.00, -20.00],  # Negative amounts
            "merchant": ["Store A", "Store B"],
            "payment_method": ["InvalidPayment", "AnotherInvalid"],  # Invalid
            "user_id": ["bad", "worse"]  # Invalid user_ids
        })

        with pytest.raises(ValueError, match=r"No valid records"):
            transform_transactions(df)

    @pytest.mark.integration
    def test_handles_dirty_data(self, dirty_transform_data):
        """Test transformation of dirty data with duplicates and formatting issues."""
        result = transform_transactions(dirty_transform_data)
        fact_data = result['fact_data']

        # Should have removed duplicates
        assert len(fact_data) <= len(dirty_transform_data)

        # Should have standardized text
        for category in fact_data['category']:
            assert category == category.strip().title()

    @pytest.mark.integration
    def test_filters_invalid_records(self, invalid_transform_data):
        """Test that invalid records are filtered out."""
        result = transform_transactions(invalid_transform_data)
        fact_data = result['fact_data']

        # Should have fewer records after filtering
        assert len(fact_data) < len(invalid_transform_data)

        # All amounts should be valid
        assert (fact_data['amount'] > 0).all()
        assert (fact_data['amount'] <= MAX_AMOUNT).all()

    @pytest.mark.integration
    def test_all_dimensions_populated(self, clean_transform_data):
        """Test that all dimensions are populated with data."""
        result = transform_transactions(clean_transform_data)

        assert len(result['dim_date']) > 0
        assert len(result['dim_category']) > 0
        assert len(result['dim_merchant']) > 0
        assert len(result['dim_payment_method']) > 0
        assert len(result['dim_user']) > 0

    @pytest.mark.integration
    def test_fact_count_matches_valid_count(self, clean_transform_data):
        """Test that fact record count matches validated record count."""
        result = transform_transactions(clean_transform_data)

        # For clean data, should match input count
        assert len(result['fact_data']) == len(clean_transform_data)

    @pytest.mark.integration
    def test_dimension_cardinality(self, clean_transform_data):
        """Test that dimension cardinalities match fact data."""
        result = transform_transactions(clean_transform_data)
        fact_data = result['fact_data']

        # Date dimension should have all unique dates
        assert len(result['dim_date']) == fact_data['date'].nunique()

        # Category dimension should have all unique categories
        assert len(result['dim_category']) == fact_data['category'].nunique()

        # Merchant dimension should have all unique merchants
        assert len(result['dim_merchant']) == fact_data['merchant'].nunique()

        # Payment method dimension should have all unique payment methods
        assert len(result['dim_payment_method']) == fact_data['payment_method'].nunique()

        # User dimension should have all unique users
        assert len(result['dim_user']) == fact_data['user_id'].nunique()

    @pytest.mark.integration
    def test_data_types_conversion(self, clean_transform_data):
        """Test that data types are properly converted."""
        result = transform_transactions(clean_transform_data)
        fact_data = result['fact_data']

        # Amount should be float
        assert fact_data['amount'].dtype in [float, 'float64']

        # Date should be datetime
        assert pd.api.types.is_datetime64_any_dtype(fact_data['date'])

        # User_id should be integer
        assert fact_data['user_id'].dtype in [int, 'int64', 'Int64']

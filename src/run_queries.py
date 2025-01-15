"""
Query Execution and Formatting Module

This module provides functionality to execute SQL queries from the analytics
query library and display formatted results. It reads queries from sql/queries.sql
and provides both individual query execution and batch execution capabilities.

Usage:
    python -m src.run_queries
    python -m src.run_queries --query <query_number>
    python -m src.run_queries --validation
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from pathlib import Path
from typing import Optional
import re
import sys

from src.config import DB_CONFIG, BASE_DIR
from src.logger import setup_logger

logger = setup_logger(__name__)


def execute_query(query: str, description: str = None) -> list[dict]:
    """
    Execute a SQL query and return formatted results.

    Args:
        query: SQL query string to execute
        description: Optional description to display before results

    Returns:
        List of dictionaries containing query results

    Raises:
        psycopg2.Error: If database connection or query execution fails
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        if description:
            print("\n" + "=" * 100)
            print(description)
            print("=" * 100)

        cursor.execute(query)
        results = cursor.fetchall()

        if not results:
            print("No results returned.")
            return []

        # Convert RealDictRow objects to regular dictionaries
        results_list = [dict(row) for row in results]

        # Format and display as table
        _display_table(results_list)

        print(f"\nRows returned: {len(results_list)}")

        cursor.close()
        conn.close()

        return results_list

    except psycopg2.Error as e:
        logger.error(f"Database error during query execution: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during query execution: {e}")
        raise


def _display_table(results: list[dict]) -> None:
    """
    Display query results in a formatted table.

    Args:
        results: List of dictionaries containing query results
    """
    if not results:
        return

    # Get column headers
    headers = list(results[0].keys())

    # Calculate column widths (minimum 10, maximum 50 characters)
    col_widths = {}
    for header in headers:
        # Start with header length
        max_width = len(str(header))
        # Check all values in this column
        for row in results[:100]:  # Sample first 100 rows for width calculation
            value_len = len(str(row[header]))
            max_width = max(max_width, value_len)
        # Apply min/max constraints
        col_widths[header] = min(max(max_width, 10), 50)

    # Print header
    header_line = " | ".join(str(h).ljust(col_widths[h]) for h in headers)
    print("\n" + header_line)
    print("-" * len(header_line))

    # Print rows (limit to first 100 for display)
    display_limit = 100
    for i, row in enumerate(results[:display_limit]):
        row_values = []
        for header in headers:
            value = row[header]
            # Format numeric values
            if isinstance(value, float):
                value_str = f"{value:.2f}"
            else:
                value_str = str(value) if value is not None else "NULL"
            # Truncate if too long
            if len(value_str) > col_widths[header]:
                value_str = value_str[:col_widths[header]-3] + "..."
            row_values.append(value_str.ljust(col_widths[header]))
        print(" | ".join(row_values))

    # Show truncation message if needed
    if len(results) > display_limit:
        print(f"\n... (showing first {display_limit} of {len(results)} rows)")


def parse_queries_file(file_path: Path) -> list[tuple[str, str]]:
    """
    Parse SQL queries from queries.sql file.

    The file is organized with section headers (comments starting with --) and
    individual query comments. This function extracts each query with its description.

    Args:
        file_path: Path to SQL queries file

    Returns:
        List of tuples containing (query_description, query_sql)
    """
    with open(file_path, 'r') as f:
        content = f.read()

    queries = []

    # Split by double newlines to separate queries
    sections = content.split('\n\n\n')

    for section in sections:
        section = section.strip()
        if not section or section.startswith('--===='):
            continue

        # Find the query description (comment before SELECT/WITH)
        lines = section.split('\n')
        description_lines = []
        query_lines = []
        in_query = False

        for line in lines:
            if line.strip().startswith('-- Purpose:'):
                # Extract purpose as description
                description_lines.append(line.replace('-- Purpose:', '').strip())
            elif line.strip().upper().startswith(('SELECT', 'WITH')) or in_query:
                in_query = True
                query_lines.append(line)

        if query_lines:
            description = ' '.join(description_lines) if description_lines else "Query"
            query = '\n'.join(query_lines).strip()
            if query.endswith(';'):
                query = query[:-1]  # Remove trailing semicolon
            queries.append((description, query))

    return queries


def run_validation_queries() -> dict[str, any]:
    """
    Run data validation queries and return results.

    Returns:
        Dictionary containing validation results:
        - orphaned_records: Count of orphaned fact records (should be 0)
        - duplicate_transactions: Count of duplicate transaction_ids (should be 0)
        - record_counts: Dictionary of table names and their record counts
    """
    logger.info("Running data validation queries...")

    # Record counts query
    record_counts_query = """
    SELECT 'fact_transactions' AS table_name, COUNT(*) AS record_count FROM fact_transactions
    UNION ALL
    SELECT 'dim_date', COUNT(*) FROM dim_date
    UNION ALL
    SELECT 'dim_category', COUNT(*) FROM dim_category
    UNION ALL
    SELECT 'dim_merchant', COUNT(*) FROM dim_merchant
    UNION ALL
    SELECT 'dim_payment_method', COUNT(*) FROM dim_payment_method
    UNION ALL
    SELECT 'dim_user', COUNT(*) FROM dim_user
    ORDER BY table_name
    """

    # Orphaned records query
    orphaned_query = """
    SELECT COUNT(*) AS orphaned_records
    FROM fact_transactions f
    WHERE NOT EXISTS (SELECT 1 FROM dim_date d WHERE d.date_key = f.date_key)
       OR NOT EXISTS (SELECT 1 FROM dim_category c WHERE c.category_key = f.category_key)
       OR NOT EXISTS (SELECT 1 FROM dim_merchant m WHERE m.merchant_key = f.merchant_key)
       OR NOT EXISTS (SELECT 1 FROM dim_payment_method pm WHERE pm.payment_method_key = f.payment_method_key)
       OR NOT EXISTS (SELECT 1 FROM dim_user u WHERE u.user_key = f.user_key)
    """

    # Duplicates query
    duplicates_query = """
    SELECT transaction_id, COUNT(*) AS duplicate_count
    FROM fact_transactions
    GROUP BY transaction_id
    HAVING COUNT(*) > 1
    """

    # Data quality query
    data_quality_query = """
    SELECT
        COUNT(*) AS total_transactions,
        MIN(amount) AS min_amount,
        MAX(amount) AS max_amount,
        AVG(amount) AS avg_amount,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) AS median_amount
    FROM fact_transactions
    """

    results = {}

    # Execute queries
    print("\n" + "=" * 100)
    print("DATA VALIDATION REPORT")
    print("=" * 100)

    # Record counts
    record_counts = execute_query(record_counts_query, "1. Record Counts Across All Tables")
    results['record_counts'] = {row['table_name']: row['record_count'] for row in record_counts}

    # Orphaned records
    orphaned = execute_query(orphaned_query, "2. Check for Orphaned Fact Records (Should be 0)")
    results['orphaned_records'] = orphaned[0]['orphaned_records'] if orphaned else 0

    # Duplicates
    duplicates = execute_query(duplicates_query, "3. Check for Duplicate Transaction IDs (Should be Empty)")
    results['duplicate_transactions'] = len(duplicates)

    # Data quality
    data_quality = execute_query(data_quality_query, "4. Transaction Amount Data Quality")
    results['data_quality'] = data_quality[0] if data_quality else {}

    # Summary
    print("\n" + "=" * 100)
    print("VALIDATION SUMMARY")
    print("=" * 100)

    all_passed = (
        results['orphaned_records'] == 0 and
        results['duplicate_transactions'] == 0
    )

    if all_passed:
        print("✓ All validation checks PASSED")
        print(f"✓ No orphaned records: {results['orphaned_records']}")
        print(f"✓ No duplicate transaction IDs: {results['duplicate_transactions']}")
    else:
        print("✗ Some validation checks FAILED")
        if results['orphaned_records'] > 0:
            print(f"✗ Found {results['orphaned_records']} orphaned records")
        if results['duplicate_transactions'] > 0:
            print(f"✗ Found {results['duplicate_transactions']} duplicate transaction IDs")

    return results


def run_all_queries(limit: Optional[int] = None) -> None:
    """
    Execute all queries from sql/queries.sql.

    Args:
        limit: Optional limit on number of queries to run (for testing)
    """
    logger.info("Executing all queries from sql/queries.sql...")

    queries_file = BASE_DIR / 'sql' / 'queries.sql'

    if not queries_file.exists():
        logger.error(f"Queries file not found: {queries_file}")
        raise FileNotFoundError(f"Queries file not found: {queries_file}")

    queries = parse_queries_file(queries_file)
    logger.info(f"Found {len(queries)} queries to execute")

    if limit:
        queries = queries[:limit]
        logger.info(f"Limiting execution to first {limit} queries")

    for i, (description, query) in enumerate(queries, 1):
        try:
            execute_query(query, f"Query {i}: {description}")
        except Exception as e:
            logger.error(f"Failed to execute query {i}: {e}")
            print(f"\nERROR executing query {i}: {e}")
            continue


def run_sample_queries() -> None:
    """
    Run a curated selection of sample queries for demonstration.
    """
    logger.info("Running sample analytics queries...")

    samples = [
        ("Top 5 Spending Categories", """
            SELECT
                c.category_name,
                COUNT(*) AS transaction_count,
                SUM(f.amount) AS total_spending,
                AVG(f.amount) AS avg_transaction,
                ROUND(SUM(f.amount) * 100.0 / (SELECT SUM(amount) FROM fact_transactions), 2) AS percentage_of_total
            FROM fact_transactions f
            JOIN dim_category c ON f.category_key = c.category_key
            GROUP BY c.category_name
            ORDER BY total_spending DESC
            LIMIT 5
        """),

        ("Monthly Spending Trends (Last 6 Months)", """
            SELECT
                d.year,
                d.month,
                d.month_name,
                COUNT(*) AS transaction_count,
                SUM(f.amount) AS total_spending,
                AVG(f.amount) AS avg_transaction_amount
            FROM fact_transactions f
            JOIN dim_date d ON f.date_key = d.date_key
            GROUP BY d.year, d.month, d.month_name
            ORDER BY d.year DESC, d.month DESC
            LIMIT 6
        """),

        ("Top 10 Merchants by Spending", """
            SELECT
                m.merchant_name,
                c.category_name,
                COUNT(*) AS transaction_count,
                SUM(f.amount) AS total_spent,
                AVG(f.amount) AS avg_transaction
            FROM fact_transactions f
            JOIN dim_merchant m ON f.merchant_key = m.merchant_key
            JOIN dim_category c ON f.category_key = c.category_key
            GROUP BY m.merchant_name, c.category_name
            ORDER BY total_spent DESC
            LIMIT 10
        """),
    ]

    for description, query in samples:
        execute_query(query, description)


if __name__ == '__main__':
    """
    Main execution block for standalone usage.

    Usage examples:
        python -m src.run_queries                    # Run sample queries
        python -m src.run_queries --validation       # Run validation queries only
        python -m src.run_queries --all              # Run all queries from file
        python -m src.run_queries --all --limit 5    # Run first 5 queries only
    """
    import argparse

    parser = argparse.ArgumentParser(
        description='Execute SQL analytics queries and display formatted results'
    )
    parser.add_argument(
        '--validation',
        action='store_true',
        help='Run data validation queries only'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Run all queries from sql/queries.sql'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of queries to execute (use with --all)'
    )

    args = parser.parse_args()

    try:
        if args.validation:
            validation_results = run_validation_queries()
            logger.info("Validation queries completed successfully")
        elif args.all:
            run_all_queries(limit=args.limit)
            logger.info("All queries executed successfully")
        else:
            # Default: run sample queries
            run_sample_queries()
            logger.info("Sample queries completed successfully")

    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        sys.exit(1)

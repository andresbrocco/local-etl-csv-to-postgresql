"""
ETL Pipeline Orchestration Module

This module serves as the main entry point for the complete ETL pipeline.
It coordinates the Extract, Transform, and Load phases with comprehensive:
- Error handling and custom exceptions
- Command-line interface (CLI) with multiple options
- Pre-flight validation checks
- Execution time tracking and statistics
- Formatted console output and logging
- Dry-run and validation-only modes
- Graceful error handling and appropriate exit codes
"""

import sys
import os
import time
import logging
import argparse
from pathlib import Path

from src.logger import setup_logger
from src.config import TRANSACTIONS_CSV
from src.extract import extract_transactions
from src.transform import transform_transactions
from src.load import load_data_warehouse, database_connection

# Set up logger for this module
logger = setup_logger(__name__)


# ============================================================================
# Custom Exception Classes
# ============================================================================

class ETLError(Exception):
    """Base exception for ETL pipeline errors"""
    pass


class ExtractError(ETLError):
    """Error during extraction phase"""
    pass


class TransformError(ETLError):
    """Error during transformation phase"""
    pass


class LoadError(ETLError):
    """Error during load phase"""
    pass


class ValidationError(ETLError):
    """Error during validation"""
    pass


# ============================================================================
# Validation Functions
# ============================================================================

def validate_prerequisites() -> tuple[bool, list]:
    """
    Validate all prerequisites before running ETL.

    Performs comprehensive pre-flight checks:
    1. Database connection
    2. Required tables exist (fact and dimensions)
    3. Source file exists and is readable

    Returns:
        Tuple of (is_valid, list_of_issues)
        - is_valid: True if all checks pass
        - list_of_issues: List of error/warning messages

    Example:
        >>> is_valid, issues = validate_prerequisites()
        >>> if not is_valid:
        >>>     for issue in issues:
        >>>         print(f"Issue: {issue}")
    """
    issues = []

    logger.info("=" * 80)
    logger.info("Validating prerequisites...")
    logger.info("=" * 80)

    # Check database connection
    try:
        with database_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            logger.info(f"Database connection successful")
            logger.info(f"PostgreSQL version: {version.split(',')[0]}")
            cursor.close()
    except Exception as e:
        issue = f"Database connection failed: {str(e)}"
        issues.append(issue)
        logger.error(issue)

    # Check required tables exist
    if not issues:  # Only check tables if DB connection works
        try:
            with database_connection() as conn:
                cursor = conn.cursor()

                # Query for all tables in public schema
                cursor.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND (table_name LIKE 'dim_%' OR table_name = 'fact_transactions')
                    ORDER BY table_name
                """)

                existing_tables = [row[0] for row in cursor.fetchall()]

                # Required tables for the ETL pipeline
                required_tables = [
                    'fact_transactions',
                    'dim_date',
                    'dim_category',
                    'dim_merchant',
                    'dim_payment_method',
                    'dim_user'
                ]

                # Check for missing tables
                missing_tables = set(required_tables) - set(existing_tables)

                if missing_tables:
                    issue = f"Missing required tables: {', '.join(sorted(missing_tables))}"
                    issues.append(issue)
                    logger.error(issue)
                    logger.error("Run database schema setup: sql/schema.sql")
                else:
                    logger.info(f"All required tables exist ({len(required_tables)} tables)")
                    for table in sorted(existing_tables):
                        # Get row count for each table
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cursor.fetchone()[0]
                        logger.info(f"  - {table}: {count:,} rows")

                cursor.close()

        except Exception as e:
            issue = f"Table validation failed: {str(e)}"
            issues.append(issue)
            logger.error(issue)

    # Check source file exists
    source_file = Path(TRANSACTIONS_CSV)
    if not source_file.exists():
        issue = f"Source file not found: {source_file}"
        issues.append(issue)
        logger.error(issue)
    else:
        # Get file size
        file_size = source_file.stat().st_size
        file_size_mb = file_size / (1024 * 1024)
        logger.info(f"Source file exists: {source_file}")
        logger.info(f"File size: {file_size_mb:.2f} MB")

        # Check if file is readable
        if not os.access(source_file, os.R_OK):
            issue = f"Source file is not readable: {source_file}"
            issues.append(issue)
            logger.error(issue)

    # Summary
    logger.info("=" * 80)
    if len(issues) == 0:
        logger.info("All prerequisites validated successfully")
    else:
        logger.error(f"Validation failed with {len(issues)} issue(s)")

    logger.info("=" * 80)

    return len(issues) == 0, issues


# ============================================================================
# Main Pipeline Function
# ============================================================================

def run_etl_pipeline(file_path: str, dry_run: bool = False) -> dict:
    """
    Execute the complete ETL pipeline.

    Coordinates all three ETL phases (Extract, Transform, Load) with
    comprehensive error handling and execution tracking.

    Execution Flow:
    1. Initialize tracking variables
    2. Extract: Read and validate CSV data
    3. Transform: Clean, validate, and prepare data
    4. Load: Insert into data warehouse (unless dry_run)
    5. Calculate statistics and execution time
    6. Return results

    Args:
        file_path: Path to the source CSV file
        dry_run: If True, skip the load phase (default: False)

    Returns:
        Dictionary with execution results:
        {
            'status': 'success' | 'failure',
            'execution_time_seconds': float,
            'extract': int,  # records extracted
            'transform': int,  # records transformed
            'load': int,  # records loaded (facts_inserted)
            'facts_skipped': int,  # duplicate records
            'dimensions_inserted': dict,  # per-dimension counts
            'error': str | None
        }

    Raises:
        ExtractError: If extraction phase fails
        TransformError: If transformation phase fails
        LoadError: If load phase fails

    Example:
        >>> results = run_etl_pipeline("data/transactions.csv")
        >>> if results['status'] == 'success':
        >>>     print(f"Loaded {results['load']} records")
        >>> else:
        >>>     print(f"Pipeline failed: {results['error']}")
    """
    start_time = time.time()

    # Initialize pipeline state tracking
    pipeline_state = {
        'extract': 0,
        'transform': 0,
        'load': 0,
        'facts_skipped': 0,
        'dimensions_inserted': {}
    }

    try:
        # ====================================================================
        # PIPELINE START
        # ====================================================================
        logger.info("")
        logger.info("=" * 80)
        logger.info("STARTING ETL PIPELINE")
        logger.info("=" * 80)
        logger.info(f"Source file: {file_path}")
        logger.info(f"Mode: {'DRY RUN (no database writes)' if dry_run else 'FULL EXECUTION'}")
        logger.info(f"Start time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 80)
        logger.info("")

        # ====================================================================
        # PHASE 1: EXTRACT
        # ====================================================================
        logger.info("PHASE 1/3: EXTRACT")
        logger.info("-" * 80)

        try:
            df_raw = extract_transactions(file_path)
            pipeline_state['extract'] = len(df_raw)
            logger.info(f"Extract phase completed: {pipeline_state['extract']:,} records")
        except Exception as e:
            error_msg = f"Extract phase failed: {str(e)}"
            logger.error(error_msg)
            raise ExtractError(error_msg) from e

        # ====================================================================
        # PHASE 2: TRANSFORM
        # ====================================================================
        logger.info("")
        logger.info("PHASE 2/3: TRANSFORM")
        logger.info("-" * 80)

        try:
            transformed_data = transform_transactions(df_raw)
            pipeline_state['transform'] = len(transformed_data['fact_data'])
            logger.info(f"Transform phase completed: {pipeline_state['transform']:,} valid records")
        except Exception as e:
            error_msg = f"Transform phase failed: {str(e)}"
            logger.error(error_msg)
            raise TransformError(error_msg) from e

        # ====================================================================
        # PHASE 3: LOAD
        # ====================================================================
        logger.info("")
        logger.info("PHASE 3/3: LOAD")
        logger.info("-" * 80)

        if dry_run:
            logger.info("DRY RUN MODE: Skipping load phase")
            logger.info(f"Would have loaded {pipeline_state['transform']:,} records to database")
            pipeline_state['load'] = 0
            pipeline_state['facts_skipped'] = 0
            pipeline_state['dimensions_inserted'] = {
                'dim_date': 0,
                'dim_category': 0,
                'dim_merchant': 0,
                'dim_payment_method': 0,
                'dim_user': 0
            }
        else:
            try:
                load_results = load_data_warehouse(transformed_data)
                pipeline_state['load'] = load_results['facts_inserted']
                pipeline_state['facts_skipped'] = load_results['facts_skipped']
                pipeline_state['dimensions_inserted'] = load_results['dimensions_inserted']
                logger.info(f"Load phase completed: {pipeline_state['load']:,} records inserted")
            except Exception as e:
                error_msg = f"Load phase failed: {str(e)}"
                logger.error(error_msg)
                raise LoadError(error_msg) from e

        # ====================================================================
        # PIPELINE SUCCESS
        # ====================================================================
        execution_time = time.time() - start_time

        logger.info("")
        logger.info("=" * 80)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Execution time: {execution_time:.2f} seconds")
        logger.info(f"Records processed: Extract({pipeline_state['extract']:,}) -> Transform({pipeline_state['transform']:,}) -> Load({pipeline_state['load']:,})")
        logger.info("=" * 80)
        logger.info("")

        return {
            'status': 'success',
            'execution_time_seconds': round(execution_time, 2),
            'extract': pipeline_state['extract'],
            'transform': pipeline_state['transform'],
            'load': pipeline_state['load'],
            'facts_skipped': pipeline_state['facts_skipped'],
            'dimensions_inserted': pipeline_state['dimensions_inserted'],
            'error': None
        }

    except (ExtractError, TransformError, LoadError) as e:
        # Handle known ETL errors
        execution_time = time.time() - start_time

        logger.error("")
        logger.error("=" * 80)
        logger.error("ETL PIPELINE FAILED")
        logger.error("=" * 80)
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        logger.error(f"Execution time: {execution_time:.2f} seconds")
        logger.error(f"Pipeline state at failure:")
        logger.error(f"  - Extract: {pipeline_state['extract']:,} records")
        logger.error(f"  - Transform: {pipeline_state['transform']:,} records")
        logger.error(f"  - Load: {pipeline_state['load']:,} records")
        logger.error("=" * 80)
        logger.error("")

        return {
            'status': 'failure',
            'execution_time_seconds': round(execution_time, 2),
            'extract': pipeline_state['extract'],
            'transform': pipeline_state['transform'],
            'load': pipeline_state['load'],
            'facts_skipped': pipeline_state['facts_skipped'],
            'dimensions_inserted': pipeline_state['dimensions_inserted'],
            'error': str(e)
        }

    except Exception as e:
        # Handle unexpected errors
        execution_time = time.time() - start_time

        logger.error("")
        logger.error("=" * 80)
        logger.error("ETL PIPELINE FAILED WITH UNEXPECTED ERROR")
        logger.error("=" * 80)
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        logger.error(f"Execution time: {execution_time:.2f} seconds")
        logger.error("=" * 80)
        logger.error("")

        return {
            'status': 'failure',
            'execution_time_seconds': round(execution_time, 2),
            'extract': pipeline_state['extract'],
            'transform': pipeline_state['transform'],
            'load': pipeline_state['load'],
            'facts_skipped': pipeline_state['facts_skipped'],
            'dimensions_inserted': pipeline_state['dimensions_inserted'],
            'error': f"Unexpected error: {str(e)}"
        }


# ============================================================================
# Output Formatting Functions
# ============================================================================

def print_pipeline_summary(results: dict) -> None:
    """
    Print formatted pipeline execution summary.

    Displays comprehensive execution statistics in a user-friendly format:
    - Pipeline status (success/failure)
    - Execution time
    - Records at each stage
    - Dimension loading statistics
    - Error details if failed

    Args:
        results: Pipeline results dictionary from run_etl_pipeline()

    Example:
        >>> results = run_etl_pipeline("data/transactions.csv")
        >>> print_pipeline_summary(results)
    """
    print("\n" + "=" * 80)
    print("ETL PIPELINE EXECUTION SUMMARY")
    print("=" * 80)

    if results['status'] == 'success':
        print(f"\nStatus: SUCCESS")
        print(f"Execution Time: {results['execution_time_seconds']:.2f} seconds")

        print("\nRecords Processed:")
        print(f"  - Extracted: {results['extract']:,}")
        print(f"  - Transformed: {results['transform']:,}")
        print(f"  - Loaded: {results['load']:,}")

        if results.get('facts_skipped', 0) > 0:
            print(f"  - Skipped (duplicates): {results['facts_skipped']:,}")

        # Dimension statistics
        dims = results.get('dimensions_inserted', {})
        if dims and any(dims.values()):
            print("\nDimension Records Inserted:")
            for dim_name, count in sorted(dims.items()):
                print(f"  - {dim_name}: {count:,}")

        # Data quality metrics
        if results['extract'] > 0:
            transform_rate = (results['transform'] / results['extract']) * 100
            print(f"\nData Quality:")
            print(f"  - Transformation success rate: {transform_rate:.2f}%")

            if results['transform'] > results['extract']:
                lost_records = results['extract'] - results['transform']
                print(f"  - Records filtered/cleaned: {lost_records:,}")

    else:
        print(f"\nStatus: FAILURE")
        print(f"Execution Time: {results['execution_time_seconds']:.2f} seconds")

        print("\nPipeline State at Failure:")
        print(f"  - Extracted: {results['extract']:,} records")
        print(f"  - Transformed: {results['transform']:,} records")
        print(f"  - Loaded: {results['load']:,} records")

        print(f"\nError Details:")
        print(f"  {results['error']}")

    print("=" * 80 + "\n")


# ============================================================================
# Command-Line Interface
# ============================================================================

def parse_arguments():
    """
    Parse command-line arguments.

    Supports multiple execution modes:
    - --file: Specify input CSV file
    - --dry-run: Test extract/transform without loading
    - --validate-only: Check prerequisites without running
    - --verbose: Enable debug logging

    Returns:
        Parsed arguments namespace

    Example:
        >>> args = parse_arguments()
        >>> if args.verbose:
        >>>     print("Verbose mode enabled")
    """
    parser = argparse.ArgumentParser(
        description='Personal Finance ETL Pipeline - CSV to PostgreSQL Data Warehouse',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline with default file
  python -m src.etl_pipeline

  # Run with custom file
  python -m src.etl_pipeline --file data/custom_transactions.csv

  # Dry run (no database writes)
  python -m src.etl_pipeline --dry-run

  # Validate prerequisites only
  python -m src.etl_pipeline --validate-only

  # Verbose output
  python -m src.etl_pipeline --verbose

For more information, see README.md
        """
    )

    parser.add_argument(
        '--file',
        type=str,
        default=str(TRANSACTIONS_CSV),
        help=f'Path to CSV file (default: {TRANSACTIONS_CSV})'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run extract and transform phases only, skip load phase'
    )

    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Validate prerequisites (DB connection, tables, file) without running ETL'
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose/debug logging output'
    )

    return parser.parse_args()


# ============================================================================
# Main Execution
# ============================================================================

if __name__ == '__main__':
    """
    Main entry point for the ETL pipeline.

    This allows running the pipeline from the command line:
        $ source venv/bin/activate.fish
        $ python -m src.etl_pipeline

    Or with options:
        $ python -m src.etl_pipeline --file data/transactions.csv --verbose
        $ python -m src.etl_pipeline --dry-run
        $ python -m src.etl_pipeline --validate-only

    Exit codes:
        0: Success
        1: General failure
        130: Interrupted by user (Ctrl+C)
    """
    try:
        # Parse command-line arguments
        args = parse_arguments()

        # Configure logging level
        if args.verbose:
            # Set root logger to DEBUG for verbose output
            logging.getLogger().setLevel(logging.DEBUG)
            logger.setLevel(logging.DEBUG)
            logger.debug("Verbose logging enabled")

        # Handle --validate-only mode
        if args.validate_only:
            print("\nRunning prerequisite validation...\n")
            is_valid, issues = validate_prerequisites()

            if is_valid:
                print("\n" + "=" * 80)
                print("VALIDATION SUCCESSFUL")
                print("=" * 80)
                print("\nAll prerequisites validated successfully.")
                print("The ETL pipeline is ready to run.")
                print("=" * 80 + "\n")
                sys.exit(0)
            else:
                print("\n" + "=" * 80)
                print("VALIDATION FAILED")
                print("=" * 80)
                print(f"\nFound {len(issues)} issue(s):")
                for idx, issue in enumerate(issues, 1):
                    print(f"  {idx}. {issue}")
                print("\nPlease resolve these issues before running the ETL pipeline.")
                print("=" * 80 + "\n")
                sys.exit(1)

        # Validate prerequisites before running pipeline
        print("\nValidating prerequisites before running pipeline...\n")
        is_valid, issues = validate_prerequisites()

        if not is_valid:
            print("\n" + "=" * 80)
            print("PREREQUISITE VALIDATION FAILED")
            print("=" * 80)
            logger.error("Prerequisites validation failed")
            for issue in issues:
                logger.error(f"  - {issue}")
            print("\nCannot proceed with ETL pipeline.")
            print("=" * 80 + "\n")
            sys.exit(1)

        # Run the ETL pipeline
        print("\nStarting ETL pipeline...\n")
        results = run_etl_pipeline(
            file_path=args.file,
            dry_run=args.dry_run
        )

        # Print summary
        print_pipeline_summary(results)

        # Exit with appropriate code
        if results['status'] == 'success':
            logger.info("Pipeline completed successfully")
            sys.exit(0)
        else:
            logger.error("Pipeline failed")
            sys.exit(1)

    except KeyboardInterrupt:
        # Handle Ctrl+C gracefully
        print("\n\n" + "=" * 80)
        print("PIPELINE INTERRUPTED BY USER")
        print("=" * 80)
        logger.warning("Pipeline interrupted by user (Ctrl+C)")
        print("=" * 80 + "\n")
        sys.exit(130)

    except Exception as e:
        # Handle any unexpected errors
        print("\n" + "=" * 80)
        print("UNEXPECTED ERROR")
        print("=" * 80)
        logger.error(f"Unexpected error in main: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")

        # Print stack trace in verbose mode
        if '--verbose' in sys.argv:
            import traceback
            traceback.print_exc()

        print("=" * 80 + "\n")
        sys.exit(1)

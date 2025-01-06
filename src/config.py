"""
Configuration module for the ETL pipeline.

This module centralizes all configuration settings including:
- Project paths
- Data sources
- Database configuration
- Logging settings
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ============================================================================
# Project Paths
# ============================================================================

# Base directory (project root)
BASE_DIR = Path(__file__).resolve().parent.parent

# Data directory
DATA_DIR = BASE_DIR / "data"

# Logs directory
LOG_DIR = BASE_DIR / "logs"

# SQL scripts directory
SQL_DIR = BASE_DIR / "sql"

# Source directory
SRC_DIR = BASE_DIR / "src"

# ============================================================================
# Data Sources
# ============================================================================

# Transactions CSV file path
TRANSACTIONS_CSV = DATA_DIR / "transactions.csv"

# Required columns in the transactions CSV
REQUIRED_CSV_COLUMNS = [
    "transaction_id",
    "date",
    "category",
    "amount",
    "merchant",
    "payment_method",
    "user_id"
]

# ============================================================================
# Database Configuration
# ============================================================================

# Database connection parameters (loaded from environment variables)
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "finance_etl"),
    "user": os.getenv("DB_USER", "andresbrocco"),
    "password": os.getenv("DB_PASSWORD", "")
}

# Database schema name
DB_SCHEMA = "public"

# ============================================================================
# Logging Configuration
# ============================================================================

# Log file path
LOG_FILE = LOG_DIR / "etl_pipeline.log"

# Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Log format
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Date format for logs
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# ============================================================================
# ETL Configuration
# ============================================================================

# Batch size for database inserts
BATCH_SIZE = 1000

# Enable data validation
ENABLE_VALIDATION = True

# Maximum number of retries for database operations
MAX_DB_RETRIES = 3

# Retry delay in seconds
RETRY_DELAY = 1

"""
Logging utility module for the ETL pipeline.

This module provides a centralized logging setup that:
- Creates log directories if they don't exist
- Configures file and console handlers
- Prevents duplicate handlers
- Uses consistent formatting across the application
"""

import logging
from pathlib import Path
from src.config import LOG_DIR, LOG_FILE, LOG_LEVEL, LOG_FORMAT, LOG_DATE_FORMAT


def setup_logger(name: str) -> logging.Logger:
    """
    Set up and configure a logger with file and console handlers.

    This function creates a logger that writes to both a log file and stdout.
    It ensures the logs directory exists and prevents duplicate handlers from
    being added if the logger already exists.

    Args:
        name: The name of the logger (typically __name__ of the calling module)

    Returns:
        Configured logging.Logger instance

    Example:
        >>> from src.logger import setup_logger
        >>> logger = setup_logger(__name__)
        >>> logger.info("Processing started")
    """
    # Get or create logger
    logger = logging.getLogger(name)

    # Only configure if this logger hasn't been configured yet
    # This prevents duplicate handlers when the same logger is requested multiple times
    if logger.handlers:
        return logger

    # Set log level from configuration
    log_level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(log_level)

    # Create logs directory if it doesn't exist
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # Create formatter
    formatter = logging.Formatter(
        fmt=LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT
    )

    # File handler - writes to log file
    file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler - writes to stdout
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Prevent propagation to root logger to avoid duplicate messages
    logger.propagate = False

    return logger

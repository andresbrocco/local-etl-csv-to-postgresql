"""
Generate synthetic transaction data for ETL pipeline testing.

This script creates 10,000 realistic transaction records with weighted category
distribution and category-appropriate amounts, saved to CSV format.
"""

from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta
import uuid
import os
from pathlib import Path
from typing import Dict, Tuple, List


def set_seeds(seed: int = 42) -> None:
    """Set random seeds for reproducibility."""
    random.seed(seed)
    Faker.seed(seed)


def get_project_root() -> Path:
    """Get the project root directory."""
    return Path(__file__).parent.parent


def generate_random_date(start_date: datetime, end_date: datetime) -> str:
    """
    Generate a random date between start_date and end_date.

    Args:
        start_date: Start of date range
        end_date: End of date range

    Returns:
        Date string in YYYY-MM-DD format
    """
    time_delta = end_date - start_date
    random_days = random.randint(0, time_delta.days)
    random_date = start_date + timedelta(days=random_days)
    return random_date.strftime('%Y-%m-%d')


def select_weighted_category(categories: Dict[str, int]) -> str:
    """
    Select a category based on weighted distribution.

    Args:
        categories: Dictionary of category names and their weights

    Returns:
        Selected category name
    """
    category_list = []
    for category, weight in categories.items():
        category_list.extend([category] * weight)
    return random.choice(category_list)


def generate_transaction(
    fake: Faker,
    categories: Dict[str, int],
    amount_ranges: Dict[str, Tuple[int, int]],
    payment_methods: List[str],
    payment_methods_weights: List[float],
    start_date: datetime,
    end_date: datetime,
    num_users: int
) -> Dict:
    """
    Generate a single transaction record.

    Args:
        fake: Faker instance for generating fake data
        categories: Category weights dictionary
        amount_ranges: Amount ranges per category
        payment_methods: List of available payment methods
        start_date: Start of date range
        end_date: End of date range
        num_users: Number of unique users

    Returns:
        Dictionary containing transaction data
    """
    # Select category with weighted distribution
    category = select_weighted_category(categories)

    # Generate amount based on category
    min_amount, max_amount = amount_ranges[category]
    amount = round(random.uniform(min_amount, max_amount), 2)

    # Generate transaction record
    transaction = {
        'transaction_id': str(uuid.uuid4()),
        'date': generate_random_date(start_date, end_date),
        'category': category,
        'amount': amount,
        'merchant': fake.company(),
        'payment_method': random.choices(payment_methods, payment_methods_weights)[0],
        'user_id': random.randint(1, num_users)
    }

    return transaction


def generate_transactions(
    num_transactions: int = 10000,
    num_users: int = 100,
    years_back: int = 2
) -> pd.DataFrame:
    """
    Generate multiple transaction records.

    Args:
        num_transactions: Number of transactions to generate
        num_users: Number of unique users
        years_back: How many years back to generate data

    Returns:
        DataFrame containing all transactions
    """
    fake = Faker()

    # Define weighted categories (more common categories appear more often)
    categories = {
        'Groceries': 25,
        'Dining': 20,
        'Transportation': 15,
        'Shopping': 15,
        'Utilities': 10,
        'Entertainment': 8,
        'Healthcare': 4,
        'Travel': 3
    }

    # Amount ranges per category
    amount_ranges = {
        'Groceries': (10, 200),
        'Dining': (15, 150),
        'Transportation': (5, 100),
        'Shopping': (20, 500),
        'Utilities': (50, 300),
        'Entertainment': (10, 200),
        'Healthcare': (30, 800),
        'Travel': (100, 2000)
    }

    payment_methods = ['Credit Card', 'Debit Card', 'Cash', 'Digital Wallet']
    payment_methods_weights = [0.7, 0.15, 0.1, 0.05]

    # Define date range
    end_date = datetime.fromisoformat('2025-01-06')
    start_date = end_date - timedelta(days=365 * years_back)

    # Generate transactions
    transactions = []
    for _ in range(num_transactions):
        transaction = generate_transaction(
            fake=fake,
            categories=categories,
            amount_ranges=amount_ranges,
            payment_methods=payment_methods,
            payment_methods_weights=payment_methods_weights,
            start_date=start_date,
            end_date=end_date,
            num_users=num_users
        )
        transactions.append(transaction)

    # Create DataFrame
    df = pd.DataFrame(transactions)

    # Sort by date for better readability
    df = df.sort_values('date').reset_index(drop=True)

    return df


def print_summary_statistics(df: pd.DataFrame) -> None:
    """
    Print summary statistics for the generated data.

    Args:
        df: DataFrame containing transaction data
    """
    print("\n" + "="*60)
    print("TRANSACTION DATA GENERATION SUMMARY")
    print("="*60)
    print(f"\nTotal Transactions: {len(df):,}")
    print(f"\nDate Range:")
    print(f"  Start Date: {df['date'].min()}")
    print(f"  End Date:   {df['date'].max()}")
    print(f"\nAmount Statistics:")
    print(f"  Min Amount:  ${df['amount'].min():,.2f}")
    print(f"  Max Amount:  ${df['amount'].max():,.2f}")
    print(f"  Mean Amount: ${df['amount'].mean():,.2f}")
    print(f"  Total Value: ${df['amount'].sum():,.2f}")
    print(f"\nCategory Distribution:")
    category_counts = df['category'].value_counts().sort_values(ascending=False)
    for category, count in category_counts.items():
        percentage = (count / len(df)) * 100
        print(f"  {category:<15} {count:>6,} ({percentage:>5.1f}%)")
    print(f"\nPayment Method Distribution:")
    payment_counts = df['payment_method'].value_counts().sort_values(ascending=False)
    for method, count in payment_counts.items():
        percentage = (count / len(df)) * 100
        print(f"  {method:<15} {count:>6,} ({percentage:>5.1f}%)")
    print(f"\nUnique Users: {df['user_id'].nunique()}")
    print(f"\nData Quality Checks:")
    print(f"  Missing Values: {df.isnull().sum().sum()}")
    print(f"  Duplicate Transaction IDs: {df['transaction_id'].duplicated().sum()}")
    print("="*60 + "\n")


def main():
    """Main execution function."""
    try:
        # Set seeds for reproducibility
        set_seeds(42)

        print("\nGenerating synthetic transaction data...")

        # Generate transactions
        df = generate_transactions(
            num_transactions=10000,
            num_users=100,
            years_back=2
        )

        # Ensure data directory exists
        project_root = get_project_root()
        data_dir = project_root / 'data'
        data_dir.mkdir(exist_ok=True)

        # Save to CSV
        output_path = data_dir / 'transactions.csv'
        df.to_csv(output_path, index=False)
        print(f"\nData saved to: {output_path}")

        # Print summary statistics
        print_summary_statistics(df)

        print("Data generation completed successfully!")

    except Exception as e:
        print(f"\nError generating data: {str(e)}")
        raise


if __name__ == "__main__":
    main()

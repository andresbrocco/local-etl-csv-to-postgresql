# Local ETL Pipeline: CSV to PostgreSQL

A Python-based ETL pipeline for personal finance transaction data.

## Setup

### 1. Create virtual environment

```bash
python3 -m venv venv
source venv/bin/activate.fish  # Fish shell
# Or: source venv/bin/activate  # Bash/Zsh
# Or: venv\Scripts\activate     # Windows
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Setup PostgreSQL Database using Docker

```bash
# Start PostgreSQL container
docker-compose up -d

# Verify it's running
docker ps
```

The Docker setup creates:
- Database: `finance_etl`
- User: `andresbrocco`
- Password: `senhaforte`

### 4. Configure environment variables

```bash
cp .env.example .env
# Edit .env with your database credentials
```

For Docker setup, use these values in `.env`:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=finance_etl
DB_USER=andresbrocco
DB_PASSWORD=senhaforte
```

### 5. Test database connection

```bash
python scripts/test_connection.py
```

You should see:
```
✅ Successfully connected to PostgreSQL!
PostgreSQL version: PostgreSQL 14.x ...
```

## Manual Setup Steps Performed

### PostgreSQL Database Setup
- PostgreSQL 14 deployed using Docker container (postgres:14-alpine)
- Configured `.env` file with database credentials
- Verified connectivity with scripts/test_connection.py

## Project Status
🚧 In Development

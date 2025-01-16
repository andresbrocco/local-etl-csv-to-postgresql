# Lessons Learned

## Overview

These are my personal reflections from building this ETL pipeline. I tried to capture the important stuff I learned, mistakes I made, and what I'd do differently next time.

## Things That Worked Well

### Star Schema Was the Right Choice

I initially considered just using normalized tables (like a typical OLTP database), but after reading about dimensional modeling, I decided to go with a star schema. This turned out to be a great decision. Analytics queries are so much simpler now—what would've been 5-6 joins is now just 1-2. Plus, it's way easier to explain to non-technical people what the data model does.

### Logging Early Saved Me Hours

I set up centralized logging right from the start, and honestly, this saved me so much debugging time. When something broke (and it did, often), I could just check the logs instead of adding print statements everywhere. The time investment upfront paid off immediately.

### Tests Caught Bugs I Would've Missed

Writing tests felt slow at first, especially for data validation logic. But I caught several edge cases during testing (like handling null values, duplicate records, invalid dates) that would've been painful to debug in production. Now I can refactor confidently knowing tests have my back.

### Modular Design Made Life Easier

Separating extract, transform, and load into distinct modules was clutch. When I needed to fix a bug in the transform logic, I didn't have to worry about breaking extraction or loading. Each module had clear inputs/outputs, which made testing way easier too.

## Biggest Challenges

### The N+1 Query Problem (And How I Fixed It)

My first version of the load module was embarrassingly slow. I was querying the dimension tables for each fact record to get the foreign keys—classic N+1 query problem. For 10,000 records, this took forever.

**The fix**: Load all dimension mappings into dictionaries upfront, then do lookups in memory. Went from ~20 seconds to ~2 seconds. Lesson learned: always think about the algorithmic complexity of database operations.

### Transaction Boundaries Confused Me

I wasn't sure whether to commit after loading each dimension table or wrap everything in one transaction. I initially went with committing after each table, but then realized this could leave the database in a half-loaded state if something failed.

**The fix**: Single transaction for the entire load (all dimensions + facts). If anything fails, rollback everything. Loading a partial star schema is worse than loading nothing.

### Testing Database Code Was Tricky

I tried using SQLite as a test database at first, but it doesn't support all PostgreSQL features (like certain date functions and array operations). This led to tests passing locally but failing against real PostgreSQL.

**The fix**: Use mocking for unit tests (fast, no dependencies) and actual PostgreSQL for integration tests (slower, but accurate). Marked integration tests with `@pytest.mark.integration` so they can be run separately.

## Performance Lessons

### Batch Loading Is 10x Faster

Row-by-row inserts were painfully slow (~20 seconds for 10K records). Switching to batch inserts (1,000 records per batch) brought it down to ~2 seconds. The network round-trip overhead is no joke.

### Indexes Matter Way More Than I Expected

I ran some queries without indexes first, and they were noticeably slow (~800ms for a simple aggregation). After adding indexes on foreign keys, the same query took ~25ms. Indexing isn't premature optimization—it's essential architecture.

### pandas Has Memory Limits

My pipeline loads the entire CSV into memory using pandas. This works fine for 10K-1M records, but it won't scale to 100M+ records. If I need to process huge files later, I'll need to implement chunk processing or switch to something like Dask/Spark.

For now, I documented this assumption in the README so future me (or anyone else) knows the limitations.

## Things I'd Improve

### The Pipeline Isn't Scheduled

Right now, I run the pipeline manually via CLI. For a real production system, I'd want this scheduled (daily/hourly) using something like Apache Airflow. This would also add retry logic and better error handling.

### Limited Data Validation

I have basic validation (null checks, business rules), but I'd love to integrate a data quality framework like Great Expectations. This would let me define more sophisticated validation rules and get automatic data quality reports.

### No Monitoring Dashboard

Logs are great for debugging, but they're not great for monitoring. Ideally, I'd have a dashboard (maybe Grafana + Prometheus) showing pipeline execution history, error rates, and data quality metrics.

## Key Takeaways

1. **Good architecture early pays off**: The modular design and star schema made everything easier down the line.

2. **Test the unhappy paths**: Most bugs aren't in the happy path—they're in edge cases like null values, duplicates, and invalid data.

3. **Production-ready ≠ working code**: Error handling, logging, testing, and idempotency aren't optional. They're what makes code production-ready.

4. **Document your assumptions**: I made assumptions about file sizes, data quality, and runtime frequency. Writing these down helps others (and future me) understand the system's limits.

5. **Optimize for actual requirements**: I don't need connection pooling yet because the pipeline runs infrequently. Don't optimize for theoretical problems—optimize for real ones.

## What I Learned About Data Engineering

Building this pipeline taught me that data engineering is less about writing clever code and more about thinking through failure modes, performance bottlenecks, and operational concerns. The "boring" stuff (logging, error handling, idempotency) is actually the most important stuff.

Also, dimensional modeling isn't just academic theory—it genuinely makes analytics easier. I'm glad I took the time to learn about star schemas instead of just throwing data into normalized tables.

## Next Steps

If I continue this project, I'd add:
- Airflow for scheduling and orchestration
- Great Expectations for data quality validation
- Streamlit dashboard for interactive analytics
- Docker container for easier deployment

But for now, I'm happy with what I've built. It's a solid portfolio piece that demonstrates ETL best practices, dimensional modeling, and production-ready code patterns.

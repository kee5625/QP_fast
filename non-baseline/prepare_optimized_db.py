#!/usr/bin/env python3
"""
Preparation Script: Build Optimized DuckDB Database
----------------------------------------------------
This script creates:
1. An optimized main 'events' table (sorted for zonemap pruning)
2. Pre-aggregated summary tables for known query patterns

Run this once to prepare the database, then use the optimized
database for fast query execution.
"""

import duckdb
import time
from pathlib import Path
import argparse


def create_optimized_database(data_dir: Path, db_path: Path):
    """
    Create optimized database with main table and summary tables
    """
    print("=" * 60)
    print("Creating Optimized DuckDB Database")
    print("=" * 60)
    
    # Ensure parent directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Remove existing database
    if db_path.exists():
        print(f"Removing existing database: {db_path}")
        db_path.unlink()
    
    con = duckdb.connect(str(db_path))
    
    # Step 1: Create optimized main table
    print("\n Step 1: Creating optimized main 'events' table...")
    create_main_table(con, data_dir)
    
    # Step 2: Create summary tables
    print("\n Step 2: Creating pre-aggregated summary tables...")
    create_summary_tables(con)
    
    # Step 3: Show statistics
    print("\ Step 3: Database statistics...")
    show_statistics(con)
    
    con.close()
    print(f"\n✅ Optimized database created: {db_path}")
    print("=" * 60)


def create_main_table(con, data_dir: Path):
    """
    Create the main events table with optimizations:
    - Proper type casting
    - Pre-calculated time columns (day, minute, etc.)
    - Sorted by common filter columns for zonemap pruning
    """
    # Check for parquet or CSV files
    parquet_files = list(data_dir.glob("events_part_*.parquet"))
    csv_files = list(data_dir.glob("events_part_*.csv"))
    
    start = time.time()
    
    if parquet_files:
        print(f"   Loading from {len(parquet_files)} Parquet files...")
        con.execute(f"""
            CREATE TABLE events AS
            WITH raw AS (
              SELECT *
              FROM read_parquet('{data_dir}/events_part_*.parquet')
            ),
            casted AS (
              SELECT
                to_timestamp(TRY_CAST(ts AS DOUBLE) / 1000.0)    AS ts,
                type,
                auction_id,
                TRY_CAST(advertiser_id AS INTEGER)        AS advertiser_id,
                TRY_CAST(publisher_id  AS INTEGER)        AS publisher_id,
                TRY_CAST(bid_price AS DOUBLE)             AS bid_price,
                TRY_CAST(user_id AS BIGINT)               AS user_id,
                TRY_CAST(total_price AS DOUBLE)           AS total_price,
                country
              FROM raw
            )
            SELECT
              ts,
              DATE_TRUNC('week', ts)::DATE              AS week,
              DATE(ts)                                  AS day,
              DATE_TRUNC('hour', ts)                    AS hour,
              STRFTIME(ts, '%Y-%m-%d %H:%M')            AS minute,
              type,
              auction_id,
              advertiser_id,
              publisher_id,
              bid_price,
              user_id,
              total_price,
              country
            FROM casted
            -- CRITICAL: Sort by common filter columns for zonemap pruning
            ORDER BY day, type, country, publisher_id;
        """)
    elif csv_files:
        print(f"   Loading from {len(csv_files)} CSV files...")
        con.execute(f"""
            CREATE TABLE events AS
            WITH raw AS (
              SELECT *
              FROM read_csv(
                '{data_dir}/events_part_*.csv',
                AUTO_DETECT = FALSE,
                HEADER = TRUE,
                union_by_name = TRUE,
                COLUMNS = {{
                  'ts': 'VARCHAR',
                  'type': 'VARCHAR',
                  'auction_id': 'VARCHAR',
                  'advertiser_id': 'VARCHAR',
                  'publisher_id': 'VARCHAR',
                  'bid_price': 'VARCHAR',
                  'user_id': 'VARCHAR',
                  'total_price': 'VARCHAR',
                  'country': 'VARCHAR'
                }}
              )
            ),
            casted AS (
              SELECT
                to_timestamp(TRY_CAST(ts AS DOUBLE) / 1000.0)    AS ts,
                type,
                auction_id,
                TRY_CAST(advertiser_id AS INTEGER)        AS advertiser_id,
                TRY_CAST(publisher_id  AS INTEGER)        AS publisher_id,
                NULLIF(bid_price, '')::DOUBLE             AS bid_price,
                TRY_CAST(user_id AS BIGINT)               AS user_id,
                NULLIF(total_price, '')::DOUBLE           AS total_price,
                country
              FROM raw
            )
            SELECT
              ts,
              DATE_TRUNC('week', ts)::DATE              AS week,
              DATE(ts)                                  AS day,
              DATE_TRUNC('hour', ts)                    AS hour,
              STRFTIME(ts, '%Y-%m-%d %H:%M')            AS minute,
              type,
              auction_id,
              advertiser_id,
              publisher_id,
              bid_price,
              user_id,
              total_price,
              country
            FROM casted
            -- CRITICAL: Sort by common filter columns for zonemap pruning
            ORDER BY day, type, country, publisher_id;
        """)
    else:
        raise FileNotFoundError(f"No events_part_*.parquet or *.csv found in {data_dir}")
    
    elapsed = time.time() - start
    row_count = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    print(f"   ✅ Main table created: {row_count:,} rows in {elapsed:.2f}s")


def create_summary_tables(con):
    """
    Create pre-aggregated summary tables for each query pattern
    """
    
    # Query 1: Daily revenue from impressions
    print("\n   Creating summary table: daily_revenue...")
    start = time.time()
    con.execute("""
        CREATE TABLE daily_revenue AS
        SELECT
            day,
            SUM(bid_price) AS total_bid_price
        FROM events
        WHERE type = 'impression'
        GROUP BY day
        ORDER BY day;
    """)
    elapsed = time.time() - start
    row_count = con.execute("SELECT COUNT(*) FROM daily_revenue").fetchone()[0]
    print(f"   ✅ daily_revenue: {row_count:,} rows in {elapsed:.2f}s")
    
    # Query 2: Publisher revenue by country and day (for date range filtering)
    print("\n   Creating summary table: publisher_revenue_by_country_day...")
    start = time.time()
    con.execute("""
        CREATE TABLE publisher_revenue_by_country_day AS
        SELECT
            day,
            country,
            publisher_id,
            SUM(bid_price) AS total_bid_price
        FROM events
        WHERE type = 'impression'
        GROUP BY day, country, publisher_id
        ORDER BY day, country, publisher_id;
    """)
    elapsed = time.time() - start
    row_count = con.execute("SELECT COUNT(*) FROM publisher_revenue_by_country_day").fetchone()[0]
    print(f"   ✅ publisher_revenue_by_country_day: {row_count:,} rows in {elapsed:.2f}s")
    
    # Query 3: Average purchase price by country
    print("\n   Creating summary table: avg_purchase_by_country...")
    start = time.time()
    con.execute("""
        CREATE TABLE avg_purchase_by_country AS
        SELECT
            country,
            AVG(total_price) AS avg_total_price,
            SUM(total_price) AS sum_total_price,
            COUNT(total_price) AS count_total_price
        FROM events
        WHERE type = 'purchase'
        GROUP BY country
        ORDER BY avg_total_price DESC;
    """)
    elapsed = time.time() - start
    row_count = con.execute("SELECT COUNT(*) FROM avg_purchase_by_country").fetchone()[0]
    print(f"   ✅ avg_purchase_by_country: {row_count:,} rows in {elapsed:.2f}s")
    
    # Query 4: Event counts by advertiser and type
    print("\n   Creating summary table: advertiser_type_counts...")
    start = time.time()
    con.execute("""
        CREATE TABLE advertiser_type_counts AS
        SELECT
            advertiser_id,
            type,
            COUNT(*) AS event_count
        FROM events
        GROUP BY advertiser_id, type
        ORDER BY event_count DESC;
    """)
    elapsed = time.time() - start
    row_count = con.execute("SELECT COUNT(*) FROM advertiser_type_counts").fetchone()[0]
    print(f"   ✅ advertiser_type_counts: {row_count:,} rows in {elapsed:.2f}s")
    
    # Query 5: Minute-level revenue by day (for specific day filtering)
    print("\n   Creating summary table: minute_revenue_by_day...")
    start = time.time()
    con.execute("""
        CREATE TABLE minute_revenue_by_day AS
        SELECT
            day,
            minute,
            SUM(bid_price) AS total_bid_price
        FROM events
        WHERE type = 'impression'
        GROUP BY day, minute
        ORDER BY day, minute;
    """)
    elapsed = time.time() - start
    row_count = con.execute("SELECT COUNT(*) FROM minute_revenue_by_day").fetchone()[0]
    print(f"   ✅ minute_revenue_by_day: {row_count:,} rows in {elapsed:.2f}s")


def show_statistics(con):
    """
    Display database statistics
    """
    tables = ['events', 'daily_revenue', 'publisher_revenue_by_country_day', 
              'avg_purchase_by_country', 'advertiser_type_counts', 'minute_revenue_by_day']
    
    print("\n   Table Statistics:")
    print("   " + "-" * 50)
    
    total_size = 0
    for table in tables:
        row_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        # Estimate size (DuckDB doesn't expose exact size easily)
        print(f"   {table:40s} {row_count:>10,} rows")
    
    print("   " + "-" * 50)


def main():
    parser = argparse.ArgumentParser(
        description="Prepare optimized DuckDB database with summary tables"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("../data_parquet"),
        help="Directory containing input data (Parquet or CSV)"
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("tmp/optimized.duckdb"),
        help="Output path for optimized database"
    )
    
    args = parser.parse_args()
    
    create_optimized_database(args.data_dir, args.db_path)


if __name__ == "__main__":
    main()

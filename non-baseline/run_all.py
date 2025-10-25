#!/usr/bin/env python3
"""
All-in-One Adaptive Query Optimizer
------------------------------------
Analyzes queries, builds optimal summary tables, and executes queries.

Usage:
  python run_all.py --data-dir ../data_parquet --out-dir ../outputs
"""

import duckdb
import time
from pathlib import Path
import csv
import argparse

from adaptive_optimizer import AdaptiveOptimizer, AdaptiveQueryRouter, generate_summary_table_sql
from inputs import queries


def create_main_table(con, data_dir: Path):
    """
    Create the optimized main events table
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
            ORDER BY day, type, country, publisher_id;
        """)
    else:
        raise FileNotFoundError(f"No events_part_*.parquet or *.csv found in {data_dir}")
    
    elapsed = time.time() - start
    row_count = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    print(f"   ✅ Main table created: {row_count:,} rows in {elapsed:.2f}s")


def main():
    parser = argparse.ArgumentParser(
        description="Adaptive query optimizer - analyzes queries and builds optimal summary tables"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        required=True,
        help="Directory containing input data (Parquet or CSV)"
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="Where to output query results"
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("tmp/optimized.duckdb"),
        help="Path for the optimized database (default: tmp/optimized.duckdb)"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("🧠 ADAPTIVE QUERY OPTIMIZER")
    print("=" * 60)
    
    # Phase 1: Analyze queries
    print("\n📊 Phase 1: Analyzing Queries")
    print("-" * 60)
    
    optimizer = AdaptiveOptimizer(queries)
    summary_specs = optimizer.analyze_queries()
    
    print(f"   Analyzed {len(queries)} queries")
    print(f"   Identified {len(summary_specs)} summary tables to create:")
    for spec in summary_specs:
        print(f"     - {spec['table_name']} (Q{spec['query_num']})")
    
    # Phase 2: Build optimized database
    print("\n📊 Phase 2: Building Optimized Database")
    print("-" * 60)
    
    # Ensure parent directory exists
    args.db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Remove existing database
    if args.db_path.exists():
        print(f"   🗑️  Removing existing database")
        args.db_path.unlink()
    
    con = duckdb.connect(str(args.db_path))
    
    # Create main table
    print("\n   Step 1: Creating main 'events' table...")
    create_main_table(con, args.data_dir)
    
    # Create summary tables
    print("\n   Step 2: Creating summary tables...")
    for spec in summary_specs:
        print(f"\n     Creating {spec['table_name']}...")
        sql = generate_summary_table_sql(spec)
        
        start = time.time()
        con.execute(sql)
        elapsed = time.time() - start
        
        row_count = con.execute(f"SELECT COUNT(*) FROM {spec['table_name']}").fetchone()[0]
        print(f"     ✅ {row_count:,} rows in {elapsed:.2f}s")
    
    con.close()
    
    # Phase 3: Execute queries
    print("\n📊 Phase 3: Executing Queries")
    print("-" * 60)
    
    # Ensure output directory exists
    args.out_dir.mkdir(parents=True, exist_ok=True)
    
    # Reconnect to database
    con = duckdb.connect(str(args.db_path), read_only=True)
    
    # Initialize adaptive router
    router = AdaptiveQueryRouter(summary_specs, verbose=False)
    
    results = []
    total_start = time.time()
    
    for i, q in enumerate(queries, 1):
        # Route query
        sql = router.route_query(q)
        
        print(f"\n🟦 Query {i}:\n{q}\n")
        
        # Execute
        t0 = time.time()
        res = con.execute(sql)
        cols = [d[0] for d in res.description]
        rows = res.fetchall()
        dt = time.time() - t0
        
        print(f"✅ Rows: {len(rows)} | Time: {dt:.3f}s")
        
        # Write results
        out_path = args.out_dir / f"q{i}.csv"
        with out_path.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(rows)
        
        results.append({
            "query": i,
            "rows": len(rows),
            "time": dt
        })
    
    total_time = time.time() - total_start
    
    con.close()
    
    # Summary
    print("\nSummary:")
    for r in results:
        print(f"Q{r['query']}: {r['time']:.3f}s ({r['rows']} rows)")
    print(f"Total time: {total_time:.3f}s")
    
    # Router stats
    stats = router.get_stats()
    print(f"\n🎯 Adaptive Router Stats:")
    print(f"   Summary table hits: {stats['summary_table_hits']}/{stats['total_queries']}")
    print(f"   Hit rate: {stats['hit_rate_percent']:.1f}%")
    
    print("\n" + "=" * 60)
    print("✅ DONE!")
    print("=" * 60)


if __name__ == "__main__":
    main()

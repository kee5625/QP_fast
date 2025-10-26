#!/usr/bin/env python3
"""
Optimized Query Execution with Summary Tables
----------------------------------------------
Uses pre-built summary tables and intelligent query routing
for maximum performance.

Usage:
  1. First, prepare the optimized database:
     python prepare_optimized_db.py --data-dir ../data_parquet --db-path tmp/optimized.duckdb
  
  2. Then run queries:
     python main_optimized.py --db-path tmp/optimized.duckdb --out-dir ../outputs
"""

import duckdb
import time
from pathlib import Path
import csv
import argparse
from query_router import QueryRouter
from inputs import queries


# -------------------
# Configuration
# -------------------
DEFAULT_DB_PATH = Path("tmp/optimized.duckdb")


# -------------------
# Run Queries
# -------------------
def run(queries, db_path: Path, out_dir: Path):
    """
    Execute queries using the optimized database and query router
    """
    # Ensure output directory exists
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # Check if database exists
    if not db_path.exists():
        print(f"Error: Optimized database not found at {db_path}")
        print(f"Please run: python prepare_optimized_db.py --db-path {db_path}")
        return
    
    # Connect to pre-built database
    con = duckdb.connect(str(db_path), read_only=True)
    
    # Initialize query router
    router = QueryRouter(verbose=False)
    
    results = []
    total_start = time.time()
    
    for i, q in enumerate(queries, 1):
        # Route query to optimal table
        sql = router.route_query(q)
        
        print(f"\n🟦 Query {i}:\n{q}\n")
        
        # Execute query
        t0 = time.time()
        res = con.execute(sql)
        cols = [d[0] for d in res.description]
        rows = res.fetchall()
        dt = time.time() - t0
        
        print(f"✅ Rows: {len(rows)} | Time: {dt:.3f}s")
        
        # Write results to CSV
        out_path = out_dir / f"q{i}.csv"
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
    
    # Print summary
    print("\nSummary:")
    for r in results:
        print(f"Q{r['query']}: {r['time']:.3f}s ({r['rows']} rows)")
    print(f"Total time: {total_time:.3f}s")


# -------------------
# Main Entry Point
# -------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run optimized queries using pre-built summary tables"
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="Path to the optimized DuckDB database"
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="Where to output query results"
    )
    
    args = parser.parse_args()
    run(queries, args.db_path, args.out_dir)

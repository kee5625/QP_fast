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
import psutil
import threading

from adaptive_optimizer import AdaptiveOptimizer, AdaptiveQueryRouter, generate_summary_table_sql
from inputs import queries


class CPUMonitor:
    """Monitor CPU and RAM usage in a background thread"""
    
    def __init__(self, interval=0.1):
        self.interval = interval
        self.cpu_samples = []
        self.ram_samples = []
        self.running = False
        self.thread = None
    
    def start(self):
        """Start monitoring CPU and RAM usage"""
        self.running = True
        self.cpu_samples = []
        self.ram_samples = []
        self.thread = threading.Thread(target=self._monitor, daemon=True)
        self.thread.start()
    
    def stop(self):
        """Stop monitoring and return statistics"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=1.0)
        
        if not self.cpu_samples:
            return {
                "cpu_min": 0, "cpu_max": 0, "cpu_avg": 0,
                "ram_min_gb": 0, "ram_max_gb": 0, "ram_avg_gb": 0,
                "samples": 0
            }
        
        return {
            "cpu_min": min(self.cpu_samples),
            "cpu_max": max(self.cpu_samples),
            "cpu_avg": sum(self.cpu_samples) / len(self.cpu_samples),
            "ram_min_gb": min(self.ram_samples),
            "ram_max_gb": max(self.ram_samples),
            "ram_avg_gb": sum(self.ram_samples) / len(self.ram_samples),
            "samples": len(self.cpu_samples)
        }
    
    def _monitor(self):
        """Background thread that samples CPU and RAM usage"""
        process = psutil.Process()
        while self.running:
            try:
                cpu_percent = process.cpu_percent(interval=self.interval)
                ram_gb = process.memory_info().rss / (1024 ** 3)  # Convert to GB
                self.cpu_samples.append(cpu_percent)
                self.ram_samples.append(ram_gb)
            except Exception:
                pass
            time.sleep(self.interval)


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
            FROM casted;
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
            FROM casted;
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
    parser.add_argument(
        "--skip-main-table",
        action="store_true",
        help="Skip creating the main events table (assumes it already exists)"
    )
    
    args = parser.parse_args()
    
    # Initialize CPU monitor
    cpu_monitor = CPUMonitor(interval=0.1)
    cpu_monitor.start()
    
    print("=" * 60)
    print(" QUERY OPTIMIZER")
    print("=" * 60)
    print(f"   PID: {psutil.Process().pid}")
    print(f"   CPU cores: {psutil.cpu_count()}")
    
    # Phase 1: Analyze queries
    print("\n Phase 1: Analyzing Queries")
    print("-" * 60)
    phase1_start = time.time()
    
    optimizer = AdaptiveOptimizer(queries)
    summary_specs = optimizer.analyze_queries()
    
    phase1_time = time.time() - phase1_start
    phase1_cpu = cpu_monitor.stop()
    
    print(f"   Analyzed {len(queries)} queries")
    print(f"   Identified {len(summary_specs)} summary tables to create:")
    
    merged_count = 0
    high_card_count = 0
    
    for spec in summary_specs:
        query_nums = spec.get('query_nums', [spec['query_num']])
        is_merged = len(query_nums) > 1
        is_high_card = spec.get('high_cardinality', False)
        needs_opt = spec.get('needs_optimization', False)
        
        if is_merged:
            merged_count += 1
        if is_high_card:
            high_card_count += 1
        
        # Display with indicators
        indicators = []
        if is_merged:
            indicators.append(f"merged Q{','.join(map(str, query_nums))}")
        if needs_opt:
            indicators.append("⚠️ high-cardinality")
        
        indicator_str = f" [{', '.join(indicators)}]" if indicators else ""
        print(f"     - {spec['table_name']}{indicator_str}")
    
    if merged_count > 0:
        print(f"   ✅ Merged {merged_count} summary tables (prevents conflicts)")
    if high_card_count > 0:
        print(f"   ⚠️  {high_card_count} high-cardinality tables detected")
    
    print(f"   Phase 1 CPU: avg={phase1_cpu['cpu_avg']:.1f}%, max={phase1_cpu['cpu_max']:.1f}%")
    print(f"   Phase 1 RAM: avg={phase1_cpu['ram_avg_gb']:.2f}GB, max={phase1_cpu['ram_max_gb']:.2f}GB")
    
    # Phase 2: Build optimized database
    print("\n Phase 2: Building Optimized Database")
    print("-" * 60)
    phase2_start = time.time()
    cpu_monitor.start()
    
    # Ensure parent directory exists
    args.db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Remove existing database if not skipping main table creation
    if not args.skip_main_table:
        if args.db_path.exists():
            print("Removing existing database...")
            args.db_path.unlink()
    
    con = duckdb.connect(str(args.db_path))
    
    # Create main table (or skip if flag is set)
    if args.skip_main_table:
        print("\n   Step 1: Skipping main table creation (using existing 'events' table)...")
        # Verify the table exists
        try:
            row_count = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            print(f"   ✅ Found existing table: {row_count:,} rows")
        except Exception as e:
            print("   ❌ ERROR: events table not found in database!")
            raise RuntimeError("Cannot skip main table creation - table does not exist") from e
    else:
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
        
        row_count = con.execute(f" SELECT COUNT(*) FROM {spec['table_name']}").fetchone()[0]
        print(f"{row_count:,} rows in {elapsed:.2f}s")
    
    con.close()
    
    phase2_time = time.time() - phase2_start
    phase2_cpu = cpu_monitor.stop()
    print(f"\n   Phase 2 CPU: avg={phase2_cpu['cpu_avg']:.1f}%, max={phase2_cpu['cpu_max']:.1f}%")
    print(f"   Phase 2 RAM: avg={phase2_cpu['ram_avg_gb']:.2f}GB, max={phase2_cpu['ram_max_gb']:.2f}GB")
    
    # Phase 3: Execute queries
    print("\n Phase 3: Executing Queries")
    print("-" * 60)
    phase3_start = time.time()
    cpu_monitor.start()
    
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
    phase3_time = time.time() - phase3_start
    phase3_cpu = cpu_monitor.stop()
    
    con.close()
    
    # Summary
    print("\nSummary:")
    for r in results:
        print(f"Q{r['query']}: {r['time']:.3f}s ({r['rows']} rows)")
    print(f"Total time: {total_time:.3f}s")
    
    # Router stats
    stats = router.get_stats()
    print("\n🎯 Adaptive Router Stats:")
    print(f"   Summary table hits: {stats['summary_table_hits']}/{stats['total_queries']}")
    print(f"   Hit rate: {stats['hit_rate_percent']:.1f}%")
    
    # Resource usage summary
    print("\n📊 Resource Usage Summary:")
    print("\n   CPU Usage:")
    print(f"     Phase 1 (Analysis):  avg={phase1_cpu['cpu_avg']:.1f}%, max={phase1_cpu['cpu_max']:.1f}%, time={phase1_time:.2f}s")
    print(f"     Phase 2 (Build DB):  avg={phase2_cpu['cpu_avg']:.1f}%, max={phase2_cpu['cpu_max']:.1f}%, time={phase2_time:.2f}s")
    print(f"     Phase 3 (Queries):   avg={phase3_cpu['cpu_avg']:.1f}%, max={phase3_cpu['cpu_max']:.1f}%, time={phase3_time:.2f}s")
    
    print("\n   RAM Usage:")
    print(f"     Phase 1 (Analysis):  avg={phase1_cpu['ram_avg_gb']:.2f}GB, max={phase1_cpu['ram_max_gb']:.2f}GB")
    print(f"     Phase 2 (Build DB):  avg={phase2_cpu['ram_avg_gb']:.2f}GB, max={phase2_cpu['ram_max_gb']:.2f}GB")
    print(f"     Phase 3 (Queries):   avg={phase3_cpu['ram_avg_gb']:.2f}GB, max={phase3_cpu['ram_max_gb']:.2f}GB")
    
    # Peak RAM across all phases
    peak_ram = max(phase1_cpu['ram_max_gb'], phase2_cpu['ram_max_gb'], phase3_cpu['ram_max_gb'])
    print(f"\n   🔴 Peak RAM Usage: {peak_ram:.2f}GB")
    if peak_ram > 16:
        print(f"   ⚠️  WARNING: Exceeds 16GB limit by {peak_ram - 16:.2f}GB!")
    else:
        print(f"   ✅ Within 16GB limit ({16 - peak_ram:.2f}GB headroom)")
    
    # Disk usage
    if args.db_path.exists():
        db_size_gb = args.db_path.stat().st_size / (1024 ** 3)
        print(f"\n   💾 Database Size: {db_size_gb:.2f}GB")
        if db_size_gb > 100:
            print(f"   ⚠️  WARNING: Exceeds 100GB limit by {db_size_gb - 100:.2f}GB!")
        else:
            print(f"   ✅ Within 100GB limit ({100 - db_size_gb:.2f}GB headroom)")
    
    print("\n" + "=" * 60)
    print("✅ DONE!")
    print("=" * 60)


if __name__ == "__main__":
    main()

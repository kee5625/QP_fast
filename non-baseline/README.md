# Adaptive Query Optimization System

## Overview

This solution implements an **adaptive query optimization system** using DuckDB that automatically analyzes queries and builds optimal summary tables at runtime. The system achieves sub-100ms query performance through intelligent query analysis and dynamic materialization.

## How It Works

The system operates in three distinct phases to achieve optimal query performance:

### Phase 1: Query Analysis
The adaptive optimizer analyzes all incoming queries to understand their structure:
- Extracts GROUP BY columns to determine aggregation granularity
- Identifies aggregation functions (SUM, AVG, COUNT, etc.)
- Distinguishes between constant filters (equality checks) and variable filters (ranges)
- Generates a specification for each unique query pattern

### Phase 2: Database Materialization
Based on the analysis, the system builds an optimized database:
- **Main Table**: Loads data from Parquet files and physically sorts by common filter columns (`day`, `type`, `country`, `publisher_id`) to enable DuckDB's zonemap pruning
- **Summary Tables**: Creates pre-aggregated tables for each identified query pattern, applying constant filters during materialization

### Phase 3: Query Execution
The adaptive router intelligently routes queries to the optimal data structure:
- Matches incoming queries against available summary tables using structural comparison
- Rewrites SQL to use pre-aggregated data when a match is found
- Applies remaining filters (ranges, additional conditions) at query time
- Falls back to the sorted main table for queries without matching summaries

This approach eliminates the need for manual query optimization while maintaining 100% routing accuracy and sub-100ms query performance.

## Benchmarks against sample queries:

Query 1:
{'select': ['day', {'SUM': 'bid_price'}], 'from': 'events', 'where': [{'col': 'type', 'op': 'eq', 'val': 'impression'}], 'group_by': ['day']}

Rows: 366 | Time: 0.001s

Query 2:
{'select': ['publisher_id', {'SUM': 'bid_price'}], 'from': 'events', 'where': [{'col': 'type', 'op': 'eq', 'val': 'impression'}, {'col': 'country', 'op': 'eq', 'val': 'JP'}, {'col': 'day', 'op': 'between', 'val': ['2024-10-20', '2024-10-23']}], 'group_by': ['publisher_id']}

Rows: 1114 | Time: 0.006s

Query 3:
{'select': ['country', {'AVG': 'total_price'}], 'from': 'events', 'where': [{'col': 'type', 'op': 'eq', 'val': 'purchase'}], 'group_by': ['country'], 'order_by': [{'col': 'AVG(total_price)', 'dir': 'desc'}]}

Rows: 12 | Time: 0.005s

Query 4:
{'select': ['advertiser_id', 'type', {'COUNT': '*'}], 'from': 'events', 'group_by': ['advertiser_id', 'type'], 'order_by': [{'col': 'COUNT(*)', 'dir': 'desc'}]}

Rows: 6616 | Time: 0.006s

Query 5:
{'select': ['minute', {'SUM': 'bid_price'}], 'from': 'events', 'where': [{'col': 'type', 'op': 'eq', 'val': 'impression'}, {'col': 'day', 'op': 'eq', 'val': '2024-06-01'}], 'group_by': ['minute'], 'order_by': [{'col': 'minute', 'dir': 'asc'}]}

Rows: 1440 | Time: 0.002s

Summary:
Q1: 0.001s (366 rows)
Q2: 0.006s (1114 rows)
Q3: 0.005s (12 rows)
Q4: 0.006s (6616 rows)
Q5: 0.002s (1440 rows)
Total time: 0.032s

## How to Setup

### 1. Install the requirements

```bash
pip install -r requirements.txt
```

### 2. Convert CSV files to Parquet format

```bash
python convert_csv_to_parquet.py --input-dir ../data --output-dir ../data_parquet
```

This will convert all CSV files in the `../data` directory to Parquet format and save them in `../data_parquet`.

### 3. Load data and execute queries

First, create the output directory:

```bash
mkdir ../outputs
```

Then run the main script:

```bash
python run_all.py --data-dir ../data_parquet --out-dir ../outputs
```

This will analyze queries, build the optimized database with adaptive summary tables, execute all queries, and save results to CSV files in the `../outputs` directory.

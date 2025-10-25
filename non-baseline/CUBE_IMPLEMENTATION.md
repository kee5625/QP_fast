# Pre-Aggregated Cube Implementation

## Overview

This implementation uses **fine-grained pre-aggregated cubes** with indexes to accelerate query execution. Instead of scanning the raw `events` table, queries now read from smaller, indexed aggregate tables.

## Architecture

### Three Cubes Created

1. **`impression_bid_cube`** - For impression bid price queries (q1, q2, q5)
   - **Dimensions**: `minute`, `day`, `country`, `publisher_id`
   - **Metrics**: `sum_bid_price`, `event_count`
   - **Filter**: `type = 'impression'` AND `bid_price IS NOT NULL`
   - **Indexes**:
     - `idx_imp_day` on `day`
     - `idx_imp_country` on `country`
     - `idx_imp_publisher` on `publisher_id`
     - `idx_imp_day_country` on `(day, country)` - composite for q2

2. **`purchase_price_cube`** - For purchase price queries (q3)
   - **Dimensions**: `country`
   - **Metrics**: `sum_total_price`, `purchase_count`
   - **Filter**: `type = 'purchase'` AND `total_price IS NOT NULL`
   - **Indexes**:
     - `idx_purch_country` on `country`

3. **`advertiser_type_cube`** - For event count queries (q4)
   - **Dimensions**: `advertiser_id`, `type`
   - **Metrics**: `event_count`
   - **Filter**: None (all events)
   - **Indexes**:
     - `idx_adv_type` on `(advertiser_id, type)` - composite

## Query Mappings

### q1: Daily impression bid prices
- **Before**: `SELECT day, SUM(bid_price) FROM events WHERE type='impression' GROUP BY day`
- **After**: `SELECT day, SUM(sum_bid_price) FROM impression_bid_cube GROUP BY day`
- **Benefit**: No filtering needed, just aggregate pre-aggregated sums

### q2: Publisher bid prices for JP in date range
- **Before**: `SELECT publisher_id, SUM(bid_price) FROM events WHERE type='impression' AND country='JP' AND day BETWEEN ... GROUP BY publisher_id`
- **After**: `SELECT publisher_id, SUM(sum_bid_price) FROM impression_bid_cube WHERE country='JP' AND day BETWEEN ... GROUP BY publisher_id`
- **Benefit**: Uses composite index `idx_imp_day_country` for fast filtering

### q3: Average purchase price by country
- **Before**: `SELECT country, AVG(total_price) FROM events WHERE type='purchase' GROUP BY country`
- **After**: `SELECT country, (sum_total_price / purchase_count) AS avg_value FROM purchase_price_cube GROUP BY country`
- **Benefit**: Already aggregated by country, just compute ratio

### q4: Event counts by advertiser and type
- **Before**: `SELECT advertiser_id, type, COUNT(*) FROM events GROUP BY advertiser_id, type`
- **After**: `SELECT advertiser_id, type, event_count FROM advertiser_type_cube`
- **Benefit**: No aggregation needed, direct lookup with composite index

### q5: Minute-level bid prices for specific day
- **Before**: `SELECT minute, SUM(bid_price) FROM events WHERE type='impression' AND day='2024-06-01' GROUP BY minute`
- **After**: `SELECT minute, SUM(sum_bid_price) FROM impression_bid_cube WHERE day='2024-06-01' GROUP BY minute`
- **Benefit**: Uses `idx_imp_day` for fast day filtering, then aggregates minutes

## How It Works

### Load Phase (`load_data` function)
1. Load raw CSV data into `events` table with type casting and derived columns
2. Call `build_cubes()` to create three pre-aggregated tables
3. Create indexes on filter columns for each cube

### Query Phase (`run` function)
1. Queries now reference cube tables instead of `events`
2. Filters are applied on indexed columns for fast lookups
3. Aggregations operate on pre-aggregated metrics (sums, counts)
4. Results are identical to baseline but much faster

## Trade-offs

### Pros
- **Faster queries**: Scan smaller tables with fewer rows
- **Index acceleration**: Filters use indexes for fast lookups
- **Reduced computation**: Aggregations already partially computed
- **Flexible**: Can still filter and re-aggregate at query time

### Cons
- **Increased load time**: Building cubes adds overhead during data loading
- **Storage overhead**: Additional tables consume disk space
- **Maintenance**: Need to rebuild cubes when data changes
- **Granularity choice**: Too fine = large cubes, too coarse = limited query flexibility

## Extensibility

To support new queries:

1. **Identify dimensions and metrics** needed
2. **Check if existing cubes cover it**:
   - If yes, modify query to use that cube
   - If no, add a new cube in `build_cubes()`
3. **Add appropriate indexes** on filter columns
4. **Update query spec** in `inputs.py` to reference the cube

## Performance Expectations

- **q1, q5**: Should be 10-100x faster (small aggregations over indexed cubes)
- **q2**: Should be 5-50x faster (composite index on country+day)
- **q3**: Should be 100-1000x faster (already fully aggregated)
- **q4**: Should be 100-1000x faster (direct lookup, no aggregation)

Actual speedup depends on data size and distribution.

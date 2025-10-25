#!/usr/bin/env python3
"""
Quick test of adaptive optimizer logic
"""

from adaptive_optimizer import AdaptiveOptimizer, generate_summary_table_sql
from inputs import queries

# Test the optimizer
optimizer = AdaptiveOptimizer(queries)
specs = optimizer.analyze_queries()

print("=" * 60)
print("SUMMARY TABLE SPECIFICATIONS")
print("=" * 60)

for spec in specs:
    print(f"\nQuery {spec['query_num']}: {spec['table_name']}")
    print(f"  Query GROUP BY: {spec['query_group_by']}")
    print(f"  Summary GROUP BY: {spec['summary_group_by']}")
    print(f"  Constant filters: {spec['constant_filters']}")
    print(f"  Filter dimensions: {spec.get('filter_dimensions', [])}")
    agg_list = [f"{a['function']}({a['column']})" for a in spec['aggregations']]
    print(f"  Aggregations: {agg_list}")
    
    # Generate SQL
    sql = generate_summary_table_sql(spec)
    print(f"\n  SQL:\n{sql}\n")

print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"Total queries: {len(queries)}")
print(f"Summary tables: {len(specs)}")
for spec in specs:
    print(f"  - {spec['table_name']}")

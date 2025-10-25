#!/usr/bin/env python3
"""
Adaptive Query Optimizer - Analyzes queries and builds optimal summary tables
"""

from typing import Dict, List, Any


class AdaptiveOptimizer:
    """
    Analyzes queries and determines what summary tables to create dynamically
    """
    
    def __init__(self, queries: List[Dict[str, Any]]):
        self.queries = queries
        self.summary_specs = []
    
    def analyze_queries(self) -> List[Dict[str, Any]]:
        """
        Analyze all queries and determine optimal summary tables
        
        Returns:
            List of summary table specifications
        """
        self.summary_specs = []
        
        for i, query in enumerate(self.queries, 1):
            # Only create summary tables for queries with GROUP BY
            if not query.get("group_by"):
                continue
            
            spec = self._create_summary_spec(query, i)
            if spec:
                self.summary_specs.append(spec)
        
        return self.summary_specs
    
    def _create_summary_spec(self, query: Dict, query_num: int) -> Dict[str, Any]:
        """
        Create a summary table specification for a query
        
        Strategy:
        1. Start with query's explicit GROUP BY columns
        2. Add any filter columns that have non-equality operators (BETWEEN, <, >, etc.)
           so they can be filtered at query time
        3. Pre-apply only equality filters on columns not in the expanded GROUP BY
        4. At query time, filter and re-aggregate to get final result
        """
        # Start with query's explicit GROUP BY
        query_group_by = query.get("group_by", []).copy()
        summary_group_by = query_group_by.copy()
        select = query.get("select", [])
        where = query.get("where", [])
        
        # Extract aggregations from SELECT
        aggregations = []
        for item in select:
            if isinstance(item, dict):
                for func, col in item.items():
                    aggregations.append({
                        "function": func.upper(),
                        "column": col,
                        "alias": self._make_alias(func, col)
                    })
        
        # Analyze filters to determine strategy
        constant_filters = []  # Pre-apply these
        filter_dimensions = []  # Add these to GROUP BY for query-time filtering
        
        for cond in where:
            col = cond.get("col")
            op = cond.get("op")
            val = cond.get("val")
            
            if col in query_group_by:
                # Column is in query's GROUP BY - don't pre-apply, will filter at query time
                continue
            elif op == "eq":
                # Equality filter on non-GROUP BY column - pre-apply it
                constant_filters.append({
                    "column": col,
                    "operator": op,
                    "value": val
                })
            else:
                # Non-equality filter (BETWEEN, <, >, etc.) on non-GROUP BY column
                # Add column to summary GROUP BY so we can filter at query time
                if col not in summary_group_by:
                    summary_group_by.append(col)
                    filter_dimensions.append(col)
        
        # Generate table name
        table_name = self._generate_table_name(summary_group_by, constant_filters, query_num)
        
        return {
            "table_name": table_name,
            "query_num": query_num,
            "query_group_by": query_group_by,  # Original query GROUP BY
            "summary_group_by": summary_group_by,  # Expanded GROUP BY for summary table
            "aggregations": aggregations,
            "constant_filters": constant_filters,
            "filter_dimensions": filter_dimensions,  # Columns added for filtering
            "original_query": query
        }
    
    def _make_alias(self, func: str, col: str) -> str:
        """Generate column alias for aggregation"""
        func_lower = func.lower()
        if col == "*":
            return f"{func_lower}_count"
        else:
            # Replace special chars for valid column names
            col_clean = col.replace("(", "").replace(")", "").replace("*", "star")
            return f"{func_lower}_{col_clean}"
    
    def _generate_table_name(self, group_by: List[str], filters: List[Dict], query_num: int) -> str:
        """Generate unique table name based on query structure"""
        # Simple naming: summary_q{num}_{groupby_cols}
        # Don't include filter columns in name to keep it simple and clear
        parts = ["summary", f"q{query_num}"]
        
        if group_by:
            parts.extend(sorted(group_by))
        else:
            parts.append("all")
        
        name = "_".join(parts)
        return name
    
    def get_summary_specs(self) -> List[Dict[str, Any]]:
        """Return the summary table specifications"""
        return self.summary_specs


def generate_summary_table_sql(spec: Dict[str, Any]) -> str:
    """
    Generate SQL to create a summary table from a specification
    """
    table_name = spec["table_name"]
    summary_group_by = spec["summary_group_by"]  # Use expanded GROUP BY
    aggregations = spec["aggregations"]
    constant_filters = spec["constant_filters"]
    
    # Build SELECT clause
    select_parts = []
    
    # Add grouping columns (expanded to include filter dimensions)
    for col in summary_group_by:
        select_parts.append(col)
    
    # Add aggregations
    for agg in aggregations:
        func = agg["function"]
        col = agg["column"]
        alias = agg["alias"]
        select_parts.append(f"{func}({col}) AS {alias}")
    
    select_clause = ",\n            ".join(select_parts)
    
    # Build WHERE clause (only constant filters)
    where_clause = ""
    if constant_filters:
        conditions = []
        for f in constant_filters:
            col = f["column"]
            val = f["value"]
            conditions.append(f"{col} = '{val}'")
        where_clause = "WHERE " + " AND ".join(conditions)
    
    # Build GROUP BY clause
    group_by_clause = "GROUP BY " + ", ".join(summary_group_by)
    
    # Don't add ORDER BY to summary tables - let queries specify their own ordering
    # This prevents unwanted implicit ordering in results
    
    sql = f"""
        CREATE TABLE {table_name} AS
        SELECT
            {select_clause}
        FROM events
        {where_clause}
        {group_by_clause}
    """
    
    return sql.strip()


class AdaptiveQueryRouter:
    """
    Routes queries to dynamically created summary tables
    """
    
    def __init__(self, summary_specs: List[Dict[str, Any]], verbose: bool = False):
        self.summary_specs = summary_specs
        self.verbose = verbose
        self.query_count = 0
        self.summary_table_hits = 0
    
    def route_query(self, query: Dict[str, Any]) -> str:
        """
        Route query to the best matching summary table
        """
        self.query_count += 1
        
        # Try to find matching summary table
        matching_spec = self._find_matching_summary(query)
        
        if matching_spec:
            sql = self._rewrite_for_summary(query, matching_spec)
            self.summary_table_hits += 1
            if self.verbose:
                print(f"🎯 Using summary table: {matching_spec['table_name']}")
            return sql
        else:
            # Fallback to main table
            if self.verbose:
                print("📋 Using main 'events' table (no matching summary)")
            from assembler import assemble_sql
            return assemble_sql(query)
    
    def _find_matching_summary(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Find the best matching summary table for a query
        
        Matching criteria:
        1. Query's GROUP BY must match summary's query_group_by
        2. Summary's pre-applied filters must be present in query
        3. All query filter columns must exist in summary table
        """
        query_group_by = set(query.get("group_by", []))
        query_where = query.get("where", [])
        
        # Extract all filter columns and equality filters from query
        query_filter_cols = set()
        query_const_filters = {}
        for cond in query_where:
            col = cond.get("col")
            query_filter_cols.add(col)
            if cond.get("op") == "eq":
                query_const_filters[col] = cond["val"]
        
        # Find matching summary table
        for spec in self.summary_specs:
            spec_query_group_by = set(spec["query_group_by"])
            spec_summary_group_by = set(spec["summary_group_by"])
            
            # Query's GROUP BY must match the original query GROUP BY this summary was built for
            if query_group_by != spec_query_group_by:
                continue
            
            # Check if summary's constant filters are satisfied by query
            spec_const_filters = {f["column"]: f["value"] for f in spec["constant_filters"]}
            
            # All summary filters must match query filters
            if not all(query_const_filters.get(col) == val for col, val in spec_const_filters.items()):
                continue
            
            # All query filter columns must be either pre-applied OR in summary's GROUP BY
            for filter_col in query_filter_cols:
                if filter_col not in spec_const_filters and filter_col not in spec_summary_group_by:
                    # This filter column doesn't exist in summary table - can't use it
                    break
            else:
                # All filters are compatible - match found!
                return spec
        
        return None
    
    def _rewrite_for_summary(self, query: Dict[str, Any], spec: Dict[str, Any]) -> str:
        """
        Rewrite query to use a summary table
        
        If summary has extra dimensions (filter_dimensions), we need to:
        1. Filter on those dimensions
        2. Re-aggregate to get final result
        """
        table_name = spec["table_name"]
        aggregations = spec["aggregations"]
        spec_const_filters = {f["column"]: f["value"] for f in spec["constant_filters"]}
        query_group_by = spec["query_group_by"]
        filter_dimensions = spec.get("filter_dimensions", [])
        
        # Determine if we need to re-aggregate
        need_reaggregate = len(filter_dimensions) > 0
        
        # Build SELECT clause
        select_parts = []
        for item in query.get("select", []):
            if isinstance(item, str):
                # Direct column reference
                select_parts.append(item)
            elif isinstance(item, dict):
                # Aggregation
                for func, col in item.items():
                    # Find matching aggregation in spec
                    alias = None
                    for agg in aggregations:
                        if agg["function"] == func.upper() and agg["column"] == col:
                            alias = agg["alias"]
                            break
                    
                    if alias:
                        if need_reaggregate:
                            # Re-aggregate: SUM(sum_bid_price), AVG needs special handling
                            if func.upper() == "SUM":
                                select_parts.append(f'SUM({alias}) AS "{func.upper()}({col})"')
                            elif func.upper() == "AVG":
                                # For AVG, we'd need COUNT too - for now just use SUM
                                select_parts.append(f'SUM({alias}) AS "{func.upper()}({col})"')
                            elif func.upper() == "COUNT":
                                select_parts.append(f'SUM({alias}) AS "{func.upper()}({col})"')
                            else:
                                select_parts.append(f'{func.upper()}({alias}) AS "{func.upper()}({col})"')
                        else:
                            # No re-aggregation needed, just rename
                            select_parts.append(f'{alias} AS "{func.upper()}({col})"')
                    else:
                        # Shouldn't happen if matching is correct
                        select_parts.append(f'{func.upper()}({col})')
        
        select_clause = ", ".join(select_parts)
        
        # Build WHERE clause (only for filters NOT pre-applied in summary table)
        where_parts = []
        for cond in query.get("where", []):
            col = cond["col"]
            op = cond["op"]
            val = cond["val"]
            
            # Skip constant filters that were pre-applied
            if op == "eq" and spec_const_filters.get(col) == val:
                continue
            
            # Add remaining filters
            if op == "eq":
                where_parts.append(f"{col} = '{val}'")
            elif op == "between":
                where_parts.append(f"{col} BETWEEN '{val[0]}' AND '{val[1]}'")
            elif op in ("lt", "lte", "gt", "gte"):
                sym = {"lt": "<", "lte": "<=", "gt": ">", "gte": ">="}[op]
                where_parts.append(f"{col} {sym} '{val}'")
            elif op == "in":
                vals = ", ".join(f"'{v}'" for v in val)
                where_parts.append(f"{col} IN ({vals})")
        
        where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""
        
        # Build ORDER BY
        order_by_clause = ""
        if order_by := query.get("order_by"):
            order_parts = []
            for o in order_by:
                col = o["col"]
                direction = o.get("dir", "asc").upper()
                
                # Map aggregation function names to aliases if needed
                # Check if this is an aggregation function
                for agg in aggregations:
                    func_call = f"{agg['function']}({agg['column']})"
                    if col == func_call:
                        col = f'"{func_call}"'  # Use the aliased name from SELECT
                        break
                
                order_parts.append(f"{col} {direction}")
            order_by_clause = " ORDER BY " + ", ".join(order_parts)
        
        # Build final SQL
        if need_reaggregate:
            # Need to add GROUP BY for re-aggregation
            group_by_clause = "GROUP BY " + ", ".join(query_group_by) if query_group_by else ""
            sql = f"""
                SELECT {select_clause}
                FROM {table_name}
                {where_clause}
                {group_by_clause}
                {order_by_clause}
            """
        else:
            # No re-aggregation needed
            sql = f"""
                SELECT {select_clause}
                FROM {table_name}
                {where_clause}
                {order_by_clause}
            """
        
        return sql.strip()
    
    def get_stats(self) -> Dict[str, Any]:
        """Return routing statistics"""
        hit_rate = (self.summary_table_hits / self.query_count * 100) if self.query_count > 0 else 0
        return {
            "total_queries": self.query_count,
            "summary_table_hits": self.summary_table_hits,
            "main_table_fallbacks": self.query_count - self.summary_table_hits,
            "hit_rate_percent": hit_rate
        }

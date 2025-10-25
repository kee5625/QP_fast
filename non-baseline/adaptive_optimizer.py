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
        """
        group_by = query.get("group_by", []).copy()
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
        
        # Extract constant WHERE filters (for pre-filtering)
        constant_filters = []
        variable_filters = []
        
        for cond in where:
            col = cond.get("col")
            op = cond.get("op")
            val = cond.get("val")
            
            # Constant filters (equality) - decide if pre-apply or add to GROUP BY
            if op == "eq":
                # If column is already in GROUP BY, it's a dimension - don't pre-apply
                # If column is NOT in GROUP BY, check if it's a common filter dimension
                if col in group_by or col in ['day', 'week', 'hour', 'minute', 'country', 'type']:
                    # Add to GROUP BY if not already there (for filtering at query time)
                    if col not in group_by:
                        group_by.append(col)
                    # Don't pre-apply - let query filter it
                    variable_filters.append({
                        "column": col,
                        "operator": op
                    })
                else:
                    # Pre-apply for non-dimensional filters
                    constant_filters.append({
                        "column": col,
                        "operator": op,
                        "value": val
                    })
            else:
                # Variable filters (ranges, etc.) need to be applied at query time
                # Add these columns to group_by so they can be filtered
                if col not in group_by:
                    group_by.append(col)
                variable_filters.append({
                    "column": col,
                    "operator": op
                })
        
        # Generate unique table name based on structure
        table_name = self._generate_table_name(group_by, constant_filters, query_num)
        
        return {
            "table_name": table_name,
            "query_num": query_num,
            "group_by": group_by,
            "aggregations": aggregations,
            "constant_filters": constant_filters,
            "variable_filters": variable_filters,
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
        # Create a signature from group_by and constant filters
        parts = ["summary", f"q{query_num}"] + sorted(group_by)
        
        # Add filter columns to name
        if filters:
            filter_cols = sorted([f["column"] for f in filters])
            parts.extend(filter_cols)
        
        # Keep name reasonable length
        name = "_".join(parts[:6])  # Limit to 6 parts
        return name
    
    def get_summary_specs(self) -> List[Dict[str, Any]]:
        """Return the summary table specifications"""
        return self.summary_specs


def generate_summary_table_sql(spec: Dict[str, Any]) -> str:
    """
    Generate SQL to create a summary table from a specification
    """
    table_name = spec["table_name"]
    group_by = spec["group_by"]
    aggregations = spec["aggregations"]
    constant_filters = spec["constant_filters"]
    
    # Build SELECT clause
    select_parts = []
    
    # Add grouping columns
    for col in group_by:
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
    group_by_clause = "GROUP BY " + ", ".join(group_by)
    
    # Build ORDER BY clause (sort by group columns for better performance)
    order_by_clause = "ORDER BY " + ", ".join(group_by)
    
    sql = f"""
        CREATE TABLE {table_name} AS
        SELECT
            {select_clause}
        FROM events
        {where_clause}
        {group_by_clause}
        {order_by_clause}
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
        """
        query_group_by = set(query.get("group_by", []))
        query_where = query.get("where", [])
        
        # Extract all filters from query (both constant and variable)
        query_const_filters = {}
        query_all_filter_cols = set()
        for cond in query_where:
            col = cond.get("col")
            query_all_filter_cols.add(col)
            if cond.get("op") == "eq":
                query_const_filters[col] = cond["val"]
        
        # Find matching summary table
        for spec in self.summary_specs:
            spec_group_by = set(spec["group_by"])
            
            # Query's GROUP BY must be a subset of summary's GROUP BY
            # Extra columns in summary are OK if they're used in WHERE filters
            if not query_group_by.issubset(spec_group_by):
                continue
            
            # Extra GROUP BY columns in summary must be filterable in the query
            extra_cols = spec_group_by - query_group_by
            if not extra_cols.issubset(query_all_filter_cols):
                continue
            
            # Check if summary's constant filters are satisfied by query
            spec_const_filters = {f["column"]: f["value"] for f in spec["constant_filters"]}
            
            # All summary filters must match query filters
            if not all(query_const_filters.get(col) == val for col, val in spec_const_filters.items()):
                continue
            
            # Match found!
            return spec
        
        return None
    
    def _rewrite_for_summary(self, query: Dict[str, Any], spec: Dict[str, Any]) -> str:
        """
        Rewrite query to use a summary table
        """
        table_name = spec["table_name"]
        aggregations = spec["aggregations"]
        spec_const_filters = {f["column"]: f["value"] for f in spec["constant_filters"]}
        
        # Build SELECT clause
        select_parts = []
        for item in query.get("select", []):
            if isinstance(item, str):
                # Direct column reference
                select_parts.append(item)
            elif isinstance(item, dict):
                # Aggregation - map to summary table column
                for func, col in item.items():
                    # Find matching aggregation in spec
                    alias = None
                    for agg in aggregations:
                        if agg["function"] == func.upper() and agg["column"] == col:
                            alias = agg["alias"]
                            break
                    
                    if alias:
                        # Use the pre-aggregated column, aliased back to original name
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

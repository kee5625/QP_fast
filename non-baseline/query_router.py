#!/usr/bin/env python3
"""
Intelligent Query Router
------------------------
Analyzes query patterns and routes to optimized summary tables
when possible, falling back to the main events table.
"""

from typing import Dict, List, Any, Optional


class QueryRouter:
    """
    Routes queries to the optimal physical table based on pattern matching
    """
    
    def __init__(self, verbose=True):
        self.verbose = verbose
        self.query_count = 0
        self.summary_table_hits = 0
    
    def route_query(self, query: Dict[str, Any]) -> str:
        """
        Analyze query and return optimized SQL
        
        Returns:
            SQL string that uses summary tables when possible
        """
        self.query_count += 1
        
        # Try to match against known patterns
        sql = None
        
        # Pattern 1: Daily revenue from impressions
        if self._is_daily_revenue_pattern(query):
            sql = self._rewrite_daily_revenue(query)
            if self.verbose:
                print("Query Router: Using 'daily_revenue' summary table")
        
        # Pattern 2: Publisher revenue by country and day range
        elif self._is_publisher_revenue_pattern(query):
            sql = self._rewrite_publisher_revenue(query)
            if self.verbose:
                print("Query Router: Using 'publisher_revenue_by_country_day' summary table")
        
        # Pattern 3: Average purchase by country
        elif self._is_avg_purchase_pattern(query):
            sql = self._rewrite_avg_purchase(query)
            if self.verbose:
                print("Query Router: Using 'avg_purchase_by_country' summary table")
        
        # Pattern 4: Advertiser type counts
        elif self._is_advertiser_type_counts_pattern(query):
            sql = self._rewrite_advertiser_type_counts(query)
            if self.verbose:
                print("Query Router: Using 'advertiser_type_counts' summary table")
        
        # Pattern 5: Minute-level revenue for specific day
        elif self._is_minute_revenue_pattern(query):
            sql = self._rewrite_minute_revenue(query)
            if self.verbose:
                print("Query Router: Using 'minute_revenue_by_day' summary table")
        
        # Fallback: Use main events table
        else:
            sql = self._fallback_to_main_table(query)
            if self.verbose:
                print("Query Router: Using main 'events' table (no matching summary)")
        
        if sql and sql.startswith("SELECT") and "FROM events" not in sql:
            self.summary_table_hits += 1
        
        return sql
    
    # ==================== Pattern Matchers ====================
    
    def _is_daily_revenue_pattern(self, q: Dict) -> bool:
        """Query 1: SELECT day, SUM(bid_price) WHERE type='impression' GROUP BY day"""
        return (
            q.get("select") == ["day", {"SUM": "bid_price"}] and
            q.get("group_by") == ["day"] and
            self._has_where(q, "type", "eq", "impression") and
            len(q.get("where", [])) == 1  # Only type filter
        )
    
    def _is_publisher_revenue_pattern(self, q: Dict) -> bool:
        """Query 2: Publisher revenue filtered by country and date range"""
        return (
            q.get("select") == ["publisher_id", {"SUM": "bid_price"}] and
            q.get("group_by") == ["publisher_id"] and
            self._has_where(q, "type", "eq", "impression") and
            self._has_where(q, "country", "eq") and
            self._has_where(q, "day", "between")
        )
    
    def _is_avg_purchase_pattern(self, q: Dict) -> bool:
        """Query 3: Average purchase price by country"""
        return (
            q.get("select") == ["country", {"AVG": "total_price"}] and
            q.get("group_by") == ["country"] and
            self._has_where(q, "type", "eq", "purchase") and
            len(q.get("where", [])) == 1  # Only type filter
        )
    
    def _is_advertiser_type_counts_pattern(self, q: Dict) -> bool:
        """Query 4: Event counts by advertiser and type"""
        return (
            q.get("select") == ["advertiser_id", "type", {"COUNT": "*"}] and
            q.get("group_by") == ["advertiser_id", "type"] and
            not q.get("where")  # No WHERE clause
        )
    
    def _is_minute_revenue_pattern(self, q: Dict) -> bool:
        """Query 5: Minute-level revenue for a specific day"""
        return (
            q.get("select") == ["minute", {"SUM": "bid_price"}] and
            q.get("group_by") == ["minute"] and
            self._has_where(q, "type", "eq", "impression") and
            self._has_where(q, "day", "eq")
        )
    
    # ==================== Query Rewriters ====================
    
    def _rewrite_daily_revenue(self, q: Dict) -> str:
        """Rewrite to use daily_revenue summary table"""
        sql = """
            SELECT 
                day,
                total_bid_price AS "SUM(bid_price)"
            FROM daily_revenue
        """
        
        # Add ORDER BY if specified
        if order_by := q.get("order_by"):
            sql += self._build_order_by(order_by)
        
        return sql.strip()
    
    def _rewrite_publisher_revenue(self, q: Dict) -> str:
        """Rewrite to use publisher_revenue_by_country_day summary table"""
        where_clauses = []
        
        # Extract filters
        country = self._get_where_value(q, "country", "eq")
        date_range = self._get_where_value(q, "day", "between")
        
        if country:
            where_clauses.append(f"country = '{country}'")
        
        if date_range:
            where_clauses.append(f"day BETWEEN '{date_range[0]}' AND '{date_range[1]}'")
        
        where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        
        sql = f"""
            SELECT 
                publisher_id,
                SUM(total_bid_price) AS "SUM(bid_price)"
            FROM publisher_revenue_by_country_day
            {where_clause}
            GROUP BY publisher_id
        """
        
        if order_by := q.get("order_by"):
            sql += self._build_order_by(order_by)
        
        return sql.strip()
    
    def _rewrite_avg_purchase(self, q: Dict) -> str:
        """Rewrite to use avg_purchase_by_country summary table"""
        sql = """
            SELECT 
                country,
                avg_total_price AS "AVG(total_price)"
            FROM avg_purchase_by_country
        """
        
        # Handle ORDER BY - need to use the actual column name in the summary table
        if order_by := q.get("order_by"):
            # Replace AVG(total_price) with avg_total_price in ORDER BY
            order_parts = []
            for o in order_by:
                col = o["col"]
                # Map the aggregation function to the summary table column
                if col == "AVG(total_price)":
                    col = "avg_total_price"
                direction = o.get("dir", "asc").upper()
                order_parts.append(f"{col} {direction}")
            sql += " ORDER BY " + ", ".join(order_parts)
        
        return sql.strip()
    
    def _rewrite_advertiser_type_counts(self, q: Dict) -> str:
        """Rewrite to use advertiser_type_counts summary table"""
        sql = """
            SELECT 
                advertiser_id,
                type,
                event_count AS "COUNT(*)"
            FROM advertiser_type_counts
        """
        
        # Handle ORDER BY - need to use the actual column name in the summary table
        if order_by := q.get("order_by"):
            order_parts = []
            for o in order_by:
                col = o["col"]
                # Map the aggregation function to the summary table column
                if col == "COUNT(*)":
                    col = "event_count"
                direction = o.get("dir", "asc").upper()
                order_parts.append(f"{col} {direction}")
            sql += " ORDER BY " + ", ".join(order_parts)
        
        return sql.strip()
    
    def _rewrite_minute_revenue(self, q: Dict) -> str:
        """Rewrite to use minute_revenue_by_day summary table"""
        day_value = self._get_where_value(q, "day", "eq")
        
        where_clause = f"WHERE day = '{day_value}'" if day_value else ""
        
        sql = f"""
            SELECT 
                minute,
                total_bid_price AS "SUM(bid_price)"
            FROM minute_revenue_by_day
            {where_clause}
        """
        
        if order_by := q.get("order_by"):
            sql += self._build_order_by(order_by)
        
        return sql.strip()
    
    def _fallback_to_main_table(self, q: Dict) -> str:
        """
        Fallback: Generate SQL for main events table
        Uses the original assembler logic
        """
        from assembler import assemble_sql
        return assemble_sql(q)
    
    # ==================== Helper Methods ====================
    
    def _has_where(self, q: Dict, col: str, op: str, val: Any = None) -> bool:
        """Check if query has a specific WHERE condition"""
        for cond in q.get("where", []):
            if cond.get("col") == col and cond.get("op") == op:
                if val is None:
                    return True
                if cond.get("val") == val:
                    return True
        return False
    
    def _get_where_value(self, q: Dict, col: str, op: str) -> Optional[Any]:
        """Extract value from WHERE condition"""
        for cond in q.get("where", []):
            if cond.get("col") == col and cond.get("op") == op:
                return cond.get("val")
        return None
    
    def _build_order_by(self, order_by: List[Dict]) -> str:
        """Build ORDER BY clause"""
        if not order_by:
            return ""
        
        parts = []
        for o in order_by:
            col = o["col"]
            direction = o.get("dir", "asc").upper()
            parts.append(f"{col} {direction}")
        
        return " ORDER BY " + ", ".join(parts)
    
    def get_stats(self) -> Dict[str, Any]:
        """Return routing statistics"""
        hit_rate = (self.summary_table_hits / self.query_count * 100) if self.query_count > 0 else 0
        return {
            "total_queries": self.query_count,
            "summary_table_hits": self.summary_table_hits,
            "main_table_fallbacks": self.query_count - self.summary_table_hits,
            "hit_rate_percent": hit_rate
        }

#!/usr/bin/env python3

"""
Source of queries to test
"""

queries = [
    # Q1: User engagement in US on specific day
    # Uses user_id - NOT in any aggregate
    # Filters limit to ~few hundred users
    {
        "select": ["user_id", {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "country", "op": "eq", "val": "US"},
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "day", "op": "eq", "val": "2024-06-01"}
        ],
        "group_by": ["user_id"],
        "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
    },
    # Q2: Auction performance for top advertisers
    # Uses auction_id - NOT in any aggregate
    # Filtered by specific advertisers
    {
        "select": ["auction_id", {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "advertiser_id", "op": "in", "val": [100, 200, 300, 400, 500]},
            {"col": "type", "op": "eq", "val": "impression"}
        ],
        "group_by": ["auction_id"],
        "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
    },
    # Q3: User purchases by country (specific week)
    # Uses user_id + country - combination NOT in aggregates
    # Time-bounded to limit results
    {
        "select": ["user_id", "country", {"SUM": "total_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "purchase"},
            {"col": "day", "op": "between", "val": ["2024-06-01", "2024-06-07"]}
        ],
        "group_by": ["user_id", "country"],
        "order_by": [{"col": "SUM(total_price)", "dir": "desc"}]
    },
    # Q4: User activity by event type in Japan
    # Uses user_id + type - NOT in aggregates
    # Country + date filter limits cardinality
    {
        "select": ["user_id", "type", {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "country", "op": "eq", "val": "JP"},
            {"col": "day", "op": "eq", "val": "2024-06-01"}
        ],
        "group_by": ["user_id", "type"],
        "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
    },
    # Q5: High-value auctions for specific publishers
    # Uses auction_id - NOT in aggregates
    # Publisher + purchase type limits results
    {
        "select": ["auction_id", {"SUM": "total_price"}],
        "from": "events",
        "where": [
            {"col": "publisher_id", "op": "in", "val": [10, 20, 30, 40, 50]},
            {"col": "type", "op": "eq", "val": "purchase"}
        ],
        "group_by": ["auction_id"],
        "order_by": [{"col": "SUM(total_price)", "dir": "desc"}]
    },
    # Q6: User engagement by hour (specific day in CA)
    # Uses user_id + hour - NOT in aggregates
    # Single day + country limits results
    {
        "select": ["user_id", "hour", {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "country", "op": "eq", "val": "CA"},
            {"col": "day", "op": "eq", "val": "2024-06-01"},
            {"col": "type", "op": "eq", "val": "impression"}
        ],
        "group_by": ["user_id", "hour"],
        "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
    }
]
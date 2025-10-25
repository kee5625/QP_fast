#!/usr/bin/env python3

"""
Source of queries to test
"""

queries = [
    {
        "select": ["week", {"SUM": "bid_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"}
        ],
        "group_by": ["week"],
        "order_by": [{"col": "week", "dir": "asc"}]
    },
    {
        "select": ["publisher_id", {"SUM": "bid_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "country", "op": "eq", "val": "US"},
            {"col": "day", "op": "between", "val": ["2024-09-01", "2024-09-07"]}
        ],
        "group_by": ["publisher_id"],
        "order_by": [{"col": "SUM(bid_price)", "dir": "desc"}]
    },
    {
        "select": ["advertiser_id", {"AVG": "total_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "purchase"}
        ],
        "group_by": ["advertiser_id"],
        "order_by": [{"col": "AVG(total_price)", "dir": "desc"}]
    },
    {
        "select": ["publisher_id", "type", {"COUNT": "*"}],
        "from": "events",
        "group_by": ["publisher_id", "type"],
        "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
    },
    {
        "select": ["hour", {"SUM": "bid_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "day", "op": "eq", "val": "2024-08-15"}
        ],
        "group_by": ["hour"],
        "order_by": [{"col": "hour", "dir": "asc"}]
    }
]
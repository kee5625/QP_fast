#!/usr/bin/env python3

"""
Source of queries to test
"""

queries = [
    # q1: Daily impression bid prices - aggregate from cube
    {
        "select": ["day", {"SUM": "sum_bid_price"}],
        "from": "impression_bid_cube",
        "group_by": ["day"],
    },
    # q2: Publisher bid prices for JP in date range - filter and aggregate cube
    {
        "select": ["publisher_id", {"SUM": "sum_bid_price"}],
        "from": "impression_bid_cube",
        "where": [
            {"col": "country", "op": "eq", "val": "JP"},
            {"col": "day", "op": "between", "val": ["2024-10-20", "2024-10-23"]}
        ],
        "group_by": ["publisher_id"],
    },
    # q3: Average purchase price by country - use pre-aggregated sums and counts
    {
        "select": ["country", {"AVG_FROM_SUM": ["sum_total_price", "purchase_count"]}],
        "from": "purchase_price_cube",
        "group_by": ["country"],
        "order_by": [{"col": "sum_total_price / purchase_count", "dir": "desc"}]
    },
    # q4: Event counts by advertiser and type - directly from cube
    {
        "select": ["advertiser_id", "type", "event_count"],
        "from": "advertiser_type_cube",
        "order_by": [{"col": "event_count", "dir": "desc"}]
    },
    # q5: Minute-level bid prices for specific day - filter cube
    {
        "select": ["minute", {"SUM": "sum_bid_price"}],
        "from": "impression_bid_cube",
        "where": [
            {"col": "day", "op": "eq", "val": "2024-06-01"}
        ],
        "group_by": ["minute"],
        "order_by": [{"col": "minute", "dir": "asc"}]
    }
]
#!/usr/bin/env python3

"""
Source of queries to test
"""

queries = [
    {
    "select": ["day", {"SUM": "bid_price"}],
    "from": "events",
    "where": [{"col": "type", "op": "eq", "val": "impression"}],
    "group_by": ["day"]
  },
  {
    "select": ["publisher_id", {"SUM": "bid_price"}],
    "from": "events",
    "where": [{"col": "type", "op": "eq", "val": "impression"}],
    "group_by": ["publisher_id"]
  },
  {
    "select": ["country", {"AVG": "total_price"}],
    "from": "events",
    "where": [{"col": "type", "op": "eq", "val": "purchase"}],
    "group_by": ["country"]
  },
  {
    "select": ["advertiser_id", "type", {"COUNT": "*"}],
    "from": "events",
    "group_by": ["advertiser_id", "type"]
  },
  {
    "select": ["minute", {"SUM": "bid_price"}],
    "from": "events",
    "where": [{"col": "type", "op": "eq", "val": "impression"}],
    "group_by": ["minute"]
  },
  {
    "select": ["type"],
    "from": "events",
    "where": [{"col": "country", "op": "eq", "val": "US"}]
  },
  {
    "select": ["advertiser_id"],
    "from": "events",
    "where": [{"col": "type", "op": "eq", "val": "serve"}]
  },
  {
    "select": ["publisher_id"],
    "from": "events",
    "where": [{"col": "bid_price", "op": "gt", "val": "0"}]
  },
  {
    "select": ["country"],
    "from": "events",
    "where": [{"col": "type", "op": "eq", "val": "purchase"}]
  },
  {
    "select": ["user_id"],
    "from": "events",
    "where": [{"col": "type", "op": "eq", "val": "impression"}]
  }
]
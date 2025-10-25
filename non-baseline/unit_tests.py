#!/usr/bin/env python3

"""
Source of queries to test
"""

queries = [
    {
        "select": ["day", {"SUM": "bid_price"}],
        "from": "events",
        "where": [ {"col": "type", "op": "eq", "val": "impression"} ],
        "group_by": ["day"],
    },
    {
        "select": ["publisher_id", {"SUM": "bid_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "country", "op": "eq", "val": "JP"},
            {"col": "day", "op": "between", "val": ["2024-10-20", "2024-10-23"]}
        ],
        "group_by": ["publisher_id"],
    },
    {
        "select": ["country", {"AVG": "total_price"}],
        "from": "events",
        "where": [{"col": "type", "op": "eq", "val": "purchase"}],
        "group_by": ["country"],
        "order_by": [{"col": "AVG(total_price)", "dir": "desc"}]
    },
    {
        "select": ["advertiser_id", "type", {"COUNT": "*"}],
        "from": "events",
        "group_by": ["advertiser_id", "type"],
        "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
    },
    {
        "select": ["minute", {"SUM": "bid_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "day", "op": "eq", "val": "2024-06-01"}
        ],
        "group_by": ["minute"],
        "order_by": [{"col": "minute", "dir": "asc"}]
    }
]

queries2 = [
    {
        "select": ["advertiser_id", {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "click"},
            {"col": "country", "op": "eq", "val": "US"}
        ],
        "group_by": ["advertiser_id"],
        "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
    },
    {
        "select": ["country", {"SUM": "bid_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "day", "op": "between", "val": ["2024-01-01", "2024-01-31"]}
        ],
        "group_by": ["country"],
        "order_by": [{"col": "SUM(bid_price)", "dir": "desc"}]
    },
    {
        "select": ["user_id", {"COUNT": "*"}],
        "from": "events",
        "where": [{"col": "type", "op": "eq", "val": "impression"}],
        "group_by": ["user_id"],
        "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
    },
    {
        "select": ["publisher_id", "day", {"SUM": "bid_price"}],
        "from": "events",
        "where": [{"col": "type", "op": "eq", "val": "impression"}],
        "group_by": ["publisher_id", "day"],
        "order_by": [{"col": "day", "dir": "asc"}]
    },
    {
        "select": ["advertiser_id", {"SUM": "total_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "purchase"},
            {"col": "total_price", "op": "gt", "val": 100}
        ],
        "group_by": ["advertiser_id"],
        "order_by": [{"col": "SUM(total_price)", "dir": "desc"}]
    }
]

queries3 = [
    {
        "select": ["publisher_id", "type", {"COUNT": "*"}],
        "from": "events",
        "where": [{"col": "day", "op": "eq", "val": "2024-03-15"}],
        "group_by": ["publisher_id", "type"],
        "order_by": [{"col": "publisher_id", "dir": "asc"}]
    },
    {
        "select": ["hour", {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "click"},
            {"col": "day", "op": "between", "val": ["2024-05-01", "2024-05-07"]}
        ],
        "group_by": ["hour"],
        "order_by": [{"col": "hour", "dir": "asc"}]
    },
    {
        "select": ["publisher_id", "country", {"SUM": "bid_price"}],
        "from": "events",
        "where": [{"col": "type", "op": "eq", "val": "impression"}],
        "group_by": ["publisher_id", "country"],
        "order_by": [{"col": "SUM(bid_price)", "dir": "desc"}]
    },
    {
        "select": ["advertiser_id", {"SUM": "bid_price"}, {"SUM": "total_price"}],
        "from": "events",
        "where": [{"col": "day", "op": "between", "val": ["2024-07-01", "2024-07-31"]}],
        "group_by": ["advertiser_id"],
        "order_by": [{"col": "SUM(total_price)", "dir": "desc"}]
    },
    {
        "select": ["day", {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "purchase"},
            {"col": "country", "op": "eq", "val": "DE"}
        ],
        "group_by": ["day"],
        "order_by": [{"col": "day", "dir": "asc"}]
    }
]

queries4 = [
    {
        "select": ["user_id", {"COUNT": "*"}],
        "from": "events",
        "where": [{"col": "type", "op": "eq", "val": "purchase"}],
        "group_by": ["user_id"],
        "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
    },
    {
        "select": ["country", {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "click"},
            {"col": "day", "op": "between", "val": ["2024-04-01", "2024-04-30"]}
        ],
        "group_by": ["country"],
        "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
    },
    {
        "select": ["advertiser_id", {"SUM": "bid_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "day", "op": "between", "val": ["2024-04-01", "2024-06-30"]}
        ],
        "group_by": ["advertiser_id"],
        "order_by": [{"col": "SUM(bid_price)", "dir": "desc"}]
    },
    {
        "select": ["publisher_id", {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "country", "op": "eq", "val": "IN"}
        ],
        "group_by": ["publisher_id"],
        "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
    },
    {
        "select": ["country", {"AVG": "bid_price"}],
        "from": "events",
        "where": [{"col": "type", "op": "eq", "val": "impression"}],
        "group_by": ["country"],
        "order_by": [{"col": "AVG(bid_price)", "dir": "desc"}]
    }
]

queries5 = [
    {
        "select": ["day", "type", {"COUNT": "*"}],
        "from": "events",
        "where": [{"col": "day", "op": "between", "val": ["2024-08-01", "2024-08-31"]}],
        "group_by": ["day", "type"],
        "order_by": [{"col": "day", "dir": "asc"}]
    },
    {
        "select": ["advertiser_id", "publisher_id", {"SUM": "bid_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "day", "op": "between", "val": ["2024-02-01", "2024-02-29"]}
        ],
        "group_by": ["advertiser_id", "publisher_id"],
        "order_by": [{"col": "SUM(bid_price)", "dir": "desc"}]
    },
    {
        "select": ["user_id", {"SUM": "total_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "purchase"},
            {"col": "total_price", "op": "gt", "val": 50}
        ],
        "group_by": ["user_id"],
        "order_by": [{"col": "SUM(total_price)", "dir": "desc"}]
    },
    {
        "select": ["hour", {"AVG": "total_price"}],
        "from": "events",
        "where": [{"col": "type", "op": "eq", "val": "purchase"}],
        "group_by": ["hour"],
        "order_by": [{"col": "hour", "dir": "asc"}]
    },
    {
        "select": ["advertiser_id", "country", {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "day", "op": "eq", "val": "2024-09-15"}
        ],
        "group_by": ["advertiser_id", "country"],
        "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
    }
]

queries6 = [
    {
        "select": ["publisher_id", "hour", {"SUM": "bid_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "day", "op": "between", "val": ["2024-06-01", "2024-06-07"]}
        ],
        "group_by": ["publisher_id", "hour"],
        "order_by": [{"col": "SUM(bid_price)", "dir": "desc"}]
    },
    {
        "select": ["advertiser_id", "day", {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "serve"},
            {"col": "country", "op": "eq", "val": "US"}
        ],
        "group_by": ["advertiser_id", "day"],
        "order_by": [{"col": "day", "dir": "asc"}]
    },
    {
        "select": ["country", "type", {"COUNT": "*"}],
        "from": "events",
        "where": [{"col": "day", "op": "between", "val": ["2024-03-01", "2024-03-31"]}],
        "group_by": ["country", "type"],
        "order_by": [{"col": "country", "dir": "asc"}]
    },
    {
        "select": ["user_id", {"SUM": "total_price"}, {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "purchase"},
            {"col": "day", "op": "between", "val": ["2024-01-01", "2024-12-31"]}
        ],
        "group_by": ["user_id"],
        "order_by": [{"col": "SUM(total_price)", "dir": "desc"}]
    },
    {
        "select": ["publisher_id", "advertiser_id", {"AVG": "bid_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "day", "op": "eq", "val": "2024-05-20"}
        ],
        "group_by": ["publisher_id", "advertiser_id"],
        "order_by": [{"col": "AVG(bid_price)", "dir": "desc"}]
    }
]

queries7 = [
    {
        "select": ["day", {"SUM": "bid_price"}, {"SUM": "total_price"}],
        "from": "events",
        "where": [{"col": "day", "op": "between", "val": ["2024-10-01", "2024-10-31"]}],
        "group_by": ["day"],
        "order_by": [{"col": "day", "dir": "asc"}]
    },
    {
        "select": ["advertiser_id", "type", {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "country", "op": "eq", "val": "JP"},
            {"col": "day", "op": "between", "val": ["2024-07-01", "2024-07-31"]}
        ],
        "group_by": ["advertiser_id", "type"],
        "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
    },
    {
        "select": ["hour", "country", {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "click"},
            {"col": "day", "op": "eq", "val": "2024-08-15"}
        ],
        "group_by": ["hour", "country"],
        "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
    },
    {
        "select": ["publisher_id", {"AVG": "bid_price"}, {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "country", "op": "eq", "val": "DE"}
        ],
        "group_by": ["publisher_id"],
        "order_by": [{"col": "AVG(bid_price)", "dir": "desc"}]
    },
    {
        "select": ["user_id", "country", {"SUM": "total_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "purchase"},
            {"col": "total_price", "op": "gt", "val": 75}
        ],
        "group_by": ["user_id", "country"],
        "order_by": [{"col": "SUM(total_price)", "dir": "desc"}]
    }
]

queries8 = [
    {
        "select": ["advertiser_id", "publisher_id", "day", {"SUM": "bid_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "day", "op": "between", "val": ["2024-11-01", "2024-11-30"]}
        ],
        "group_by": ["advertiser_id", "publisher_id", "day"],
        "order_by": [{"col": "SUM(bid_price)", "dir": "desc"}]
    },
    {
        "select": ["country", "hour", {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "day", "op": "between", "val": ["2024-09-01", "2024-09-30"]}
        ],
        "group_by": ["country", "hour"],
        "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
    },
    {
        "select": ["advertiser_id", {"SUM": "bid_price"}, {"AVG": "bid_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "country", "op": "eq", "val": "IN"},
            {"col": "day", "op": "between", "val": ["2024-05-01", "2024-05-31"]}
        ],
        "group_by": ["advertiser_id"],
        "order_by": [{"col": "SUM(bid_price)", "dir": "desc"}]
    },
    {
        "select": ["day", "type", {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "publisher_id", "op": "eq", "val": 100},
            {"col": "day", "op": "between", "val": ["2024-01-01", "2024-01-31"]}
        ],
        "group_by": ["day", "type"],
        "order_by": [{"col": "day", "dir": "asc"}]
    },
    {
        "select": ["user_id", "day", {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "click"},
            {"col": "day", "op": "between", "val": ["2024-02-01", "2024-02-29"]}
        ],
        "group_by": ["user_id", "day"],
        "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
    }
]

queries9 = [
    {
        "select": ["publisher_id", "country", "day", {"SUM": "bid_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "day", "op": "between", "val": ["2024-04-01", "2024-04-30"]}
        ],
        "group_by": ["publisher_id", "country", "day"],
        "order_by": [{"col": "SUM(bid_price)", "dir": "desc"}]
    },
    {
        "select": ["advertiser_id", "hour", {"AVG": "bid_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "day", "op": "eq", "val": "2024-07-04"}
        ],
        "group_by": ["advertiser_id", "hour"],
        "order_by": [{"col": "AVG(bid_price)", "dir": "desc"}]
    },
    {
        "select": ["country", {"COUNT": "*"}, {"AVG": "total_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "purchase"},
            {"col": "day", "op": "between", "val": ["2024-12-01", "2024-12-31"]}
        ],
        "group_by": ["country"],
        "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
    },
    {
        "select": ["publisher_id", "type", {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "country", "op": "eq", "val": "US"},
            {"col": "day", "op": "between", "val": ["2024-06-01", "2024-06-30"]}
        ],
        "group_by": ["publisher_id", "type"],
        "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
    },
    {
        "select": ["advertiser_id", "country", {"SUM": "bid_price"}, {"SUM": "total_price"}],
        "from": "events",
        "where": [{"col": "day", "op": "between", "val": ["2024-08-01", "2024-08-31"]}],
        "group_by": ["advertiser_id", "country"],
        "order_by": [{"col": "SUM(total_price)", "dir": "desc"}]
    }
]

queries10 = [
    {
        "select": ["day", "hour", {"SUM": "bid_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "day", "op": "between", "val": ["2024-03-01", "2024-03-07"]}
        ],
        "group_by": ["day", "hour"],
        "order_by": [{"col": "day", "dir": "asc"}]
    },
    {
        "select": ["user_id", "advertiser_id", {"SUM": "total_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "purchase"},
            {"col": "country", "op": "eq", "val": "JP"}
        ],
        "group_by": ["user_id", "advertiser_id"],
        "order_by": [{"col": "SUM(total_price)", "dir": "desc"}]
    },
    {
        "select": ["publisher_id", "advertiser_id", "country", {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "day", "op": "eq", "val": "2024-10-10"}
        ],
        "group_by": ["publisher_id", "advertiser_id", "country"],
        "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
    },
    {
        "select": ["hour", {"SUM": "bid_price"}, {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "country", "op": "eq", "val": "US"},
            {"col": "day", "op": "between", "val": ["2024-11-01", "2024-11-30"]}
        ],
        "group_by": ["hour"],
        "order_by": [{"col": "hour", "dir": "asc"}]
    },
    {
        "select": ["advertiser_id", "publisher_id", "type", {"COUNT": "*"}],
        "from": "events",
        "where": [{"col": "day", "op": "between", "val": ["2024-09-01", "2024-09-30"]}],
        "group_by": ["advertiser_id", "publisher_id", "type"],
        "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
    }
]

queries11 = [
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
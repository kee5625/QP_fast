queries = [
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
    },
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
    },
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
    },
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
SELECT  bidder,
        Count(*)         AS bid_count,
        Min(b_date_time) AS start_time,
        Max(b_date_time) AS end_time
FROM    bid
GROUP BY bidder;

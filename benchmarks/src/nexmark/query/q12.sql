SELECT *, now() as p_time FROM bid;

SELECT  bidder,
        Count(*)        AS bid_count,
        Min(p_time)     AS start_time,
        Max(p_time)     AS end_time
FROM    bid
GROUP BY bidder;

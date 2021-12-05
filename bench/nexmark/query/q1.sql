SELECT auction,
       bidder,
       0.908 * price AS price,
       b_date_time
FROM   bid;

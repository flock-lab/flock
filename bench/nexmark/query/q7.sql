SELECT auction,
       price,
       bidder,
       b_date_time
FROM   bid
       JOIN (SELECT Max(price) AS maxprice
             FROM   bid) AS B1
         ON price = maxprice;

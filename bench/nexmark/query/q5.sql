SELECT auction,
       num
FROM   (SELECT auction,
               Count(*) AS num
        FROM   bid
        GROUP  BY auction) AS AuctionBids
       INNER JOIN (SELECT Max(num) AS maxn
                   FROM   (SELECT auction,
                                  Count(*) AS num
                           FROM   bid
                           GROUP  BY auction) AS CountBids) AS MaxBids
               ON num = maxn;

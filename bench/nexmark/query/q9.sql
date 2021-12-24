SELECT auction,
       bidder,
       price,
       b_date_time
FROM   bid
       JOIN (SELECT a_id       AS id,
                    Max(price) AS final
             FROM   auction
                    INNER JOIN bid
                            ON a_id = auction
             WHERE  b_date_time BETWEEN a_date_time AND expires
             GROUP  BY a_id) AS Q
         ON auction = id
            AND price = final;

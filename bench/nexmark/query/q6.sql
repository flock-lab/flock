SELECT seller,
       Avg(price)
FROM   (SELECT seller,
               price,
               b_date_time,
               Row_number()
                 OVER (
                   partition BY seller
                   ORDER BY b_date_time DESC) AS time_rank
        FROM   (SELECT seller,
                       a_id,
                       price,
                       b_date_time,
                       Row_number()
                         OVER (
                           partition BY a_id
                           ORDER BY price DESC) AS price_rank
                FROM   auction
                       INNER JOIN bid
                               ON a_id = auction
                WHERE  b_date_time BETWEEN a_date_time AND expires
                ORDER  BY a_id,
                          price DESC) AS Q
        WHERE  price_rank = 1) AS R
WHERE  time_rank <= 10
GROUP  BY seller

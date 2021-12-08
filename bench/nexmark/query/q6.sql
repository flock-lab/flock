SELECT seller,
       Avg(final)
FROM   (SELECT ROW_NUMBER()
                 OVER (
                   PARTITION BY seller
                   ORDER BY date_time DESC) AS row,
               seller,
               final
        FROM   (SELECT seller,
                       Max(price)       AS final,
                       Max(b_date_time) AS date_time
                FROM   auction
                       INNER JOIN bid
                               ON a_id = auction
                WHERE  b_date_time BETWEEN a_date_time AND expires
                GROUP  BY a_id,
                          seller) AS Q) AS R
WHERE  row <= 10
GROUP  BY seller;

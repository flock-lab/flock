SELECT category,
       Avg(final)
FROM   (SELECT Max(price) AS final,
               category
        FROM   auction
               INNER JOIN bid
                       ON a_id = auction
        WHERE  b_date_time BETWEEN a_date_time AND expires
        GROUP  BY a_id,
                  category) AS Q
GROUP  BY category;

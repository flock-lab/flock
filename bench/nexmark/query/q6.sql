SELECT Count(DISTINCT seller)
FROM   auction
       INNER JOIN bid
               ON a_id = auction
WHERE  b_date_time BETWEEN a_date_time AND expires;


SELECT seller,
       Max(price) AS final
FROM   auction
       INNER JOIN bid
               ON a_id = auction
WHERE  b_date_time BETWEEN a_date_time AND expires
GROUP  BY a_id,
          seller
ORDER  BY seller;


SELECT seller,
       Avg(final)
FROM   q
GROUP  BY seller;

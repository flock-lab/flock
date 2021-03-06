SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM   lineitem
WHERE  l_shipdate >= DATE '1994-01-01'
       AND l_shipdate < DATE '1995-01-01'
       AND l_discount BETWEEN .06 - 0.01 AND .06 + 0.01
       AND l_quantity < 24;

SELECT o_orderpriority,
       Count(*) AS order_count
FROM   orders
WHERE  o_orderdate >= '1993-07-01'
       AND o_orderdate < DATE '1993-07-01' + interval '3' month
       AND EXISTS (SELECT *
                   FROM   lineitem
                   WHERE  l_orderkey = o_orderkey
                          AND l_commitdate < l_receiptdate)
GROUP  BY o_orderpriority
ORDER  BY o_orderpriority;

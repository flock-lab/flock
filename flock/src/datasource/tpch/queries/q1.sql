SELECT l_returnflag,
       l_linestatus,
       SUM(l_quantity)                                           AS sum_qty,
       SUM(l_extendedprice)                                      AS
       sum_base_price,
       SUM(l_extendedprice * ( 1 - l_discount ))                 AS
       sum_disc_price,
       SUM(l_extendedprice * ( 1 - l_discount ) * ( 1 + l_tax )) AS sum_charge,
       Avg(l_quantity)                                           AS avg_qty,
       Avg(l_extendedprice)                                      AS avg_price,
       Avg(l_discount)                                           AS avg_disc,
       Count(*)                                                  AS count_order
FROM   lineitem
WHERE  l_shipdate <= DATE '1998-09-02'
GROUP  BY l_returnflag,
          l_linestatus
ORDER  BY l_returnflag,
          l_linestatus;

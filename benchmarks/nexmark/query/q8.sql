SELECT p_id,
       name
FROM   (SELECT p_id,
               name
        FROM   person
        GROUP  BY p_id,
                  name) AS P
       JOIN (SELECT seller
             FROM   auction
             GROUP  BY seller) AS A
         ON p_id = seller;

SELECT name,
       city,
       state,
       a_id
FROM   auction
       INNER JOIN person
               ON seller = p_id
WHERE  category = 10
       AND ( state = 'or'
              OR state = 'id'
              OR state = 'ca' );

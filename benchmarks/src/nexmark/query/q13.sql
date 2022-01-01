SELECT  auction,
        bidder,
        price,
        b_date_time,
        value
FROM    bid
        JOIN side_input
            ON auction = key;

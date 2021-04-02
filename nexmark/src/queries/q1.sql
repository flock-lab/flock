-- -------------------------------------------------------------------------------------------------
-- Query1: Currency conversion
-- -------------------------------------------------------------------------------------------------
-- Convert each bid value from dollars to euros. Illustrates a simple transformation.
--
-- The purpose of Query 1 is to test the processing speed of the stream system and to provide a
-- reference point for the rest of the queries. One essential feature tested by Query 1 is parse
-- speed. Parse speed is particularly important in stream systems because stream systems operate
-- on data that is not in native database format and reading such data can be slow.
-- -------------------------------------------------------------------------------------------------

SELECT
    auction,
    bidder,
    0.908 * price as price, -- convert dollar to euro
    date_time
FROM bid;

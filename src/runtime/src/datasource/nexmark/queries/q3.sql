-- -------------------------------------------------------------------------------------------------
-- Query 3: Local Item Suggestion
-- -------------------------------------------------------------------------------------------------
-- Who is selling in OR, ID or CA in category 10, and for what auction ids?
-- Illustrates an incremental join (using per-key state and timer) and filter.
--
-- Query 3 is designed to test join functionality. We imagine that buyers want to find items in a
-- particular category that are for sale by sellers who live near them. Query 3 performs a join
-- between the stream of new items for auction and the people registered with the auction system.
-- The query should output a result every time a new item becomes for sale in category 10 in Oregon.
--
-- Note that the result should not contain items that are no longer up for auction (closed auctions).
-- -------------------------------------------------------------------------------------------------

SELECT
    P.name, P.city, P.state, A.id
FROM
    auction AS A INNER JOIN person AS P on A.seller = P.id
WHERE
    A.category = 10 and (P.state = 'OR' OR P.state = 'ID' OR P.state = 'CA');

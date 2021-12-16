SELECT  campaign_id,
        Count(*)
FROM    ad_event
        INNER JOIN campaign
                ON ad_id = c_ad_id
WHERE   event_type = 'view'
GROUP   BY campaign_id

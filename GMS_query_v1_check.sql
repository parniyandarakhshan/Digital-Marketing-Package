###############################################################################################################
#########                                         Pageview                                       ##############
###############################################################################################################
SELECT SUM(pageviews), SUM(unique_pageviews)
FROM (
    SELECT 
        hit_year, hit_month, pagePath, source, channel_grouping, device_category, country, city,
        COUNT(*) AS pageviews, COUNT(DISTINCT session_id) AS unique_pageviews 
    FROM ( 
        SELECT 
            SUBSTR(date, 1 ,4) AS hit_year,
            SUBSTR(date, 5 ,2) AS hit_month,
            hits.page.pagePath AS pagePath,
            trafficsource.source AS source,
            channelgrouping as channel_grouping,
            device.deviceCategory as device_category,
            geonetwork.country as country,
            geonetwork.city as city,
            CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) AS session_id 
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801` AS GA, UNNEST(GA.hits) AS hits 
        WHERE hits.type = 'PAGE'
    ) 
    GROUP BY hit_year, hit_month, pagePath, source, channel_grouping, device_category, country, city
)
;
SELECT SUM(pageviews), SUM(unique_pageviews)
FROM (
    SELECT pagePath, COUNT(*) AS pageviews, COUNT(DISTINCT session_id) AS unique_pageviews 
    FROM ( 
        SELECT hits.page.pagePath, CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) AS session_id 
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801` AS GA, UNNEST(GA.hits) AS hits 
        WHERE hits.type = 'PAGE'
    ) 
    GROUP BY pagePath
)
;
SELECT SUM(pageviews), SUM(unique_pageviews)
FROM (
    SELECT pagePath, COUNT(*) AS pageviews, COUNT(DISTINCT session_id) AS unique_pageviews 
    FROM ( 
        SELECT hits.page.pagePath, CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) AS session_id 
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801` AS GA, UNNEST(GA.hits) AS hits 
        WHERE hits.type = 'PAGE'
    ) 
    GROUP BY pagePath
)
;
###############################################################################################################
#########                                       Time On Page                                     ##############
###############################################################################################################
SELECT sum(total_time_on_page_combined)
FROM (
    SELECT 
        hit_year, hit_month, pagePath, source, channel_grouping, device_category, country, city,
        SUM(time_on_page_combined) as total_time_on_page_combined
    FROM (
        SELECT 
            *, 
            CASE WHEN isExit IS TRUE THEN last_interaction_second - hit_time_second 
                ELSE next_pageview_second - hit_time_second END as time_on_page_combined
        FROM ( 
            SELECT *, LEAD(hit_time_second) OVER 
                        (PARTITION BY fullVisitorId, visitStartTime ORDER BY hit_time_second) AS next_pageview_second  
            FROM ( 
                SELECT 
                    SUBSTR(date, 1 ,4) AS hit_year,
                    SUBSTR(date, 5 ,2) AS hit_month,
                    fullVisitorId, 
                    visitStartTime, 
                    hits.page.pagePath AS pagePath,
                    trafficsource.source AS source,
                    channelgrouping as channel_grouping,
                    device.deviceCategory as device_category,
                    geonetwork.country as country,
                    geonetwork.city as city,
                    hits.type, hits.isExit, 
                    hits.time/1000 AS hit_time_second, 
                    MAX(hits.time/1000) OVER (PARTITION BY fullVisitorId, visitStartTime) as last_interaction_second
                FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801`, UNNEST(hits) AS hits 
                WHERE hits.isInteraction is TRUE
            ) 
            WHERE type = 'PAGE'
        )
    )
    GROUP BY hit_year, hit_month, pagePath, source, channel_grouping, device_category, country, city
)
;
SELECT SUM(total_time_on_page_combined)
FROM (
    SELECT pagePath, SUM(time_on_page_combined) as total_time_on_page_combined
    FROM (
        SELECT *, CASE WHEN isExit IS TRUE THEN last_interaction_second - hit_time_second ELSE next_pageview_second - hit_time_second END as time_on_page_combined
        FROM ( 
            SELECT *, LEAD(hit_time_second) OVER (PARTITION BY fullVisitorId, visitStartTime ORDER BY hit_time_second) AS next_pageview_second  
            FROM ( 
                SELECT fullVisitorId, visitStartTime, hits.page.pagePath, hits.type, hits.isExit, hits.time/1000 AS hit_time_second, MAX(hits.time/1000) OVER (PARTITION BY fullVisitorId, visitStartTime) as last_interaction_second
                FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801`, UNNEST(hits) AS hits 
                WHERE hits.isInteraction is TRUE
            ) 
            WHERE type = 'PAGE'
        )
    )
    GROUP BY pagePath
)
;
###############################################################################################################
#########                                         Session                                        ##############
###############################################################################################################
SELECT SUM(total_sessions)
FROM (
    SELECT 
        hit_year, hit_month, pagePath, source, channel_grouping, device_category, country, city,
        SUM(sessions) AS total_sessions
    FROM (
        SELECT 
            *,
            CASE WHEN hitNumber = first_interaction THEN visits ELSE 0 END AS sessions 
        FROM ( 
            SELECT 
                SUBSTR(date, 1 ,4) AS hit_year,
                SUBSTR(date, 5 ,2) AS hit_month,
                fullVisitorId, 
                visitStartTime, 
                hits.page.pagePath AS pagePath,
                trafficsource.source AS source,
                channelgrouping as channel_grouping,
                device.deviceCategory as device_category,
                geonetwork.country as country,
                geonetwork.city as city,
                totals.visits, hits.hitNumber, 
                MIN(hits.hitNumber) OVER (PARTITION BY fullVisitorId, visitStartTime) AS first_interaction
            FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801` AS GA, UNNEST(GA.hits) AS hits
            WHERE hits.isInteraction IS TRUE
        )
    )
    GROUP BY hit_year, hit_month, pagePath, source, channel_grouping, device_category, country, city
)
;
SELECT SUM(totals.visits)
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801`
;
###############################################################################################################
#########                                         Bounce                                         ##############
###############################################################################################################
SELECT SUM(total_bounces)
FROM (
    SELECT 
        hit_year, hit_month, pagePath, source, channel_grouping, device_category, country, city,
        SUM(page_bounces) AS total_bounces
    FROM (
        SELECT 
            *, 
            CASE WHEN hitNumber = first_interaction THEN bounces ELSE 0 END AS page_bounces 
        FROM ( 
            SELECT 
                SUBSTR(date, 1 ,4) AS hit_year,
                SUBSTR(date, 5 ,2) AS hit_month,
                fullVisitorId, 
                visitStartTime, 
                hits.page.pagePath AS pagePath,
                trafficsource.source AS source,
                channelgrouping as channel_grouping,
                device.deviceCategory as device_category,
                geonetwork.country as country,
                geonetwork.city as city,
                totals.bounces, 
                hits.hitNumber, 
                MIN(hits.hitNumber) OVER (PARTITION BY fullVisitorId, visitStartTime) AS first_interaction 
            FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801` AS GA, UNNEST(GA.hits) AS hits
            WHERE hits.isInteraction IS TRUE
        )
    )
    GROUP BY hit_year, hit_month, pagePath, source, channel_grouping, device_category, country, city
)
;
SELECT SUM(totals.bounces)
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801`
;


{{
	config(
		materialized='table',
		tags=["dim"]
	)
}}

WITH dim_proj_internotes AS (

    SELECT
        id
        , name
        , customer_id
        , CASE
            WHEN internal_notes LIKE '%Auditor%' THEN 'Auditor'
            WHEN internal_notes LIKE '%Non-SRT%' THEN 'Non-SRT'
            WHEN internal_notes LIKE '%Not AWD%' THEN 'Not AWD'
            WHEN internal_notes LIKE '%AWD%' THEN 'AWD'
            ELSE 'Needs Updating'
        END AS is_awd
        , CASE
            WHEN internal_notes LIKE '%Ads Integrity%' THEN 'Ads Integrity'
            WHEN internal_notes LIKE '%Search Metrics%' THEN 'Search Metrics'
            WHEN internal_notes LIKE '%Community Ops%' THEN 'Community Ops'
            WHEN internal_notes LIKE '%Halo%' THEN 'Halo'
            WHEN internal_notes LIKE '%Media Ops%' THEN 'Media Ops'
            WHEN internal_notes LIKE '%Business Integrity%' THEN 'Business Integrity'
            WHEN internal_notes LIKE '%ADAP%' THEN 'ADAP'
            WHEN internal_notes LIKE '%Contextual Optimization%' THEN 'Contextual Optimization'
            WHEN internal_notes LIKE '%BI%' THEN 'BI'
            ELSE 'Needs Updating'
        END AS vertical
        , internal_notes
    FROM qrp.exp_projects

)

SELECT *
FROM dim_proj_internotes

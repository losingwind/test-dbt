{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

SELECT DISTINCT
    mp_user_id
    , CASE
        WHEN utm_campaign LIKE '%Campaign_%' THEN 'Digital Acquisition'
        ELSE 'Organic Acquisition'
    END AS user_acquisition
FROM {{ source('mixpanel','fct_contributors_verify_email') }}
WHERE mp_date_created >= '2022-08-15'

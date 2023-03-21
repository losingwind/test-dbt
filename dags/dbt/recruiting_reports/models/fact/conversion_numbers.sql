{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

WITH list AS (

    SELECT
        f.user_id
        , f.project_id
        , f.locale_id
        , f.country
        , f.screened
        , f.qualified
        , f.project_registered
        , f.application_received
        , f.ready_to_contribute
        , f.invoiced
    FROM {{ source('recruiting','fct_funnel_report_flow_status') }} f
    LEFT JOIN {{ source('qrp','exp_projects') }} p
        ON p.id = f.project_id
    WHERE p.status = 'ENABLED'

)

SELECT
    list.project_id
    , list.locale_id
    , list.country
    , COUNT(DISTINCT CASE WHEN list.screened = 1 THEN list.user_id END) AS screened
    , COUNT(DISTINCT CASE WHEN list.qualified = 1 THEN list.user_id END) AS qualified
    , COUNT(DISTINCT CASE WHEN list.project_registered = 1 THEN list.user_id END) AS registered
    , COUNT(DISTINCT CASE WHEN list.ready_to_contribute = 1 THEN list.user_id END) AS rtc
    , COUNT(DISTINCT CASE WHEN list.application_received = 1 THEN list.user_id END) AS applications
    , COUNT(DISTINCT CASE WHEN list.invoiced = 1 THEN list.user_id END) AS invoiced
FROM list
GROUP BY 1, 2, 3

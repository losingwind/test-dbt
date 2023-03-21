{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

SELECT
    DATE_TRUNC('day', fct_funnel.registration_date) AS registration_date
    , CASE
        WHEN mixpanel.campaign_source = 'GRG' THEN 'GRG'
        ELSE 'NON GRG'
    END AS campaign_source
    , fct_funnel.user_id
    , fct_funnel.project_id
FROM {{ source('recruiting','fct_funnel_report_flow_status') }} AS fct_funnel
LEFT JOIN {{ ref('fct_contributors_verify_email') }} AS mixpanel
    ON mixpanel.mp_user_id = fct_funnel.user_id
WHERE fct_funnel.project_id IN (
        1, 40, 87, 106, 108, 109, 142, 352, 610, 1060, 1373, 1822, 2022, 2224
        , 2334, 2478, 2723, 2804, 3572, 3653, 3767, 4340, 4754, 4755, 4763, 4931
        , 4963, 4988
    )
    AND fct_funnel.ready_to_contribute = 1

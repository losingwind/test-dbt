{{
    config(
        materialized='table', sort=['project_id', 'locale_id']
        , tags=["summary"]
    )
}}

WITH smry_contributor_conversion_rate_proj_loc AS (

    SELECT
        locale_id
        , project_id
        , AVG(DATEDIFF(DAY, project_registered_date, application_received_date)) AS regis_to_app
        , AVG(DATEDIFF(DAY, application_received_date, screened_date)) AS app_to_screen
        , AVG(DATEDIFF(DAY, screened_date, qualified_active_date)) AS screen_to_quali
        , AVG(DATEDIFF(DAY, qualified_active_date, ready_to_contribute_active_date)) AS quali_to_rtc
        , AVG(DATEDIFF(DAY, ready_to_contribute_active_date, first_invoice)) AS rtc_to_first_invoice
        , CONVERT_TIMEZONE('UTC', 'US/Pacific', GETDATE()) AS edw_date_created
    FROM
        {{ ref('fct_contributor_lifecycle_pl_sort') }}
    GROUP BY
        project_id, locale_id
)

SELECT *
FROM smry_contributor_conversion_rate_proj_loc

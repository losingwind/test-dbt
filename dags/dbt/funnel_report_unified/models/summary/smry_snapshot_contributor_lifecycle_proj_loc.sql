{{
    config(
        materialized='table'
        , tags=["summary"]
    )
}}

WITH smry_snapshot_contributor_lifecycle_proj_loc AS (

    SELECT
        MAX(registration_date) AS registration_date
        , project_id AS project_id
        , locale_id AS locale_id
        , MAX(country) AS country
        , SUM(project_registered) AS project_registered
        , SUM(application_received) AS application_received
        , SUM(screened) AS screened
        , SUM(qualified) AS qualified
        , SUM(ready_to_contribute) AS ready_to_contribute
        , CONVERT_TIMEZONE('UTC', 'US/Pacific', GETDATE()) AS edw_date_created
    FROM
        {{ ref('fct_snapshot_contributor_lifecycle') }}
    GROUP BY
        project_id, locale_id
)

SELECT *
FROM smry_snapshot_contributor_lifecycle_proj_loc

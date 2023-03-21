{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

WITH quiz_project_mapping AS (

    SELECT DISTINCT
        quiz_id
        , project_id
    FROM {{ source('qrp','resource_mappings') }}
    WHERE project_id = 4931
        AND usage_type = 'QUALIFICATION'

)

SELECT
    quiz_trackers.quiz_id
    , quiz_trackers.user_id
    , quiz_project_mapping.project_id
    , usr_proj_mapping.locale_id
    , quiz_trackers.status
    , quiz_trackers.result
    , quiz_trackers.score
    , quiz_trackers.date_created
    , quiz_trackers.date_updated
    , ROW_NUMBER() OVER (
        PARTITION BY quiz_trackers.quiz_id, quiz_trackers.user_id
        ORDER BY quiz_trackers.date_created
    ) AS attempt_count
FROM {{ source('qrp','quiz_trackers') }} quiz_trackers
LEFT JOIN quiz_project_mapping
    ON quiz_trackers.quiz_id = quiz_project_mapping.quiz_id
LEFT JOIN {{ source('qrp','exp_user_project_mappings') }} usr_proj_mapping
    ON quiz_project_mapping.project_id = usr_proj_mapping.project_id
        AND quiz_trackers.user_id = usr_proj_mapping.user_id
WHERE quiz_project_mapping.project_id = 4931

{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

WITH active_users AS (

    SELECT DISTINCT
        user_id
        , project_id
    FROM {{ source('qrp','exp_user_project_access_status_changes') }}
    WHERE status = 'ACTIVE'
        AND project_id = 4931

)

SELECT
    usr_proj_sts.id AS status_change_id
    , usr_proj_sts.user_id
    , usr_proj_sts.project_id
    , usr_proj_sts.status
    , usr_proj_sts.reason AS status_change_reason
    , usr_proj_sts.date_created AS revoked_date_historical
    , proj_mapping.revoked_date
    , proj_mapping.date_created AS registration_date
    , proj_mapping.active_date
    , proj_mapping.revoked_reason
FROM {{ source('qrp','exp_user_project_access_status_changes') }} usr_proj_sts
LEFT JOIN active_users
    ON usr_proj_sts.user_id = active_users.user_id
        AND usr_proj_sts.project_id = active_users.project_id
LEFT JOIN {{ source('qrp','exp_user_project_mappings') }} proj_mapping
    ON proj_mapping.user_id = usr_proj_sts.user_id
        AND proj_mapping.project_id = usr_proj_sts.project_id
WHERE usr_proj_sts.project_id = 4931
    AND active_users.user_id IS NULL
    AND active_users.project_id IS NULL
    AND usr_proj_sts.status IN ('REVOKED', 'REJECTED', 'BLOCKED', 'ABANDONED', 'EXAM_FAILED')

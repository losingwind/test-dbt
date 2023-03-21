{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

WITH active_users AS (

    SELECT
        user_id
        , project_id
        , MAX(date_created) AS last_active_date
    FROM {{ source('qrp','exp_user_project_access_status_changes') }}
    WHERE status = 'ACTIVE'
        AND project_id = 4931
    GROUP BY 1, 2

)

, invoiced AS (

    SELECT DISTINCT
        user_id
        , project_id
    FROM {{ source('invoicing','fct_contributor_invoices') }}
    WHERE project_id = 4931
        AND invoice_status = 'PAID'

)

SELECT
    usr_proj_sts.id AS status_change_id
    , usr_proj_sts.user_id
    , usr_proj_sts.project_id
    , usr_proj_sts.status
    , CASE WHEN invoiced.user_id IS NOT NULL THEN TRUE END AS invoiced
    , usr_proj_sts.reason AS status_change_reason
    , usr_proj_sts.date_created AS revoked_date_historical
    , proj_mapping.revoked_date
    , proj_mapping.date_created AS registration_date
    , proj_mapping.active_date
    , proj_mapping.revoked_reason
FROM {{ source('qrp','exp_user_project_access_status_changes') }} usr_proj_sts
INNER JOIN active_users
    ON usr_proj_sts.user_id = active_users.user_id
        AND usr_proj_sts.project_id = active_users.project_id
LEFT JOIN invoiced
    ON usr_proj_sts.user_id = invoiced.user_id
        AND usr_proj_sts.project_id = invoiced.project_id
LEFT JOIN {{ source('qrp','exp_user_project_mappings') }} proj_mapping
    ON proj_mapping.user_id = usr_proj_sts.user_id
        AND proj_mapping.project_id = usr_proj_sts.project_id
WHERE usr_proj_sts.project_id = 4931
    AND usr_proj_sts.status IN ('REVOKED', 'REJECTED', 'BLOCKED', 'ABANDONED', 'EXAM_FAILED')
    AND usr_proj_sts.date_created > active_users.last_active_date

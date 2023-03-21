{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

SELECT
    ue.user_id
    , ue.project_id || ' - ' || p.name AS project
    , ue.type
FROM {{ source('qrp','user_email_unsubscriptions') }} ue
LEFT JOIN {{ source('qrp','exp_projects') }} p
    ON p.id = ue.project_id
WHERE ue.is_unsubscribed = 1
    AND p.status = 'ENABLED'
    AND ue.type NOT IN ('RECRUITMENT', 'PROJECT_OPERATIONAL')
    AND ue.project_id IS NOT NULL

{{
    config(
        materialized='table',
        tags=["staging"]
    )
}}

SELECT
    prj_mappings.user_id
    , prj.id AS project_id
    , prj.name AS project_name
    , prj.type AS project_type
    , prj_mappings.locale_id
    , prj_mappings.status AS project_status
    , prj_mappings.date_updated AS last_project_update
FROM {{ source('qrp','exp_user_project_mappings') }} prj_mappings
LEFT JOIN {{ source('qrp','exp_projects') }} prj
    ON prj.id = prj_mappings.project_id
WHERE prj.status = 'ENABLED'

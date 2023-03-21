{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}


SELECT
    user_list.contributor_id
    , prj_mappings.locale_id
    , prj_mappings.project_id
    , prj_mappings.project_name
    , prj_mappings.project_status
    , user_list.status AS user_status
    , user_list.last_user_update
    , prj_mappings.last_project_update
FROM {{ source('dim','dim_contributors') }} user_list
INNER JOIN {{ ref('enabled_user_projects') }} prj_mappings
    ON user_list.contributor_id = prj_mappings.user_id
WHERE user_list.last_login  >= (CURRENT_TIMESTAMP - INTERVAL '6 month') -- noqa
    AND user_list.status IN ('ACTIVE', 'CONTRACT_PENDING', 'IN_ACTIVATION_QUEUE', 'PAYONEER_SETUP'
        , 'EXPRESS_QUALIFYING', 'EXPRESS_ACTIVE', 'SCREENED', 'REGISTERED', 'APPLICATION_RECEIVED', 'ON_HOLD', 'STAGED'
    )

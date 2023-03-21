{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

WITH int_attribute AS (

    SELECT
        usr_int_attr.user_id
        , int_attr.name AS user_intelligent_attribute
        , usr_int_attr.string_value
        , usr_int_attr.numeric_value
        , CASE
            WHEN usr_int_attr.boolean_value = 1 THEN
                'True'
            WHEN usr_int_attr.boolean_value IS NULL THEN
                NULL
            ELSE
                'False'
        END AS boolean_value
        , usr_int_attr.project_id
    FROM {{ source('qrp','user_intelligent_attributes') }} usr_int_attr
    LEFT JOIN {{ source('qrp','exp_intelligent_attributes') }} int_attr
        ON usr_int_attr.intelligent_attribute_id = int_attr.id
    LEFT JOIN {{ source('qrp','exp_projects') }} projects
        ON projects.id = usr_int_attr.project_id
    WHERE int_attr.is_disabled = 0
        AND projects.status = 'ENABLED'

)

SELECT
    user_list.contributor_id
    , int_attribute.user_intelligent_attribute
    , int_attribute.project_id
    , int_attribute.string_value
    , int_attribute.boolean_value
    , int_attribute.numeric_value
    , CONCAT(
        CONCAT(
            COALESCE(int_attribute.string_value, '')
            , COALESCE(int_attribute.boolean_value, '')
        )
        , COALESCE(int_attribute.numeric_value::VARCHAR, '')
    ) AS attribute_value
FROM {{ source('dim','dim_contributors') }} user_list
INNER JOIN int_attribute
    ON int_attribute.user_id = user_list.contributor_id
WHERE user_list.last_login  >= (CURRENT_TIMESTAMP - INTERVAL '6 month') -- noqa
    AND user_list.status IN ('ACTIVE', 'CONTRACT_PENDING', 'IN_ACTIVATION_QUEUE', 'PAYONEER_SETUP'
        , 'EXPRESS_QUALIFYING', 'EXPRESS_ACTIVE', 'SCREENED', 'REGISTERED', 'APPLICATION_RECEIVED', 'ON_HOLD', 'STAGED'
    )

{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

SELECT
    hiring_targets.project_id
    , hiring_targets.id AS hiring_target_id
    , hiring_targets.locale_id
    , projects.workday_id
    , projects.customer_id
    , hiring_targets.to_locale_id
    , hiring_targets.locale_lang AS hiring_target_locale_lang
    , hiring_targets.locale_country AS hiring_target_locale_country
    , hiring_targets.country AS hiring_target_country
    , hiring_targets.target AS hiring_target
    , hiring_targets.date_created AS hiring_target_date_created
FROM {{ source('qrp','exp_project_hiring_targets') }} hiring_targets
LEFT JOIN {{ source('qrp','exp_projects') }} projects
    ON hiring_targets.project_id = projects.id

{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

SELECT
    proj_mapping.project_id
    , proj_mapping.locale_id
    , usr.country
    , COUNT(DISTINCT proj_mapping.user_id) AS count
    , COUNT(DISTINCT invoices.user_id) AS invoice_count
FROM {{ source('qrp','exp_user_project_mappings') }} proj_mapping
INNER JOIN {{ source('qrp','users') }} usr
    ON proj_mapping.user_id = usr.id
LEFT JOIN {{ source('invoicing','fct_contributor_invoices') }} invoices
    ON proj_mapping.user_id = invoices.user_id
        AND proj_mapping.project_id = invoices.project_id
        AND proj_mapping.locale_id = invoices.locale_id
WHERE proj_mapping.status = 'ACTIVE'
    AND usr.status = 'ACTIVE'
GROUP BY 1, 2, 3

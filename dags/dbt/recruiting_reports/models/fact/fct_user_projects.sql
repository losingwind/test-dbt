{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

WITH user_project_first_invoice AS (

    SELECT
        i.user_id
        , i.project_id
        , MIN(i.invoice_start_date) AS first_usr_proj_invoice
        , MAX(i.invoice_start_date) AS last_usr_proj_invoice
    FROM {{ source('invoicing','fct_contributor_invoices') }} i
    GROUP BY 1, 2

)

, last_revoked AS (

    SELECT *
    FROM (
        SELECT
            r.user_id
            , r.project_id
            , r.date_created AS latest_revoked_date
            , ROW_NUMBER() OVER (
                PARTITION BY r.user_id, r.project_id ORDER BY r.date_created DESC
            ) AS revoke_num
        FROM {{ source('qrp','exp_user_project_access_status_changes') }} r
        WHERE r.status = 'REVOKED'
    ) temp
    WHERE temp.revoke_num = 1

)

SELECT
    COALESCE(usr_proj_mapp.user_id, last_revoked.user_id) AS contributor_id
    , COALESCE(usr_proj_mapp.project_id, last_revoked.project_id) AS project_id
    , usr_proj_mapp.locale_id
    , usr_proj_mapp.status
    , user_project_first_invoice.first_usr_proj_invoice
    , user_project_first_invoice.last_usr_proj_invoice
    , last_revoked.latest_revoked_date
FROM {{ source('qrp','exp_user_project_mappings') }} usr_proj_mapp
LEFT JOIN last_revoked
    ON last_revoked.user_id = usr_proj_mapp.user_id
        AND last_revoked.project_id = usr_proj_mapp.project_id
LEFT JOIN user_project_first_invoice
    ON user_project_first_invoice.user_id = usr_proj_mapp.user_id
        AND user_project_first_invoice.project_id = usr_proj_mapp.project_id

{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

WITH user_invoices_other_projects AS (

    SELECT
        user_id
        , project_id
        , locale_id
        , MAX(invoice_requested_date) AS latest_paid_invoice
    FROM {{ source('invoicing','fct_contributor_invoices') }}
    WHERE invoice_status = 'PAID'
        AND project_id != 4931
    GROUP BY 1, 2, 3

)

, user_invoices_uolo_project AS (

    SELECT
        user_id
        , project_id
        , locale_id
        , MAX(invoice_requested_date) AS latest_paid_invoice
    FROM {{ source('invoicing','fct_contributor_invoices') }}
    WHERE invoice_status = 'PAID'
        AND project_id = 4931
    GROUP BY 1, 2, 3

)

SELECT
    bpm_user_processes.id
    , bpm_user_processes.user_id
    , usr_proj_map.project_id
    , usr_proj_map.locale_id
    , bpm_user_processes.type
    , bpm_user_processes.name
    , bpm_user_processes.status
    , bpm_user_processes.date_created
    , CASE
        WHEN bpm_user_processes.status = 'COMPLETE' THEN
            DATEDIFF(SECONDS, bpm_user_processes.date_created, bpm_user_processes.date_updated)
    END AS process_duration_seconds
    , COALESCE(usr.status IN ('ACTIVE', 'EXPRESS_ACTIVE') AND usr_proj_map.status = 'ACTIVE', FALSE) AS is_rtc
    , user_invoices_other_projects.latest_paid_invoice
    , user_invoices_uolo_project.latest_paid_invoice AS uolo_latest_paid_invoice
FROM {{ source('qrp','bpm_user_processes') }} AS bpm_user_processes
LEFT JOIN {{ source('qrp','exp_user_project_mappings') }} AS usr_proj_map
    ON bpm_user_processes.user_id = usr_proj_map.user_id
        AND bpm_user_processes.id = usr_proj_map.user_registration_process_id
LEFT JOIN {{ source('qrp','users') }} AS usr
    ON bpm_user_processes.user_id = usr.id
LEFT JOIN user_invoices_other_projects
    ON bpm_user_processes.user_id = user_invoices_other_projects.user_id
        AND usr_proj_map.project_id = user_invoices_other_projects.project_id
        AND usr_proj_map.locale_id = user_invoices_other_projects.locale_id
LEFT JOIN user_invoices_uolo_project
    ON bpm_user_processes.user_id = user_invoices_uolo_project.user_id
        AND usr_proj_map.project_id = user_invoices_uolo_project.project_id
        AND usr_proj_map.locale_id = user_invoices_uolo_project.locale_id
WHERE usr_proj_map.project_id = 4931

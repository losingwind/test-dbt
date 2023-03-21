{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

WITH inv AS (

    SELECT
        i.project_id
        , i.user_country AS country
        , i.user_id
        , i.locale_id
        , i.invoice_start_date AS invoice_month
        , i.invoice_requested_date AS date_requested
        , i.invoice_status AS invoice_status
        , i.rate_type AS value
        , SUM(i.worked_hours) + SUM(i.units_of_work_completed) AS quantity
        , SUM(i.amount_paid) AS invoice_amount
    FROM {{ source('invoicing','fct_contributor_invoices') }} i
    LEFT JOIN {{ source('qrp','exp_projects') }} p
        ON p.id = i.project_id
    WHERE p.status = 'ENABLED'
        AND i.invoice_requested_date > CURRENT_DATE - INTERVAL '3 Months'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

)

, prj AS (

    SELECT
        proj_mapping.user_id
        , proj_mapping.project_id
        , p.name AS project_name
        , proj_mapping.locale_id
        , proj_mapping.status AS project_status
    FROM {{ source('qrp','exp_user_project_mappings') }} proj_mapping
    LEFT JOIN {{ source('qrp','exp_projects') }} p
        ON proj_mapping.project_id = p.id
    WHERE p.status = 'ENABLED'

)

SELECT
    usr.contributor_id
    , usr.contributor_name
    , usr.email
    , usr.country
    , usr.status AS user_status
    , usr.state
    , usr.city
    , usr.age
    , usr.gender
    , usr.phone_number
    , usr.language
    , usr.dialect
    , usr.locale_country
    , usr.spoken_fluency
    , usr.written_fluency
    , usr.is_unsubs_in_type
    , usr.user_application_source
    , usr.tenant
    , usr.last_login
    , usr.last_user_update
    , usr.date_created AS registration_date
    , usr.num_days_in_user_status
    , prj.project_id
    , prj.project_name AS invoice_project
    , prj.locale_id
    , prj.project_status
    , prj.project_name AS applied_to_project
    , COUNT(CASE WHEN inv.invoice_status = 'PAID' THEN inv.user_id END) AS paid_invoice_count
    , COUNT(CASE WHEN inv.invoice_status != 'REJECTED' THEN inv.user_id END) AS invoice_request_count
    , AVG(CASE WHEN inv.invoice_status = 'PAID' THEN inv.invoice_amount END) AS avg_paid_invoiced_amount
FROM {{ source('dim','dim_contributors') }} usr
LEFT JOIN prj
    ON usr.contributor_id = prj.user_id
LEFT JOIN inv
    ON prj.user_id = inv.user_id
        AND prj.project_id = inv.project_id
        AND prj.locale_id = inv.locale_id
WHERE usr.contributor_id IS NOT NULL
    AND usr.last_login  >= (CURRENT_TIMESTAMP - INTERVAL '6 month') -- noqa
    AND usr.status IN ('ACTIVE', 'CONTRACT_PENDING', 'IN_ACTIVATION_QUEUE', 'PAYONEER_SETUP'
        , 'EXPRESS_QUALIFYING', 'EXPRESS_ACTIVE', 'SCREENED', 'REGISTERED', 'APPLICATION_RECEIVED', 'ON_HOLD', 'STAGED'
    )
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27

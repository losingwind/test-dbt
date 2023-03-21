{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

WITH invoice_submitted_date AS (

    SELECT
        invoice_events.invoice_id
        , MAX(invoice_events.date_created) AS invoice_submitted_date
    FROM {{ source('qrp','invoice_events') }} invoice_events
    WHERE invoice_events.type = 'SUBMITTED'
    GROUP BY 1

)

, invoice_items AS (

    SELECT
        inv_adjustments.invoice_id
        , usr_proj_rates.user_id
        , usr.country AS user_country
        , usr_proj_rates.project_id
        , usr_proj_rates.locale_id
        , invoices.direct_pay_type
        , invoices.status AS invoice_status
        , invoices.start_date AS invoice_start_date
        , invoices.end_date AS invoice_end_date
        , invoices.date_created AS invoice_requested_date
        , invoices.date_updated AS invoice_date_updated
        , projects.customer_id AS client_id
        , projects.type AS project_type
        , inv_adjustments.description
        , projects.rate_type
        , COALESCE(inv_adjustments.units_of_work_completed, 0) AS units_of_work_completed
        , usr_proj_rates.effective_rate
        , COALESCE(CAST(
            DATEDIFF(MINUTE, inv_adjustments.start_date, inv_adjustments.end_date) AS NUMERIC(10, 2)
        ) / 60, 0) AS worked_hours
        , CASE
            WHEN projects.rate_type = 'HOURLY' THEN
                ROUND(worked_hours * usr_proj_rates.effective_rate, 2)
            WHEN projects.rate_type = 'PIECERATE' THEN
                ROUND(inv_adjustments.units_of_work_completed * usr_proj_rates.effective_rate, 2)
            ELSE
                0
        END AS amount_paid
    FROM {{ source('qrp','invoice_adjustments') }} AS inv_adjustments
    LEFT JOIN {{ source('qrp','exp_user_project_rates') }} AS usr_proj_rates
        ON inv_adjustments.user_project_rate_id = usr_proj_rates.id
    LEFT JOIN {{ source('qrp','exp_projects') }} AS projects
        ON usr_proj_rates.project_id = projects.id
    LEFT JOIN {{ source('qrp','invoices') }} AS invoices
        ON inv_adjustments.invoice_id = invoices.id
    LEFT JOIN {{ source('qrp','users') }} AS usr
        ON invoices.user_id = usr.id

)

, inv_spec_items AS (

    SELECT
        inv_spec_adjustments.invoice_id
        , invoices.user_id
        , usr.country AS user_country
        , inv_spec_adjustments.project_id
        , usr_proj_rates.locale_id
        , invoices.direct_pay_type
        , invoices.status AS invoice_status
        , invoices.start_date AS invoice_start_date
        , invoices.end_date AS invoice_end_date
        , invoices.date_created AS invoice_requested_date
        , invoices.date_updated AS invoice_date_updated
        , projects.customer_id AS client_id
        , projects.type AS project_type
        , inv_spec_adjustments.description
        , inv_spec_adjustments.type AS rate_type
        , NULL AS units_of_work_completed
        , inv_spec_adjustments.rate AS effective_rate
        , inv_spec_adjustments.millis / (1000 * 60 * 60) AS worked_hours
        , inv_spec_adjustments.dollar_amount AS amount_paid
    FROM {{ source('qrp','invoice_special_adjustments') }} AS inv_spec_adjustments
    LEFT JOIN {{ source('qrp','exp_projects') }} AS projects
        ON inv_spec_adjustments.project_id = projects.id
    LEFT JOIN {{ source('qrp','invoices') }} AS invoices
        ON inv_spec_adjustments.invoice_id = invoices.id
    LEFT JOIN {{ source('qrp','exp_user_project_rates') }} AS usr_proj_rates
        ON inv_spec_adjustments.user_project_rate_id = usr_proj_rates.id
    LEFT JOIN {{ source('qrp','users') }} AS usr
        ON invoices.user_id = usr.id

)

, fct_contributor_invoices AS (

    SELECT
        invoice_id
        , user_id
        , user_country
        , project_id
        , locale_id
        , direct_pay_type
        , client_id
        , invoice_status
        , invoice_start_date
        , invoice_end_date
        , invoice_requested_date
        , invoice_date_updated
        , project_type
        , description
        , rate_type
        , worked_hours
        , units_of_work_completed
        , effective_rate
        , amount_paid
    FROM invoice_items
    UNION ALL
    SELECT
        invoice_id
        , user_id
        , user_country
        , project_id
        , locale_id
        , direct_pay_type
        , client_id
        , invoice_status
        , invoice_start_date
        , invoice_end_date
        , invoice_requested_date
        , invoice_date_updated
        , project_type
        , description
        , rate_type
        , worked_hours
        , units_of_work_completed
        , effective_rate
        , amount_paid
    FROM inv_spec_items

)

SELECT
    fct_contributor_invoices.invoice_id
    , fct_contributor_invoices.user_id
    , fct_contributor_invoices.user_country
    , fct_contributor_invoices.project_id
    , fct_contributor_invoices.locale_id
    , fct_contributor_invoices.direct_pay_type
    , fct_contributor_invoices.client_id
    , fct_contributor_invoices.invoice_status
    , fct_contributor_invoices.invoice_start_date
    , fct_contributor_invoices.invoice_end_date
    , fct_contributor_invoices.invoice_requested_date
    , fct_contributor_invoices.invoice_date_updated
    , fct_contributor_invoices.project_type
    , fct_contributor_invoices.description
    , fct_contributor_invoices.rate_type
    , fct_contributor_invoices.worked_hours
    , fct_contributor_invoices.units_of_work_completed
    , fct_contributor_invoices.effective_rate
    , fct_contributor_invoices.amount_paid
    , DATEDIFF(
        DAY, fct_contributor_invoices.invoice_end_date, fct_contributor_invoices.invoice_date_updated
    ) AS time_to_pay
    , invoice_submitted_date.invoice_submitted_date
FROM fct_contributor_invoices
LEFT JOIN invoice_submitted_date
    ON invoice_submitted_date.invoice_id = fct_contributor_invoices.invoice_id

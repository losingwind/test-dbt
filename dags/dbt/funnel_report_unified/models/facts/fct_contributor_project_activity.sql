{{
    config(
        materialized='table',dist='user_id', sort=['user_id', 'project_id', 'locale_id']
        , tags=["fact"]
    )
}}

WITH users AS (

    SELECT *
    FROM {{ source('qrp', 'users') }}

)

, fct_contributor_invoices AS (

    SELECT
        user_id
        , project_id
        , locale_id
        , MIN((CASE WHEN
                    invoice_status = 'PAID'
                    THEN invoice_requested_date END)) AS first_invoiced_date
        , MAX((CASE WHEN
                    invoice_status = 'PAID'
                    THEN invoice_requested_date END)) AS max_invoiced_date
        , MAX((CASE WHEN
                        invoice_status = 'PAID'
                        THEN invoice_start_date END)) AS last_invoice
    FROM {{ source('invoicing','fct_contributor_invoices') }}
    GROUP BY 1, 2, 3

)

, last_invoice AS (
    SELECT
        e.user_id
        , e.project_id
        , MAX(e.invoice_start_date) AS invoice_start_date
    FROM {{ source('invoicing','fct_contributor_invoices') }} e
    GROUP BY 1, 2
    ORDER BY 1, 2, 3 DESC
)

, last_project AS (
    SELECT
        user_id
        , project_id
        , invoice_start_date
        , LAG(project_id, 1) OVER (ORDER BY invoice_start_date) AS last_project
        , LAG(invoice_start_date, 1) OVER (ORDER BY invoice_start_date) AS last_project_invoice
    FROM last_invoice ORDER BY invoice_start_date
)

, fct_contributor_project_activity AS (
    SELECT
        fci.user_id
        , fci.project_id
        , fci.locale_id
        , fci.max_invoiced_date
        , fci.first_invoiced_date
        , fci.last_invoice
        , lp.last_project
        , lp.last_project_invoice
        , CONVERT_TIMEZONE('UTC', 'US/Pacific', GETDATE()) AS edw_date_created
    FROM
        fct_contributor_invoices fci
    LEFT JOIN users u
        ON u.id = fci.user_id
    LEFT JOIN last_project lp
        ON fci.project_id = lp.project_id
            AND fci.user_id = lp.user_id
)

SELECT * FROM
    fct_contributor_project_activity
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9

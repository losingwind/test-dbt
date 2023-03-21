{{
    config(
        tags=['summary'],
        materialized='table',
    )
}}

WITH cp AS (

    SELECT *
    FROM {{ ref('fct_contributor_invoices') }}
    WHERE invoice_status = 'PAID'
        AND amount_paid IS NOT NULL

)

, um AS (

    SELECT
        id
        , status
        , termination_reason
    FROM {{ source('qrp','users') }}
    WHERE status = 'TERMINATED'
        AND termination_reason = 'MALICIOUSNESS'

)

, total_paid AS (

    SELECT
        project_id
        , project_type
        , client_id
        , TO_CHAR(invoice_start_date, 'Mon-YYYY') AS payment_month
        , COALESCE(SUM(amount_paid), 0) AS total_amount_paid
    FROM {{ ref('fct_contributor_invoices') }}
    WHERE invoice_status = 'PAID'
        AND amount_paid IS NOT NULL
    GROUP BY 1, 2, 3, 4

)

, contributor_payments_mly AS (
    SELECT
        cp.project_id
        , cp.project_type
        , cp.client_id
        , total_paid.total_amount_paid
        , TO_CHAR(cp.invoice_start_date, 'Mon-YYYY') AS payment_month
        , COUNT(um.id) AS malicious_worker_count
        , COALESCE(SUM(cp.amount_paid), 0) AS malicious_worker_amount_paid
    FROM cp
    INNER JOIN total_paid
        ON cp.project_id = total_paid.project_id
            AND cp.client_id = total_paid.client_id
            AND payment_month = total_paid.payment_month
    INNER JOIN um
        ON cp.user_id = um.id
    GROUP BY 1, 2, 3, 4, 5

)

SELECT *
FROM contributor_payments_mly

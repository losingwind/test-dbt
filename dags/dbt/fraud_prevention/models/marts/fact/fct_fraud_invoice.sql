{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

WITH fct_fraud_invoice AS (

    SELECT
        u.id AS contributor_id
        , i.status AS invoice_status
        , upi.payee_id AS payoneer_id
        , i.start_date AS invoice_date
        , i.amount_paid AS amount
    FROM
        {{ source('qrp', 'users') }} u
    LEFT JOIN {{ source('qrp', 'user_payoneer_ids') }} upi ON upi.user_id = u.id
    LEFT JOIN {{ source('qrp', 'invoices') }} i ON i.user_id = u.id
    WHERE
        i.status IN ('PAID')
        AND i.amount_paid IS NOT NULL
    UNION DISTINCT
    SELECT
        u.id AS contributor_id
        , i.status AS invoice_status
        , upi.payee_id AS payoneer_id
        , i.start_date AS invoice_date
        , i.amount_authorized AS amount
    FROM
        {{ source('qrp', 'users') }} u
    LEFT JOIN {{ source('qrp', 'user_payoneer_ids') }} upi ON upi.user_id = u.id
    LEFT JOIN {{ source('qrp', 'invoices') }} i ON i.user_id = u.id
    WHERE
        i.status IN ('APPROVED')
)

SELECT * FROM fct_fraud_invoice

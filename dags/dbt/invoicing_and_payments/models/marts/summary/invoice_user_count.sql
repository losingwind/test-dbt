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
        , i.locale_id
        , DATE_TRUNC('month', i.invoice_start_date) AS invoice_month
        , COUNT(DISTINCT i.user_id) AS invoice_count
    FROM {{ ref('fct_contributor_invoices') }} i
    INNER JOIN {{ source('qrp','users') }} u
        ON i.user_id = u.id
    LEFT JOIN {{ source('qrp','exp_projects') }} p
        ON p.id = i.project_id
    WHERE u.status NOT IN ('INTERNAL', 'PARTNER')
        AND invoice_month >= (CURRENT_TIMESTAMP - INTERVAL '3 month') -- noqa
        AND p.status = 'ENABLED'
    GROUP BY 1, 2, 3, 4

)

SELECT
    project_id
    , country
    , locale_id
    , AVG(invoice_count) AS invoice_count
FROM inv
GROUP BY 1, 2, 3

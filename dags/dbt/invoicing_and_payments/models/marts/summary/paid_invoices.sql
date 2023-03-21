{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

WITH inv AS (

    SELECT
        i.project_id
        , i.user_id
        , i.locale_id
        , i.invoice_requested_date AS date_requested
        , i.invoice_status AS status
        , i.rate_type AS value
        , SUM(i.worked_hours) + SUM(i.units_of_work_completed) AS quantity
        , SUM(i.amount_paid) AS invoice_amount
    FROM {{ ref('fct_contributor_invoices') }} i
    WHERE i.invoice_requested_date > CURRENT_DATE - INTERVAL '3 Months'
        AND i.invoice_status = 'PAID'
    GROUP BY 1, 2, 3, 4, 5, 6

)

SELECT
    user_list.contributor_id
    , inv.project_id
    , inv.locale_id
    , user_list.country
    , COUNT(inv.user_id) AS invoice_count
FROM {{ source('dim','dim_contributors') }} user_list
LEFT JOIN inv
    ON inv.user_id = user_list.contributor_id
LEFT JOIN {{ source('qrp','exp_projects') }} p
    ON inv.project_id = p.id
WHERE inv.project_id IS NOT NULL
GROUP BY 1, 2, 3, 4

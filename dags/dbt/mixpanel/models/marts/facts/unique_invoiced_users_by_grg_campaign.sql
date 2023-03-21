{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

SELECT
    invoices.invoice_id
    , invoices.project_id
    , invoices.user_id
    , CASE
        WHEN mixpanel.campaign_source = 'GRG' THEN 'GRG'
        ELSE 'NON GRG'
    END AS campaign_source
    , invoices.invoice_start_date AS invoice_month
FROM {{ source('invoicing','fct_contributor_invoices') }} AS invoices
INNER JOIN {{ source('qrp','users') }} AS users
    ON invoices.user_id = users.id
LEFT JOIN {{ ref('fct_contributors_verify_email') }} AS mixpanel
    ON mixpanel.mp_user_id = invoices.user_id
WHERE users.status NOT IN ('INTERNAL', 'PARTNER')
    AND invoices.invoice_requested_date >= '2022-01-01'
    AND invoices.project_id IN (
        1, 40, 87, 106, 108, 109, 142, 352, 610, 1060, 1373, 1822, 2022, 2224
        , 2334, 2478, 2723, 2804, 3572, 3653, 3767, 4340, 4754, 4755, 4763, 4931
        , 4963, 4988
    )
GROUP BY 1, 2, 3, 4, 5

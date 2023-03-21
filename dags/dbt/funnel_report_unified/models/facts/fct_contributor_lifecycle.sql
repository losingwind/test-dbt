{{
    config(
        materialized='table',dist='user_id', sort=['user_id', 'project_id', 'locale_id']
        , tags=["fact"]
    )
}}

WITH exp_user_project_mappings AS (

    SELECT
        id
        , date_created
        , project_id
        , locale_id
        , user_id
        , status
        , date_updated
        , application_date
        , screened_date
        , rejected_date
        , active_date
    FROM {{ source('qrp', 'exp_user_project_mappings') }}

)

, users AS (

    SELECT *
    FROM {{ source('qrp', 'users') }}

)

, country AS (

    SELECT
        country_2
        , country_full
    FROM {{ source('dim', 'dim_country') }}

)

, qualified AS (

    SELECT
        a.user_id
        , a.project_id
        , MAX((CASE WHEN
            a.status IN( 'ACTIVE')
            AND u.status IN( 'REACTIVATION_REQUESTED', 'ABANDONED', 'ACTIVE', 'CONTRACT_PENDING'
                , 'EXPIRED', 'EXPRESS_ACTIVE', 'EXPRESS_QUALIFYING', 'IN_ACTIVATION_QUEUE', 'PAYONEER_SETUP'
                , 'REJECTED', 'SUSPENDED', 'TERMINATED'
                , 'EXPRESS_TO_BE_CONVERTED', 'ARCHIVED', 'STAGED'
            )
            THEN a.date_created END)) AS qualified_active_date
        , MAX((CASE WHEN
            a.status IN( 'REVOKED')
            AND u.status IN( 'REACTIVATION_REQUESTED', 'ABANDONED', 'ACTIVE', 'CONTRACT_PENDING', 'EXPIRED'
                , 'EXPRESS_ACTIVE'
                , 'EXPRESS_QUALIFYING', 'IN_ACTIVATION_QUEUE', 'PAYONEER_SETUP', 'REJECTED', 'SUSPENDED', 'TERMINATED'
                , 'EXPRESS_TO_BE_CONVERTED', 'ARCHIVED', 'STAGED'
            )
            THEN a.date_created END)) AS qualified_revoked_date
    FROM {{ source('qrp', 'exp_user_project_access_status_changes') }} a
    INNER JOIN {{ source('qrp', 'users') }} u ON a.user_id = u.id
    WHERE
        u.status NOT IN( 'INTERNAL', 'PARTNER')
    GROUP BY 1, 2

)

, qualified_pm AS (

    SELECT
        a.user_id
        , a.project_id
        , MAX((CASE WHEN
            a.status IN( 'ACTIVE')
            AND u.status IN( 'REACTIVATION_REQUESTED', 'ABANDONED', 'ACTIVE', 'CONTRACT_PENDING', 'EXPIRED'
                , 'EXPRESS_ACTIVE', 'EXPRESS_QUALIFYING', 'IN_ACTIVATION_QUEUE', 'PAYONEER_SETUP', 'REJECTED'
                , 'SUSPENDED', 'TERMINATED', 'EXPRESS_TO_BE_CONVERTED', 'ARCHIVED', 'STAGED'
            )
            THEN a.date_created END)
        ) AS qualified_active_date
        , MAX((CASE
                WHEN a.status IN( 'REVOKED')
                    AND u.status IN( 'REACTIVATION_REQUESTED', 'ABANDONED', 'ACTIVE', 'CONTRACT_PENDING', 'EXPIRED'
                        , 'EXPRESS_ACTIVE', 'EXPRESS_QUALIFYING', 'IN_ACTIVATION_QUEUE', 'PAYONEER_SETUP', 'REJECTED'
                        , 'SUSPENDED', 'TERMINATED', 'EXPRESS_TO_BE_CONVERTED', 'ARCHIVED', 'STAGED'
                    )
                    THEN a.date_created
            END)
        ) AS qualified_revoked_date
    FROM {{ source('qrp', 'exp_user_project_mappings') }} a
    INNER JOIN {{ source('qrp', 'users') }} u ON a.user_id = u.id
    WHERE
        u.status NOT IN( 'INTERNAL', 'PARTNER')
    GROUP BY 1, 2
)

, application_received_dt AS (

    SELECT
        ac.user_id
        , ac.project_id
        , MAX(CASE
                WHEN ac.status IN( 'APPLICATION_RECEIVED')
                    THEN ac.date_created
            END) AS application_received_date
    FROM {{ source('qrp', 'exp_user_project_access_status_changes') }} ac
    INNER JOIN {{ source('qrp', 'users') }} u ON ac.user_id = u.id
    WHERE u.status NOT IN( 'INTERNAL', 'PARTNER')
    GROUP BY 1, 2

)

, screened_dt AS (

    SELECT
        ac.user_id
        , ac.project_id
        , MAX(CASE
                WHEN
                    ac.status IN( 'QUALIFYING', 'QUALIFYING_ON_HOLD')
                    THEN ac.date_created
            END) AS screened_date
    FROM {{ source('qrp', 'exp_user_project_access_status_changes') }} ac
    INNER JOIN {{ source('qrp', 'users') }} u ON ac.user_id = u.id
    WHERE u.status NOT IN( 'INTERNAL', 'PARTNER')
    GROUP BY 1, 2

)

, ready_to_contribute_pm AS (

    SELECT
        pm.user_id
        , pm.project_id
        , MAX(CASE
                WHEN ac.status IN( 'ACTIVE')
                    AND u.status IN(
                        'REACTIVATION_REQUESTED', 'ACTIVE', 'EXPIRED', 'EXPRESS_ACTIVE', 'SUSPENDED'
                        , 'TERMINATED', 'ARCHIVED'
                    )
                    THEN ac.date_created
            END) AS ready_to_contribute_active_date
        , MAX((CASE
                WHEN ac.status IN( 'REVOKED')
                    AND u.status IN(
                        'REACTIVATION_REQUESTED', 'ACTIVE', 'EXPIRED', 'EXPRESS_ACTIVE', 'SUSPENDED'
                        , 'TERMINATED', 'ARCHIVED'
                    )
                    THEN ac.date_created
            END)) AS ready_to_contribute_revoked_date
    FROM {{ source('qrp', 'exp_user_project_mappings') }} pm
    INNER JOIN {{ source('qrp', 'users') }} u ON pm.user_id = u.id
    INNER JOIN {{ source('qrp', 'exp_user_project_access_status_changes') }} ac
        ON pm.user_id = ac.user_id AND pm.project_id = ac.project_id
    WHERE u.status NOT IN( 'INTERNAL', 'PARTNER')
    GROUP BY 1, 2
)

, ready_to_contribute_status AS (

    SELECT
        ac.user_id
        , ac.project_id
        , MAX(CASE
                WHEN ac.status IN( 'ACTIVE') AND u.status IN(
                    'REACTIVATION_REQUESTED', 'ACTIVE', 'EXPIRED', 'EXPRESS_ACTIVE', 'SUSPENDED'
                    , 'TERMINATED', 'ARCHIVED'
                )
                THEN ac.date_created
            END) AS ready_to_contribute_active_date
        , MAX((CASE
                WHEN ac.status IN( 'REVOKED') AND u.status IN(
                    'REACTIVATION_REQUESTED', 'ACTIVE', 'EXPIRED', 'EXPRESS_ACTIVE', 'SUSPENDED'
                    , 'TERMINATED', 'ARCHIVED'
                )
                THEN ac.date_created
            END)) AS ready_to_contribute_revoked_date
    FROM {{ source('qrp', 'exp_user_project_access_status_changes') }} ac
    INNER JOIN {{ source('qrp', 'users') }} u ON ac.user_id = u.id
    WHERE u.status NOT IN( 'INTERNAL', 'PARTNER')
    GROUP BY 1, 2


)

, fct_contributor_invoices_paid_status AS (

    SELECT
        user_id
        , project_id
        , locale_id
        , MAX((CASE WHEN invoice_status = 'PAID'
                         THEN invoice_requested_date END)
        ) AS invoice_requested_date
        , MIN(invoice_requested_date) AS first_invoice
    FROM {{ source( 'invoicing', 'fct_contributor_invoices') }} WHERE invoice_status = 'PAID'
    GROUP BY 1, 2, 3

)

, fct_contributor_invoices_fci AS (

    SELECT
        user_id
        , project_id
        , MAX((CASE WHEN invoice_status = 'PAID'
                    THEN invoice_requested_date END)

        ) AS invoice_requested_date
        , MIN((CASE WHEN invoice_status = 'PAID'
                    THEN invoice_requested_date END)
        ) AS first_invoiced_date
        , MAX((CASE WHEN invoice_status = 'PAID'
                    THEN invoice_requested_date END)
        ) AS max_invoiced_date
        , MAX((CASE WHEN invoice_status = 'PAID'
                        THEN invoice_start_date END)
        ) AS last_invoice
    FROM {{ source('invoicing', 'fct_contributor_invoices') }}
    GROUP BY 1, 2

)

, user_activity_log_records AS (

    SELECT
        user_id
        , MAX(date_created) AS last_login_date
    FROM {{ source('qrp', 'user_activity_log_records') }}
    GROUP BY 1


)

, fct_funnel_report_flow_detailed_status AS (

    SELECT
        DATE_TRUNC('day', exp_user_project_mappings.date_created) AS registration_date
        , exp_user_project_mappings.locale_id
        , exp_user_project_mappings.project_id
        , country.country_full AS country
        , exp_user_project_mappings.user_id
        , users.status AS user_status
        , exp_user_project_mappings.status AS user_project_status
        , COUNT(DISTINCT CASE WHEN
                    exp_user_project_mappings.status IN( 'ACTIVE', 'ABANDONED', 'NEED_REVIEW', 'APPLICATION_RECEIVED'
                        , 'PRESCREENING_IP', 'BLOCKED', 'REGISTERED', 'REGISTERED_ON_HOLD', 'REJECTED', 'REVOKED'
                        , 'STAGED', 'QUALIFYING', 'QUALIFYING_ON_HOLD', 'EXAM_FAILED', 'EXAM_SCHEDULED', 'EXAM_STARTED'
                    )
                    AND users.status IN( 'REACTIVATION_REQUESTED', 'ABANDONED', 'ACTIVE', 'CONTRACT_PENDING', 'EXPIRED'
                        , 'EXPRESS_ACTIVE', 'EXPRESS_QUALIFYING', 'IN_ACTIVATION_QUEUE', 'ON_HOLD', 'PAYONEER_SETUP'
                        , 'REJECTED', 'SUSPENDED', 'TERMINATED', 'EXPRESS_TO_BE_CONVERTED', 'ARCHIVED', 'STAGED'
                    )
                    THEN exp_user_project_mappings.user_id END
        ) AS project_registered
        , COUNT(DISTINCT CASE WHEN
            exp_user_project_mappings.status IN( 'ACTIVE', 'ABANDONED', 'NEED_REVIEW', 'APPLICATION_RECEIVED'
                , 'PRESCREENING_IP'
                , 'BLOCKED', 'REJECTED', 'REVOKED', 'STAGED', 'QUALIFYING', 'QUALIFYING_ON_HOLD', 'EXAM_FAILED'
                , 'EXAM_SCHEDULED'
                , 'EXAM_STARTED'
            )
            AND users.status IN( 'REACTIVATION_REQUESTED', 'ABANDONED', 'ACTIVE', 'CONTRACT_PENDING', 'EXPIRED'
                , 'EXPRESS_ACTIVE'
                , 'EXPRESS_QUALIFYING', 'IN_ACTIVATION_QUEUE', 'PAYONEER_SETUP', 'REJECTED', 'SUSPENDED', 'TERMINATED'
                , 'EXPRESS_TO_BE_CONVERTED', 'ARCHIVED', 'STAGED'
            )
            THEN exp_user_project_mappings.user_id END
        ) AS application_received

        , COUNT(DISTINCT CASE WHEN
            exp_user_project_mappings.status IN( 'ACTIVE', 'REVOKED', 'NEED_REVIEW', 'QUALIFYING', 'QUALIFYING_ON_HOLD'
                , 'EXAM_FAILED', 'EXAM_SCHEDULED', 'EXAM_STARTED', 'ABANDONED')
            AND users.status IN( 'REACTIVATION_REQUESTED', 'ABANDONED', 'ACTIVE', 'CONTRACT_PENDING', 'EXPIRED'
                , 'EXPRESS_ACTIVE', 'EXPRESS_QUALIFYING', 'IN_ACTIVATION_QUEUE', 'PAYONEER_SETUP', 'REJECTED'
                , 'SUSPENDED', 'TERMINATED', 'EXPRESS_TO_BE_CONVERTED', 'ARCHIVED', 'STAGED'
            )
            THEN exp_user_project_mappings.user_id END
        ) AS screened

        , COUNT(DISTINCT CASE WHEN
            exp_user_project_mappings.active_date IS NOT NULL
            AND exp_user_project_mappings.status IN( 'REVOKED')
            AND users.status IN( 'REACTIVATION_REQUESTED', 'ABANDONED', 'ACTIVE', 'CONTRACT_PENDING', 'EXPIRED'
                , 'EXPRESS_ACTIVE'
                , 'EXPRESS_QUALIFYING'
                , 'IN_ACTIVATION_QUEUE'
                , 'PAYONEER_SETUP'
                , 'REJECTED'
                , 'SUSPENDED'
                , 'TERMINATED'
                , 'EXPRESS_TO_BE_CONVERTED', 'ARCHIVED', 'STAGED'
            )
            THEN exp_user_project_mappings.user_id END
        ) AS qualified_revoked

        , COUNT(DISTINCT CASE WHEN
            exp_user_project_mappings.status IN( 'ACTIVE')
            AND users.status IN( 'REACTIVATION_REQUESTED', 'ABANDONED', 'ACTIVE', 'CONTRACT_PENDING', 'EXPIRED'
                , 'EXPRESS_ACTIVE', 'EXPRESS_QUALIFYING', 'IN_ACTIVATION_QUEUE', 'PAYONEER_SETUP', 'REJECTED'
                , 'SUSPENDED', 'TERMINATED', 'EXPRESS_TO_BE_CONVERTED', 'ARCHIVED', 'STAGED'
            )
            THEN exp_user_project_mappings.user_id END
        ) AS qualified_activeonly

        , COUNT(DISTINCT CASE WHEN
            exp_user_project_mappings.active_date IS NOT NULL
            AND exp_user_project_mappings.status IN( 'REVOKED')
            AND users.status IN( 'REACTIVATION_REQUESTED', 'ACTIVE', 'EXPIRED', 'EXPRESS_ACTIVE', 'SUSPENDED'
                , 'TERMINATED', 'ARCHIVED'
            )
            THEN exp_user_project_mappings.user_id END
        ) AS ready_to_contribute_revoked

        , COUNT(DISTINCT CASE WHEN
            exp_user_project_mappings.status IN( 'ACTIVE')
            AND users.status IN( 'REACTIVATION_REQUESTED', 'ACTIVE', 'EXPIRED', 'EXPRESS_ACTIVE', 'SUSPENDED'
                , 'TERMINATED', 'ARCHIVED'
            )
            THEN exp_user_project_mappings.user_id END
        ) AS ready_to_contribute_active

        , COUNT(DISTINCT CASE WHEN
            fci.invoice_requested_date IS NOT NULL
            THEN exp_user_project_mappings.user_id END
        ) AS invoiced

    FROM exp_user_project_mappings
    INNER JOIN users
        ON exp_user_project_mappings.user_id = users.id
    INNER JOIN country
        ON users.country = country.country_2
    LEFT JOIN qualified
        ON exp_user_project_mappings.user_id = qualified.user_id
            AND exp_user_project_mappings.project_id = qualified.project_id
    LEFT JOIN ready_to_contribute_pm
        ON exp_user_project_mappings.user_id = ready_to_contribute_pm.user_id
            AND exp_user_project_mappings.project_id = ready_to_contribute_pm.project_id
    LEFT JOIN fct_contributor_invoices_fci fci
        ON exp_user_project_mappings.user_id = fci.user_id
            AND exp_user_project_mappings.project_id = fci.project_id
    WHERE users.status NOT IN( 'INTERNAL', 'PARTNER')
    GROUP BY 1, 2, 3, 4, 5, 6, 7
    ORDER BY 4

)

, actv AS (

    SELECT
        f.user_id
        , f.project_id
        , MAX(f.date_created) AS active_date
    FROM {{ source('qrp', 'exp_user_project_access_status_changes') }} f
    LEFT JOIN {{ source('dim', 'dim_projects') }} p ON p.project_id = f.project_id
    WHERE f.status = 'ACTIVE'
    GROUP BY 1, 2

)

, fct_contributor_lifecycle AS (

    SELECT
        exp_user_project_mappings.user_id
        , exp_user_project_mappings.locale_id
        , exp_user_project_mappings.project_id
        , DATE_TRUNC('DAY', exp_user_project_mappings.date_created) AS registration_date
        , country.country_2 AS country
        , users.status AS user_status
        , COALESCE(actv.active_date, exp_user_project_mappings.active_date) AS active_date
        , exp_user_project_mappings.status AS user_project_status
        , exp_user_project_mappings.date_updated AS project_status_date_updated
        , exp_user_project_mappings.date_created AS project_registered_date
        , ff.project_registered
        , ff.application_received
        , ff.invoiced
        , ff.screened
        , l.last_login_date
        , ff.ready_to_contribute_revoked + ff.ready_to_contribute_active AS ready_to_contribute
        , DATEDIFF( DAY, l.last_login_date, CURRENT_DATE) AS days_to_last_login
        , DATEDIFF( DAY, exp_user_project_mappings.date_updated, CURRENT_DATE) AS days_to_project_status_date_updated
        , ff.qualified_revoked + ff.qualified_activeonly AS qualified
        , (COALESCE(exp_user_project_mappings.application_date, application_received_dt.application_received_date)
        ) AS application_received_date
        , (COALESCE(exp_user_project_mappings.screened_date, screened_dt.screened_date)
        ) AS screened_date
        , COALESCE(qualified.qualified_active_date, qualified_pm.qualified_active_date) AS qualified_active_date
        , COALESCE(qualified.qualified_revoked_date, qualified_pm.qualified_revoked_date )AS qualified_revoked_date
        , COALESCE(ready_to_contribute_status.ready_to_contribute_active_date
            , ready_to_contribute_pm.ready_to_contribute_active_date
        ) AS ready_to_contribute_active_date
        , COALESCE(ready_to_contribute_status.ready_to_contribute_revoked_date
            , ready_to_contribute_pm.ready_to_contribute_revoked_date
        )
        AS ready_to_contribute_revoked_date
        , fct_contributor_invoices_paid_status.invoice_requested_date AS invoiced_date
        , fct_contributor_invoices_paid_status.first_invoice
        , CONVERT_TIMEZONE('UTC', 'US/Pacific', GETDATE()) AS edw_date_created
    FROM {{ source('qrp', 'exp_user_project_mappings') }} exp_user_project_mappings
    INNER JOIN users
        ON exp_user_project_mappings.user_id = users.id
    INNER JOIN country
        ON UPPER(users.country) = UPPER(country.country_2)
    LEFT JOIN application_received_dt
        ON exp_user_project_mappings.user_id = application_received_dt.user_id
            AND exp_user_project_mappings.project_id = application_received_dt.project_id
    LEFT JOIN screened_dt
        ON exp_user_project_mappings.user_id = screened_dt.user_id
            AND exp_user_project_mappings.project_id = screened_dt.project_id
    LEFT JOIN qualified
        ON exp_user_project_mappings.user_id = qualified.user_id
            AND exp_user_project_mappings.project_id = qualified.project_id
    LEFT JOIN qualified_pm
        ON exp_user_project_mappings.user_id = qualified_pm.user_id
            AND exp_user_project_mappings.project_id = qualified_pm.project_id
    LEFT JOIN ready_to_contribute_status
        ON exp_user_project_mappings.user_id = ready_to_contribute_status.user_id
            AND exp_user_project_mappings.project_id = ready_to_contribute_status.project_id
    LEFT JOIN ready_to_contribute_pm
        ON exp_user_project_mappings.user_id = ready_to_contribute_pm.user_id
            AND exp_user_project_mappings.project_id = ready_to_contribute_pm.project_id
    LEFT JOIN fct_contributor_invoices_paid_status
        ON exp_user_project_mappings.user_id = fct_contributor_invoices_paid_status.user_id
            AND exp_user_project_mappings.project_id = fct_contributor_invoices_paid_status.project_id
            AND exp_user_project_mappings.locale_id = fct_contributor_invoices_paid_status.locale_id
    LEFT JOIN fct_funnel_report_flow_detailed_status ff ON ff.user_id = exp_user_project_mappings.user_id
            AND exp_user_project_mappings.project_id = ff.project_id
    LEFT JOIN user_activity_log_records l
        ON ff.user_id = l.user_id
    LEFT JOIN {{ source('dim', 'dim_projects') }} dp
        ON exp_user_project_mappings.project_id = dp.project_id
    LEFT JOIN actv
        ON actv.user_id = ff.user_id AND actv.project_id = ff.project_id
    WHERE users.status NOT IN( 'INTERNAL', 'PARTNER')
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28
)

SELECT *
FROM fct_contributor_lifecycle

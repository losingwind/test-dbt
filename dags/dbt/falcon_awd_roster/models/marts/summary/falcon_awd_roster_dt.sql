{{
    config(
        materialized='table',
        tags=["summary"]
    )
}}

WITH partner_invoice_data AS (

    SELECT
        user_id
        , external_client_user_id
        , project_id
        , completion_date
        , time_worked_milliseconds
    FROM {{ source('qrp','partner_invoice_data') }}
    WHERE report_date > '2021-09-05'

)

, projec_mapping AS (

    SELECT
        user_id
        , project_id
        , locale_id
        , status
        , active_date
    FROM {{ source('qrp','exp_user_project_mappings') }}
    WHERE status NOT IN ('REJECTED', 'QUALIFYING', 'APPLICATION_RECEIVED')

)

, most_recent_start_week AS (

    SELECT
        user_id
        , project_id
        , DATE_TRUNC('week', MAX(completion_date) + 2) - 2 AS latest_week_start_date
    FROM partner_invoice_data
    GROUP BY 1, 2

)

, most_recent_week_hrs AS (

    SELECT
        partner_invoice_data.user_id
        , partner_invoice_data.project_id
        , most_recent_start_week.latest_week_start_date
        , CAST(
            SUM(partner_invoice_data.time_worked_milliseconds) AS FLOAT
        ) / CAST(3600000 AS FLOAT) AS latest_week_work_hours
    FROM partner_invoice_data
    INNER JOIN most_recent_start_week
        ON partner_invoice_data.user_id = most_recent_start_week.user_id
            AND partner_invoice_data.project_id = most_recent_start_week.project_id
            AND partner_invoice_data.completion_date >= most_recent_start_week.latest_week_start_date
            AND partner_invoice_data.completion_date < DATEADD(
                DAYS, 7, most_recent_start_week.latest_week_start_date
            )
    GROUP BY 1, 2, 3

)

, total_hrs AS (

    SELECT
        user_id
        , project_id
        , external_client_user_id
        , CAST(SUM(time_worked_milliseconds) AS FLOAT) / CAST(3600000 AS FLOAT) AS total_work_hours
    FROM partner_invoice_data
    GROUP BY 1, 2, 3

)

, falcon_awd_roster_dt AS (

    SELECT
        projec_mapping.user_id
        , CONCAT('vendora_', pub_key_usr.id) AS oidc
        , usr.email
        , usr.country AS user_country
        , usr.primary_language AS user_primary_language
        , total_hrs.external_client_user_id
        , projec_mapping.project_id
        , locale.country AS project_country
        , locale.language AS project_language
        , projects.status AS project_status
        , ep.customer_id
        , projec_mapping.status AS user_project_status
        , projec_mapping.active_date AS user_project_active_date
        , ep.is_awd
        , ep.vertical
        , most_recent_week_hrs.latest_week_start_date
        , most_recent_week_hrs.latest_week_work_hours
        , total_hrs.total_work_hours
    FROM {{ source('dim','dim_proj_internotes') }} AS ep
    INNER JOIN projec_mapping
        ON ep.id = projec_mapping.project_id
    INNER JOIN {{ source('qrp','users') }} AS usr
        ON projec_mapping.user_id = usr.id
    INNER JOIN most_recent_week_hrs
        ON projec_mapping.project_id = most_recent_week_hrs.project_id
            AND projec_mapping.user_id = most_recent_week_hrs.user_id
    INNER JOIN total_hrs
        ON projec_mapping.project_id = total_hrs.project_id
            AND projec_mapping.user_id = total_hrs.user_id
    LEFT JOIN {{ source('qrp','exp_locales') }} AS locale
        ON projec_mapping.locale_id = locale.id
    LEFT JOIN {{ source('pub_keycloak','user_entity') }} AS pub_key_usr
        ON usr.email = pub_key_usr.email
            AND pub_key_usr.email IS NOT NULL
    LEFT JOIN {{ source('qrp','exp_projects') }} AS projects
        ON projec_mapping.project_id = projects.id
    WHERE ep.customer_id IN (14, 43, 45, 46, 50, 62, 209, 208, 210, 211, 220, 233, 240
        , 243, 265, 293, 306, 320, 332, 335, 341, 354, 440, 472)

)

SELECT *
FROM falcon_awd_roster_dt

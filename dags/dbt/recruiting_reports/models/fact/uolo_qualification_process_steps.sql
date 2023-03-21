{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

WITH user_signatures AS (

    SELECT
        user_id
        , document_id
        , COUNT(*) AS signatures_count
    FROM {{ source('qrp','esign_signatures') }}
    GROUP BY 1, 2

)

SELECT
    usr_process_steps.user_process_id
    , usr_process_steps.process_step_id
    , usr_process_steps.user_id
    , usr_proj_map.project_id
    , usr_proj_map.locale_id
    , usr_process_steps.date_created
    , usr_process_steps.date_updated
    , steps.name AS step_name
    , step_attributes.value AS step_attribute
    , COALESCE(user_signatures.signatures_count, 0) AS signatures_count
    , CASE
        WHEN user_signatures.signatures_count IS NULL THEN
            'NOT_SIGNED'
        WHEN user_signatures.signatures_count = 1 THEN
            'SIGNED'
        WHEN user_signatures.signatures_count > 1 THEN
            'COSIGNED'
    END AS signature_result
    , usr_process_steps.type AS process_type
    , usr_process_steps."order" AS step_order
    , usr_process_steps.status AS step_status
    , usr_processes.status AS process_status
    , LAG(usr_process_steps.date_updated, 1) OVER(
        PARTITION BY usr_process_steps.user_id
        ORDER BY usr_process_steps.date_created, usr_process_steps."order"
    ) AS previous_update_date
    , CASE
        WHEN usr_process_steps."order" = 1 AND usr_process_steps.status = 'COMPLETE' THEN
            DATEDIFF(SECONDS, usr_process_steps.date_created, usr_process_steps.date_updated)
        WHEN usr_process_steps."order" > 1 AND usr_process_steps.status = 'COMPLETE' THEN
            DATEDIFF(SECONDS, previous_update_date, usr_process_steps.date_updated)
    END AS step_duration_seconds
FROM {{ source('qrp','bpm_user_process_steps') }} usr_process_steps
INNER JOIN {{ source('qrp','bpm_user_processes') }} usr_processes
    ON usr_process_steps.user_process_id = usr_processes.id
LEFT JOIN {{ source('qrp','bpm_steps') }} steps
    ON usr_process_steps.step_id = steps.id
LEFT JOIN {{ source('qrp','exp_user_project_mappings') }} usr_proj_map
    ON usr_proj_map.user_id = usr_process_steps.user_id
        AND usr_proj_map.user_qualification_process_id = usr_process_steps.user_process_id
LEFT JOIN {{ source('qrp','bpm_user_process_step_attributes') }} step_attributes
    ON step_attributes.process_step_id = usr_process_steps.process_step_id
        AND step_attributes.user_process_id = usr_process_steps.user_process_id
        AND step_attributes.name = 'Electronic Document'
LEFT JOIN user_signatures
    ON usr_process_steps.user_id = user_signatures.user_id
        AND step_attributes.value = user_signatures.document_id
WHERE usr_process_steps.type = 'QUALIFICATION_PROJECT'
    AND usr_proj_map.project_id = 4931

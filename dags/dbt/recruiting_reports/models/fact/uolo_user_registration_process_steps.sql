{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

WITH usr_registration_steps AS (

    SELECT
        usr_process_steps.user_process_id
        , usr_process_steps.process_id
        , usr_process_steps.process_step_id
        , usr_process_steps.user_id
        , usr_proj_map.project_id
        , usr_proj_map.locale_id
        , usr_process_steps.date_created
        , usr_process_steps.date_updated
        , steps.name AS step_name
        , step_attributes.name
        , CASE
            WHEN step_attributes.name = 'Locale Quiz Mapping' THEN
                JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(step_attributes.value, 0, TRUE), 'quizId')
        END AS quiz_id_parsed
        , usr_process_steps.type AS process_type
        , usr_process_steps."order" AS step_order
        , usr_process_steps.status AS step_status
        , LAG(usr_process_steps.date_updated, 1) OVER(
            PARTITION BY usr_process_steps.user_id, usr_process_steps.user_process_id
            ORDER BY usr_process_steps.date_created, usr_process_steps."order"
        ) AS previous_update_date
        , CASE
            WHEN usr_process_steps."order" = 1 AND usr_process_steps.status = 'COMPLETE' THEN
                DATEDIFF(SECONDS, usr_process_steps.date_created, usr_process_steps.date_updated)
            WHEN usr_process_steps."order" > 1 AND usr_process_steps.status = 'COMPLETE' THEN
                DATEDIFF(SECONDS, previous_update_date, usr_process_steps.date_updated)
        END AS step_duration_seconds
    FROM {{ source('qrp','bpm_user_process_steps') }} usr_process_steps
    LEFT JOIN {{ source('qrp','bpm_process_steps') }} process_steps
        ON usr_process_steps.process_step_id = process_steps.id
    LEFT JOIN {{ source('qrp','bpm_steps') }} steps
        ON usr_process_steps.step_id = steps.id
    LEFT JOIN {{ source('qrp','exp_user_project_mappings') }} usr_proj_map
        ON usr_proj_map.user_id = usr_process_steps.user_id
            AND usr_proj_map.user_registration_process_id = usr_process_steps.user_process_id
    LEFT JOIN {{ source('qrp','bpm_user_process_step_attributes') }} step_attributes
        ON step_attributes.process_step_id = usr_process_steps.process_step_id
            AND step_attributes.user_process_id = usr_process_steps.user_process_id
            AND step_attributes.name = 'Locale Quiz Mapping'
    WHERE usr_process_steps.type = 'REGISTRATION_PROJECT'
        AND usr_proj_map.project_id = 4931

)

, latest_quiz_attempt AS (

    SELECT
        date_created
        , quiz_id
        , user_id
        , result
        , score
        , attempt_count
    FROM (
        SELECT
            quiz_id
            , user_id
            , result
            , date_created
            , score
            , ROW_NUMBER() OVER (
                PARTITION BY quiz_id, user_id
                ORDER BY date_created
            ) AS attempt_count
            , ROW_NUMBER() OVER(
                PARTITION BY user_id, quiz_id
                ORDER BY date_created DESC
            ) AS quiz_count
        FROM {{ source('qrp','quiz_trackers') }}
    ) quiz_ordered
    WHERE quiz_ordered.quiz_id IN
        (
            SELECT DISTINCT quiz_id_parsed
            FROM usr_registration_steps
        )
        AND quiz_ordered.quiz_count = 1

)

SELECT
    usr_registration_steps.user_process_id
    , usr_registration_steps.process_id
    , usr_registration_steps.process_step_id
    , usr_registration_steps.user_id
    , usr_registration_steps.project_id
    , usr_registration_steps.locale_id
    , usr_registration_steps.date_created
    , usr_registration_steps.date_updated
    , usr_registration_steps.step_name
    , usr_registration_steps.step_order
    , usr_registration_steps.quiz_id_parsed
    , latest_quiz_attempt.result AS quiz_result
    , latest_quiz_attempt.score AS quiz_score
    , latest_quiz_attempt.attempt_count
    , usr_registration_steps.process_type
    , usr_registration_steps.step_status
    , usr_registration_steps.step_duration_seconds
FROM usr_registration_steps
LEFT JOIN latest_quiz_attempt
    ON usr_registration_steps.user_id = latest_quiz_attempt.user_id
        AND usr_registration_steps.quiz_id_parsed = latest_quiz_attempt.quiz_id
        AND usr_registration_steps.name = 'Locale Quiz Mapping'
        AND usr_registration_steps.quiz_id_parsed IS NOT NULL

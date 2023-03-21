{{
    config(
        materialized='table'
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
        , scheduled_exam_date
    FROM
        {{ source('qrp', 'exp_user_project_mappings') }}

)

, user_status_changes AS (
    SELECT
        id
        , user_id
        , date_created
        , type
    FROM
        {{ source('qrp', 'user_status_changes') }}

)

, users AS (
    SELECT
        id
        , country
        , status
    FROM
        {{ source('qrp', 'users') }}

)

, country AS (
    SELECT
        country_2
        , country_full
    FROM
        {{ source('dim', 'dim_country') }}

)

, user_activity_log_records AS (
    SELECT
        a.user_id
        , MAX(a.date_created) AS max_login_date
    FROM
        {{ source('qrp', 'user_activity_log_records') }} a
    WHERE
        a.login_result IN('SUCCESS')
    GROUP BY 1

)

, qualified AS (
    SELECT
        a.user_id
        , a.project_id
        , MAX(a.date_created) AS qualified_date
    FROM
        {{ source('qrp', 'exp_user_project_access_status_changes') }} a
    INNER JOIN users u
        ON a.user_id = u.id
    WHERE
        a.status IN('ACTIVE')
        AND u.status IN('CONTRACT_PENDING', 'EXPRESS_QUALIFYING'
            , 'IN_ACTIVATION_QUEUE'
            , 'PAYONEER_SETUP'
            , 'REJECTED'
        )
    GROUP BY
        1, 2

)

, ready_to_deploy AS (
    SELECT
        pm.user_id
        , pm.project_id
        , MAX(a.date_created) AS ready_to_deploy_date
    FROM
        {{ source('qrp', 'exp_user_project_access_status_changes') }} a
    INNER JOIN users u
        ON a.user_id = u.id
    LEFT JOIN user_status_changes uc
        ON uc.user_id = a.user_id
    FULL JOIN exp_user_project_mappings pm
        ON a.user_id = pm.user_id AND a.project_id = pm.project_id
    WHERE ( a.status IN('ACTIVE')
            OR pm.status IN('ACTIVE')
        )
        AND ( uc.type IN('ACTIVE')
            OR u.status IN('ACTIVE', 'EXPRESS_ACTIVE')
        )
    GROUP BY
        1, 2

)

, snapshot_contributor_stage AS (
    SELECT
        pm.locale_id
        , pm.project_id
        , pm.user_id
        , DATE_TRUNC('day', pm.date_created) AS registration_date
        , country.country_2 AS country
        , COUNT(DISTINCT
            CASE
                WHEN
                    pm.status IN
                    (
                        'REGISTERED'
                        , 'REGISTERED_ON_HOLD'
                    )
                    AND users.status IN
                    (
                        'ACTIVE'
                        , 'CONTRACT_PENDING'
                        , 'EXPRESS_ACTIVE'
                        , 'EXPRESS_QUALIFYING'
                        , 'IN_ACTIVATION_QUEUE'
                        , 'ON_HOLD'
                        , 'PAYONEER_SETUP'
                        , 'REJECTED'
                    )
                    THEN
                    pm.user_id
            END
        ) AS project_registered
        , COUNT(DISTINCT
            CASE
                WHEN
                    pm.status IN
                    (
                        'APPLICATION_RECEIVED', 'PRESCREENING_IP', 'STAGED'
                    )
                    AND users.status IN
                    (
                        'ACTIVE', 'CONTRACT_PENDING', 'EXPRESS_ACTIVE', 'EXPRESS_QUALIFYING'
                        , 'IN_ACTIVATION_QUEUE', 'PAYONEER_SETUP', 'REJECTED'
                    )
                    THEN
                    pm.user_id
            END
        ) AS application_received
        , COUNT(DISTINCT
            CASE
                WHEN
                    pm.status IN
                    (
                        'QUALIFYING', 'QUALIFYING_ON_HOLD', 'EXAM_SCHEDULED', 'EXAM_STARTED', 'NEED_REVIEW'
                    )
                    AND users.status IN
                    (
                        'ACTIVE', 'CONTRACT_PENDING', 'EXPRESS_ACTIVE', 'EXPRESS_QUALIFYING'
                        , 'IN_ACTIVATION_QUEUE', 'PAYONEER_SETUP', 'REJECTED'
                    )
                    THEN
                    pm.user_id
            END
        ) AS screened
        , COUNT(DISTINCT
            CASE
                WHEN
                    pm.status IN
                    (
                        'ACTIVE'
                    )
                    AND users.status IN
                    (
                        'CONTRACT_PENDING', 'EXPRESS_QUALIFYING', 'IN_ACTIVATION_QUEUE', 'PAYONEER_SETUP', 'REJECTED'
                    )
                    THEN
                    pm.user_id
            END
        ) AS qualified
        , COUNT(DISTINCT
            CASE
                WHEN
                    pm.status IN
                    (
                        'ACTIVE'
                    )
                    AND users.status IN
                    (
                        'ACTIVE', 'EXPRESS_ACTIVE'
                    )
                    THEN
                    pm.user_id
            END
        ) AS ready_to_contribute
    FROM
        exp_user_project_mappings pm
    INNER JOIN
        users
        ON pm.user_id = users.id
    INNER JOIN
        country
        ON UPPER(users.country) = UPPER(country.country_2)
    WHERE
        users.status NOT IN
        (
            'INTERNAL', 'PARTNER'
        )
    GROUP BY
        1, 2, 3, 4, 5
)

, fct_snapshot_contributor_lifecycle AS (
    SELECT
        exp_user_project_mappings.user_id
        , exp_user_project_mappings.locale_id
        , exp_user_project_mappings.project_id
        , DATE_TRUNC('DAY', exp_user_project_mappings.date_created) AS registration_date
        , cntry.country_2 AS country
        , u.status AS user_status
        , exp_user_project_mappings.status AS user_project_status
        , exp_user_project_mappings.date_updated AS project_status_date_updated
        , (CASE
            WHEN
                exp_user_project_mappings.status IN( 'REGISTERED', 'REGISTERED_ON_HOLD')
                AND u.status IN
                (
                    'ACTIVE'
                    , 'CONTRACT_PENDING'
                    , 'EXPRESS_ACTIVE'
                    , 'EXPRESS_QUALIFYING'
                    , 'IN_ACTIVATION_QUEUE'
                    , 'ON_HOLD'
                    , 'PAYONEER_SETUP'
                    , 'REJECTED'
                )
                THEN
                exp_user_project_mappings.date_created
            END) AS project_registered_date
        , (CASE
            WHEN
                exp_user_project_mappings.status IN
                (
                    'APPLICATION_RECEIVED', 'PRESCREENING_IP', 'STAGED'
                )
                AND u.status IN
                (
                    'ACTIVE', 'CONTRACT_PENDING', 'EXPRESS_ACTIVE', 'EXPRESS_QUALIFYING', 'IN_ACTIVATION_QUEUE'
                    , 'PAYONEER_SETUP', 'REJECTED'
                )
                THEN
                exp_user_project_mappings.application_date
            END) AS application_received_date
        , (CASE
            WHEN
                exp_user_project_mappings.status IN(
                    'QUALIFYING', 'QUALIFYING_ON_HOLD', 'EXAM_SCHEDULED', 'EXAM_STARTED', 'NEED_REVIEW'
                )
                AND u.status IN( 'ACTIVE', 'CONTRACT_PENDING', 'EXPRESS_ACTIVE', 'EXPRESS_QUALIFYING'
                    , 'IN_ACTIVATION_QUEUE', 'PAYONEER_SETUP', 'REJECTED'
                )
                THEN
                exp_user_project_mappings.screened_date
            END) AS screened_date
        , qualified.qualified_date AS qualified_date
        , ready_to_deploy.ready_to_deploy_date AS ready_to_deploy_date
        , user_activity_log_records.max_login_date AS last_login_date
        , scs.project_registered
        , scs.application_received
        , scs.screened
        , scs.qualified
        , scs.ready_to_contribute
        , CONVERT_TIMEZONE('UTC', 'US/Pacific', GETDATE()) AS edw_date_created
    FROM
        {{ source('qrp', 'exp_user_project_mappings') }} exp_user_project_mappings
    INNER JOIN users u
        ON exp_user_project_mappings.user_id = u.id
    INNER JOIN country cntry
        ON UPPER(u.country) = UPPER(cntry.country_2)
    FULL JOIN qualified
        ON exp_user_project_mappings.user_id = qualified.user_id
            AND exp_user_project_mappings.project_id = qualified.project_id
    FULL JOIN ready_to_deploy
        ON exp_user_project_mappings.user_id = ready_to_deploy.user_id
            AND exp_user_project_mappings.project_id = ready_to_deploy.project_id
    LEFT JOIN user_activity_log_records
        ON exp_user_project_mappings.user_id = user_activity_log_records.user_id
    LEFT JOIN snapshot_contributor_stage scs
        ON scs.user_id = exp_user_project_mappings.user_id
            AND scs.project_id = exp_user_project_mappings.project_id
    WHERE
        u.status NOT IN('INTERNAL', 'PARTNER'
        )
        AND
        (
            project_registered_date IS NOT NULL
            OR application_received_date IS NOT NULL
            OR screened_date IS NOT NULL
            OR qualified_date IS NOT NULL
            OR ready_to_deploy_date IS NOT NULL
        )
)

SELECT *
FROM fct_snapshot_contributor_lifecycle
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20

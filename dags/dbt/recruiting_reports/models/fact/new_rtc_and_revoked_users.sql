{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

WITH users_status_changes AS (

    SELECT
        user_id
        , CASE
            WHEN type IN ('ACTIVE', 'EXPRESS_ACTIVE') THEN 'user_activation_after_proj_active'
            ELSE 'user_revoked'
        END AS action_type
        , type AS status
        , date_created
        , ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY date_created ASC) AS event_count
    FROM {{ source('qrp','user_status_changes') }}
    WHERE type IN ('ON_HOLD', 'ABANDONED', 'REACTIVATION_REQUESTED', 'CONTRACT_PENDING', 'EXPIRED', 'ARCHIVED'
        , 'EXPRESS_TO_BE_CONVERTED', 'IN_ACTIVATION_QUEUE', 'PARTNER', 'REGISTERED', 'SUSPENDED', 'TERMINATED'
        , 'EXPRESS_QUALIFYING', 'PAYONEER_SETUP', 'SCREENED', 'APPLICATION_RECEIVED', 'INTERNAL', 'REJECTED', 'STAGED'
        , 'ACTIVE', 'EXPRESS_ACTIVE'
    )

)

, users_status_changes_filtered AS (
    -- This next table removes the sequences (same status multiple rows) of user_revoked or user_activations. 
    -- We need just the 1st user revoke between user activations
    SELECT
        u1.user_id
        , u1.action_type
        , u1.status
        , u1.date_created
        , u2.action_type AS previous_event
    FROM users_status_changes AS u1
    LEFT JOIN users_status_changes AS u2
        ON u1.user_id = u2.user_id
            AND u1.event_count = u2.event_count + 1
    WHERE ((u1.event_count = 1 AND u1.action_type != 'user_revoked')
            OR u1.event_count > 1
        )
        AND ((u1.action_type = 'user_revoked'
                AND previous_event != 'user_revoked'
            )
            OR (u1.action_type = 'user_activation_after_proj_active'
                AND previous_event != 'user_activation_after_proj_active'
            )
            OR previous_event IS NULL
        )

)

, user_revoked AS (

    SELECT
        user_id
        , date_created
    FROM users_status_changes_filtered
    WHERE users_status_changes_filtered.action_type = 'user_revoked'

)

, user_active AS (

    SELECT
        user_id
        , date_created
        , ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY date_created ASC) AS active_event_count
    FROM users_status_changes_filtered
    WHERE users_status_changes_filtered.action_type = 'user_activation_after_proj_active'

)

, projects_status_changes AS (

    SELECT
        user_id
        , project_id
        , CASE
            WHEN status = 'ACTIVE' THEN 'project_activation'
            ELSE 'project_revoked'
        END AS action_type
        , status
        , date_created
        , ROW_NUMBER() OVER (PARTITION BY user_id, project_id ORDER BY date_created ASC) AS event_count
    FROM {{ source('qrp','exp_user_project_access_status_changes') }}
    WHERE status IN (
        'REVOKED'
        , 'ABANDONED'
        , 'EXAM_FAILED'
        , 'PRESCREENING'
        , 'QUALIFYING'
        , 'APPLICATION_RECEIVED'
        , 'NEED_REVIEW'
        , 'BLOCKED'
        , 'EXAM_SCHEDULED'
        , 'EXAM_STARTED'
        , 'REGISTERED'
        , 'REJECTED'
        , 'STAGED'
        , 'QUALIFYING_ON_HOLD'
        , 'REGISTERED_ON_HOLD', 'ACTIVE'
    )

)

, projects_status_changes_filtered AS (

    -- This next table removes the sequences (same status multiple rows) of project_revoked or project_activation. 
    -- We need just the 1st user revoke between user activations
    SELECT
        p1.user_id
        , p1.project_id
        , p1.action_type
        , p1.status
        , p1.date_created
        , p1.event_count
        , p2.action_type AS previous_event
    FROM projects_status_changes AS p1
    LEFT JOIN projects_status_changes AS p2
        ON p1.user_id = p2.user_id
            AND p1.project_id = p2.project_id
            AND p1.event_count = p2.event_count + 1
    WHERE ((p1.event_count = 1 AND p1.action_type != 'project_revoked')
            OR p1.event_count > 1
        )
        AND ((p1.action_type = 'project_revoked' AND previous_event != 'project_revoked')
            OR (p1.action_type = 'project_activation' AND previous_event != 'project_activation')
            OR previous_event IS NULL
        )

)

, proj_active AS (

    SELECT
        user_id
        , project_id
        , action_type
        , date_created
    FROM projects_status_changes_filtered
    WHERE action_type = 'project_activation'

)

, proj_revoked AS (

    SELECT
        user_id
        , project_id
        , action_type
        , date_created
    FROM projects_status_changes_filtered
    WHERE action_type = 'project_revoked'

)

, users_timeline AS (

    SELECT
        users_status_changes_filtered.user_id
        , NULL AS project_id
        , users_status_changes_filtered.action_type
        , users_status_changes_filtered.status
        , users_status_changes_filtered.date_created
    FROM users_status_changes_filtered

    UNION ALL

    SELECT
        projects_status_changes_filtered.user_id
        , projects_status_changes_filtered.project_id
        , projects_status_changes_filtered.action_type
        , projects_status_changes_filtered.status
        , projects_status_changes_filtered.date_created
    FROM projects_status_changes_filtered

)

, new_rtc_and_revoked AS (

    SELECT DISTINCT
        users_timeline.user_id
        , COALESCE(users_timeline.project_id, projects_status_changes_filtered.project_id) AS project_id
        , proj_mapping.locale_id
        , users_timeline.action_type
        , users_timeline.status
        , users_timeline.date_created
    -- , projects_status_changes_filtered.project_id AS project_id_user_revoked_active
    -- , projects_status_changes_filtered.action_type AS proj_revoked_active_status
    -- , projects_status_changes_filtered.date_created AS project_revoked_active_status_date
    -- , proj_active.project_id AS project_id_proj_active
    -- , proj_active.action_type AS proj_active_status
    -- , proj_active.date_created AS project_active_date
    -- , proj_revoked.project_id AS project_id_proj_revoked
    -- , proj_revoked.action_type AS proj_revoked_status
    -- , proj_revoked.date_created AS project_revoked_date
    -- , user_active.user_id AS user_id_active
    -- , user_active.date_created AS user_active_date
    -- , user_active.active_event_count AS user_active_event_count
    -- , user_revoked.user_id AS user_id_revoked
    -- , user_revoked.date_created AS user_revoke_date
    FROM users_timeline
    LEFT JOIN projects_status_changes_filtered
        ON users_timeline.user_id = projects_status_changes_filtered.user_id
            AND users_timeline.action_type IN ('user_revoked', 'user_activation_after_proj_active')
            AND projects_status_changes_filtered.action_type = 'project_activation'
    LEFT JOIN {{ source('qrp','exp_user_project_mappings') }} AS proj_mapping
        ON users_timeline.user_id = proj_mapping.user_id
            AND (projects_status_changes_filtered.project_id = proj_mapping.project_id
                OR users_timeline.project_id = proj_mapping.project_id
            )
    LEFT JOIN proj_revoked
        ON users_timeline.user_id = proj_revoked.user_id
            AND projects_status_changes_filtered.project_id = proj_revoked.project_id
            AND users_timeline.action_type IN ('user_revoked', 'user_activation_after_proj_active')
    LEFT JOIN proj_active
        ON users_timeline.user_id = proj_active.user_id
            AND users_timeline.action_type IN (
                'user_revoked'
                , 'project_revoked'
                , 'user_activation_after_proj_active'
            )
            AND (projects_status_changes_filtered.project_id = proj_active.project_id
                OR users_timeline.project_id = proj_active.project_id
            )
    LEFT JOIN user_active
        ON users_timeline.user_id = user_active.user_id
    LEFT JOIN user_revoked
        ON users_timeline.user_id = user_revoked.user_id
            AND (users_timeline.action_type = 'user_activation_after_proj_active'
                OR ( users_timeline.action_type = 'project_revoked'
                    AND user_revoked.date_created > user_active.date_created
                )
                OR users_timeline.action_type = 'project_activation'
            )
    WHERE
        --(1): there shall be one user activation event before the project activation to be new rtc
        --(2): there shall be one project activation before the project revoked to be proj revoked
        --(3): there shall be one project activation before user activation to be new rtc
        --(4): there shall be one project activation before the user revoked to be considered user revoked 
        --     level on that project
        --(5): there shall be no project revoked between the project ativation and the user revoked(timestamp
        --     +1 second to avoid situations where the user revoke timestamp is equal to the project revoked)
        --(6): user reactivation shall reactivate projects never revoked on project level or revoked after the 
        --     user reactivation
        --(8): to be considered project revoked, there shall be no user revoke before(the user will be considered 
        --     revoked on the user revoked event)
        --(9): to be considered project revoked, with existing user revoked only if user reactivates again 
        --     (UA-PA-UR-UA-PR)
        --(10): consider user activation only if it is after a user revoked. Not consider when user has 
        --     express_active and then active again.
        --(11): for subsequent user activations will only be considered if there was a user revoke before
        --(12): condition to match the same activation event
        ( users_timeline.action_type = 'project_activation'
            AND users_timeline.date_created >= user_active.date_created -- (1)
            AND ((user_active.date_created > user_revoked.date_created
                    OR user_revoked.date_created IS NULL
                )
                OR users_timeline.date_created < user_revoked.date_created
            )
        )
        OR (users_timeline.action_type = 'user_activation_after_proj_active'
            AND proj_active.date_created < users_timeline.date_created -- (3)
            AND user_active.date_created = users_timeline.date_created -- (12)
            AND (user_active.active_event_count = 1
            OR (user_active.active_event_count > 1
                AND user_revoked.date_created < users_timeline.date_created
            ) -- (11)
            ) -- (10)
            AND (proj_revoked.date_created IS NULL
                OR proj_revoked.date_created > users_timeline.date_created
                OR (proj_revoked.date_created < users_timeline.date_created
                    AND proj_active.date_created > proj_revoked.date_created
                )
            ) -- (6)
        )
        OR (users_timeline.action_type = 'user_revoked'
            AND users_timeline.date_created >= user_active.date_created -- (1)
            AND users_timeline.date_created >= proj_active.date_created -- (4)
            AND (
                DATEADD(
                    SECOND, 1, proj_revoked.date_created
                ) NOT BETWEEN proj_active.date_created AND users_timeline.date_created
                OR proj_revoked.date_created IS NULL
            )--(5)
        )
        OR (users_timeline.action_type = 'project_revoked'
            AND user_active.date_created <= users_timeline.date_created -- (1)
            AND proj_active.date_created <= users_timeline.date_created -- (2)
            AND ((users_timeline.date_created < user_revoked.date_created
                    OR user_revoked.date_created IS NULL
                ) -- (8)
                OR (users_timeline.date_created >= user_revoked.date_created
                    AND user_active.date_created > user_revoked.date_created
                ) -- (9)
            )
        )

)

, new_rtc_and_revoked_ordered AS (

    SELECT
        new_rtc_and_revoked.user_id
        , new_rtc_and_revoked.project_id
        , new_rtc_and_revoked.locale_id
        , new_rtc_and_revoked.action_type
        , new_rtc_and_revoked.status
        , new_rtc_and_revoked.date_created
        , ROW_NUMBER() OVER (
            PARTITION BY
                new_rtc_and_revoked.user_id
                , new_rtc_and_revoked.project_id
            ORDER BY
                new_rtc_and_revoked.date_created ASC
        ) AS event_count
    FROM new_rtc_and_revoked

)

, new_rtc_and_revoked_filtered AS (

    SELECT
        p1.user_id
        , p1.project_id
        , p1.locale_id
        , p1.action_type
        , p1.status
        , p1.date_created
        , p1.event_count
        , p2.action_type AS previous_event
    FROM new_rtc_and_revoked_ordered AS p1
    LEFT JOIN new_rtc_and_revoked_ordered AS p2
        ON p1.user_id = p2.user_id
            AND p1.project_id = p2.project_id
            AND p1.event_count = p2.event_count + 1
    WHERE p1.action_type IN ('project_activation', 'user_activation_after_proj_active')
        OR (
            (p1.action_type = 'project_revoked' AND previous_event != 'user_revoked')
            OR (p1.action_type = 'user_revoked' AND previous_event != 'project_revoked')
            OR previous_event IS NULL
        )

)

, new_rtc_and_revoked_users AS (

    SELECT
        new_rtc_and_revoked_filtered.user_id
        , new_rtc_and_revoked_filtered.project_id
        , new_rtc_and_revoked_filtered.locale_id
        , 'project_rejected' AS action_type
        , new_rtc_and_revoked_filtered.status
        , new_rtc_and_revoked_filtered.date_created
    FROM new_rtc_and_revoked_filtered
    WHERE new_rtc_and_revoked_filtered.action_type = 'project_revoked'
        AND new_rtc_and_revoked_filtered.status IN (
            'ABANDONED'
            , 'EXAM_FAILED'
            , 'PRESCREENING'
            , 'QUALIFYING'
            , 'APPLICATION_RECEIVED'
            , 'NEED_REVIEW'
            , 'BLOCKED'
            , 'EXAM_SCHEDULED'
            , 'EXAM_STARTED'
            , 'REGISTERED'
            , 'REJECTED'
            , 'STAGED'
            , 'QUALIFYING_ON_HOLD'
            , 'REGISTERED_ON_HOLD'
        )

    UNION ALL

    SELECT
        new_rtc_and_revoked_filtered.user_id
        , new_rtc_and_revoked_filtered.project_id
        , new_rtc_and_revoked_filtered.locale_id
        , new_rtc_and_revoked_filtered.action_type
        , new_rtc_and_revoked_filtered.status
        , new_rtc_and_revoked_filtered.date_created
    FROM new_rtc_and_revoked_filtered
    WHERE
        new_rtc_and_revoked_filtered.action_type IN (
            'user_revoked', 'project_activation', 'user_activation_after_proj_active'
        )
        OR (new_rtc_and_revoked_filtered.action_type = 'project_revoked'
            AND new_rtc_and_revoked_filtered.status = 'REVOKED'
        )

)

SELECT *
FROM new_rtc_and_revoked_users

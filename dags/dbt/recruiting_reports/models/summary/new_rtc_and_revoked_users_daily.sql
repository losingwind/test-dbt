{{
    config(
        materialized='table',
        tags=["summary"]
    )
}}

WITH rtc_users AS (

    SELECT
        project_id
        , locale_id
        , DATE_TRUNC('day', new_rtc_and_revoked_users.date_created) AS rtc_date
        , COUNT(user_id) AS new_rtc_users
    FROM {{ ref('new_rtc_and_revoked_users') }}
    WHERE action_type IN ('project_activation', 'user_activation_after_proj_active')
    GROUP BY 1, 2, 3
)

, revoked_users AS (

    SELECT
        project_id
        , locale_id
        , DATE_TRUNC('day', new_rtc_and_revoked_users.date_created) AS rtc_date
        , COUNT(user_id) AS revoked_users
    FROM {{ ref('new_rtc_and_revoked_users') }}
    WHERE action_type IN ('user_revoked', 'project_revoked', 'project_rejected')
    GROUP BY 1, 2, 3
)

, new_rtc_and_revoked AS (

    SELECT DISTINCT
        project_id
        , locale_id
        , DATE_TRUNC('day', date_created) AS rtc_date
    FROM {{ ref('new_rtc_and_revoked_users') }}

)

SELECT
    new_rtc_and_revoked.project_id
    , new_rtc_and_revoked.locale_id
    , new_rtc_and_revoked.rtc_date
    , COALESCE(rtc_users.new_rtc_users, 0) AS new_rtc_users
    , COALESCE(revoked_users.revoked_users, 0) AS revoked_users
    , (COALESCE(rtc_users.new_rtc_users, 0) - COALESCE(revoked_users.revoked_users, 0)) AS rtc_balance
    , SUM((COALESCE(rtc_users.new_rtc_users, 0) - COALESCE(revoked_users.revoked_users, 0))) OVER (
        PARTITION BY new_rtc_and_revoked.project_id, new_rtc_and_revoked.locale_id
        ORDER BY new_rtc_and_revoked.rtc_date, new_rtc_and_revoked.locale_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS existing_users
FROM new_rtc_and_revoked
LEFT JOIN rtc_users
    ON new_rtc_and_revoked.project_id = rtc_users.project_id
        AND (new_rtc_and_revoked.locale_id = rtc_users.locale_id
            OR (rtc_users.locale_id IS NULL
                AND new_rtc_and_revoked.locale_id IS NULL
            )
        )
        AND new_rtc_and_revoked.rtc_date = rtc_users.rtc_date
LEFT JOIN revoked_users
    ON new_rtc_and_revoked.project_id = revoked_users.project_id
        AND (new_rtc_and_revoked.locale_id = revoked_users.locale_id
            OR (revoked_users.locale_id IS NULL
                AND new_rtc_and_revoked.locale_id IS NULL
            )
        )
        AND new_rtc_and_revoked.rtc_date = revoked_users.rtc_date

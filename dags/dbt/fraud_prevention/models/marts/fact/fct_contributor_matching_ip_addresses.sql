{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

WITH contributors_ips AS (

    SELECT DISTINCT
        contributors_logins.contributor_id
        , contributors_logins.ip_address
    FROM {{ ref('fct_contributor_logins') }} contributors_logins

)

, not_terminated_contributors_ips AS (

    SELECT DISTINCT
        contributors_logins.contributor_id
        , contributors_logins.ip_address
    FROM {{ ref('fct_contributor_logins') }} contributors_logins
    INNER JOIN {{ source('crowdcrm', 'stg_sp_users') }} u
        ON u.user_id = contributors_logins.contributor_id
    WHERE u.status IN ('ACTIVE', 'EXPRESS_ACTIVE', 'CONTRACT_PENDING', 'APPLICATION_RECEIVED', 'EXPRESS_QUALIFYING'
        , 'STAGED', 'IN_ACTIVATION_QUEUE', 'PAYONEER_SETUP'
    )

)

, terminated_contributors_ips AS (

    -- Here we get data directly from qrp.user_activity_log_records because we dont want
    -- the time filter present on fct_contributor_logins
    SELECT DISTINCT
        ualr.user_id AS contributor_id
        , ualr.ip_address AS scammer_ip_address
    FROM {{ source('qrp', 'user_activity_log_records') }} ualr
    INNER JOIN {{ source('crowdcrm', 'stg_sp_users') }} u
        ON u.user_id = ualr.user_id
    WHERE u.user_conditional = 'user_terminated_for_maliciousness'

)

, not_terminated_matching_users_ips AS (

    SELECT DISTINCT
        t1.ip_address
        , t1.contributor_id AS contributor_id
        , t2.contributor_id AS contributor_id_matched_same_ip
        , 'NOT TERMINATED' AS contributor_status_matched_same_ip
    FROM contributors_ips t1
    INNER JOIN not_terminated_contributors_ips t2
        ON t1.ip_address = t2.ip_address AND t1.contributor_id != t2.contributor_id

)

, terminated_matching_users_ips AS (

    SELECT DISTINCT
        t1.ip_address
        , t1.contributor_id AS contributor_id
        , t2.contributor_id AS contributor_id_matched_same_ip
        , 'TERMINATED' AS contributor_status_matched_same_ip
    FROM contributors_ips t1
    INNER JOIN terminated_contributors_ips t2
        ON t1.ip_address = t2.scammer_ip_address AND t1.contributor_id != t2.contributor_id

)

SELECT *
FROM not_terminated_matching_users_ips
UNION ALL
SELECT *
FROM terminated_matching_users_ips

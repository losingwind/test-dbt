{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

SELECT
    ualr.id AS login_id
    , ualr.user_id AS contributor_id
    , ualr.ip_address
    , ips.ip_address AS ipqs_ip_address
    , ualr.date_created
    , COALESCE(ips.country_code, '') AS ipqs_country_code
    , COALESCE(ips.active_vpn, FALSE) AS active_vpn
    , ips.vpn
    , ips.tor
    , ips.proxy
    , COALESCE(ips.bot_status, FALSE) AS bot_status
FROM {{ source('qrp', 'user_activity_log_records') }} ualr
LEFT JOIN {{ source('ipqs', 'ip_quality_history') }} ips
    ON ualr.id = ips.login_id
WHERE ualr.date_created >= DATE(GETDATE()) - '45 days'::INTERVAL
    AND ualr.user_id IS NOT NULL
    AND ualr.ip_address NOT IN (
        SELECT DISTINCT ip_address
        FROM {{ source('recruiting', 'internal_ips') }}
    )
    AND ualr.ip_address NOT IN (
        SELECT DISTINCT ip_address
        FROM {{ source('crowdcrm', 'dim_suspicious_ips') }}
    )

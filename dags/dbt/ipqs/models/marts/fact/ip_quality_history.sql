{{
  config(
    materialized='incremental',
    unique_key='id',
    tags=["fact"]
  )
}}

WITH ip_quality_history AS (

    SELECT
        id
        , TO_TIMESTAMP(created_at, 'YYYY/MM/DD HH24:MI:SS.US') AS created_at
        , TO_TIMESTAMP(login_at, 'YYYY/MM/DD HH24:MI:SS.US') AS login_at
        , login_id
        , user_id
        , region
        , country_code
        , abuse_velocity
        , city
        , timezone
        , zip_code
        , isp
        , active_tor
        , latitude
        , longitude
        , source
        , fraud_score
        , recent_abuse
        , tor
        , ip_address
        , active_vpn
        , connection_type
        , mobile
        , bot_status
        , proxy
        , is_crawler
        , vpn
        , organization
        , asn
    FROM {{ source('r_ipqs','ip_quality_history') }}

)

SELECT *
FROM ip_quality_history

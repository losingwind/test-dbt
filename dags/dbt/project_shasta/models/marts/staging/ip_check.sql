{{
  config(
    materialized='incremental',
    tags=["stg"]
  )
}}

WITH users AS (

    SELECT
        id
        , email
    FROM {{ source('qrp','users') }}

)

, raw_ip_check AS (

    SELECT
        ip
        , TO_TIMESTAMP(created, 'YYYY/MM/DD HH24:MI:SS.US') AS created
        , rater_id
        , rater_email
        , vendor
        , locale
        , status
    FROM {{ source('r_shasta','ip_check') }}

)

, ip_check AS (

    SELECT
        raw_ip_check.created
        , raw_ip_check.ip
        , users.id AS user_id
        , raw_ip_check.rater_id
        , raw_ip_check.rater_email
        , raw_ip_check.vendor
        , raw_ip_check.locale
        , raw_ip_check.status
    FROM raw_ip_check
    LEFT JOIN users
        ON raw_ip_check.rater_email = users.email

)

SELECT *
FROM ip_check

{% if is_incremental() %}
    WHERE created > (SELECT MAX(created) FROM {{ this }})
{% endif %}

{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}


SELECT DISTINCT
    usr.id AS user_id
    , usr.country AS user_country
    , usr_locale.locale_id
    , locales.name AS locale
    , locales.language_3 AS dialect
    , locales.country_3 AS country
    , usr_locale.spoken_fluency
    , usr_locale.written_fluency
    , usr_locale.is_primary
FROM {{ source('qrp','users') }} usr
LEFT JOIN {{ source('qrp','exp_user_locales') }} usr_locale
    ON usr_locale.user_id = usr.id
LEFT JOIN {{ source('qrp','exp_locales') }} locales
    ON locales.id = usr_locale.locale_id
INNER JOIN {{ source('qrp','user_activity_log_records') }} usr_actv_log
    ON usr_actv_log.user_id = usr.id
        AND usr_actv_log.type = 'LOGIN'
        AND usr_actv_log.date_created >= (CURRENT_TIMESTAMP - INTERVAL '6 Month') -- noqa
WHERE usr.status IN ('ACTIVE', 'CONTRACT_PENDING', 'IN_ACTIVATION_QUEUE', 'PAYONEER_SETUP', 'EXPRESS_QUALIFYING'
    , 'EXPRESS_ACTIVE', 'SCREENED', 'REGISTERED', 'APPLICATION_RECEIVED', 'ON_HOLD', 'STAGED'
)

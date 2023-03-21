-- noqa: TMP
{{
    config(
        materialized='incremental',
        unique_key='unique_key'
    )
}}

WITH u AS (

    SELECT *
    FROM {{ source('recruiting', 'stg_users') }}

)

, pm AS (

    SELECT *
    FROM {{ source('recruiting', 'stg_exp_user_project_mappings') }}

)

, el AS (

    SELECT *
    FROM {{ source('recruiting', 'stg_exp_locales') }}

)

, el2 AS (

    SELECT *
    FROM {{ source('recruiting', 'stg_exp_locales') }}

)

, cmap AS (

    SELECT *
    FROM {{ source('map', 'dim_country') }}

)

, active_vendors_hly AS (

    SELECT
        pm.project_id
        , cmap.country_3 AS user_country
        , pm.locale_id
        , el.country_3 AS locale_country
        , el.language_3 AS locale_lang
        , pm.to_locale_id
        , el2.country_3 AS to_locale_country
        , el2.language_3 AS to_locale_lang
        , TRUNC(DATEADD(hour, -1, DATE_TRUNC('hour', CONVERT_TIMEZONE('UTC', 'US/Pacific', GETDATE())))) AS fact_date
        , CONVERT_TIMEZONE('UTC', 'US/Pacific', GETDATE()) AS fact_tmp
        , {{ dbt_utils.surrogate_key([
            'pm.project_id'
            , 'cmap.country_3'
            , 'pm.locale_id'
            , 'el.country_3'
            , 'el.language_3'
            , 'pm.to_locale_id'
            , 'el2.country_3'
            , 'el2.language_3'
            , 'fact_date'
        ]) }} AS unique_key
        , {{ dbt_utils.surrogate_key([ 
            'pm.project_id'
            , 'cmap.country_3'
            , 'pm.locale_id'
            , 'el.country_3'
            , 'el.language_3'
            , 'pm.to_locale_id'
            , 'el2.country_3'
            , 'el2.language_3'
        ]) }} AS project_locale_key
        , COUNT(u.user_id)
    FROM pm
    INNER JOIN u
        ON pm.user_id = u.user_id
    LEFT JOIN el
        ON pm.locale_id = el.locale_id
    LEFT JOIN el2
        ON pm.to_locale_id = el2.locale_id
    LEFT JOIN cmap
        ON u.user_country = cmap.country_2
    WHERE (u.job_offer_start_at < CONVERT_TIMEZONE('UTC', 'US/Pacific', DATE_TRUNC('hour', GETDATE()))
            OR u.first_hired_at < CONVERT_TIMEZONE('UTC', 'US/Pacific', DATE_TRUNC('hour', GETDATE()))
        )
        AND pm.pm_active_at < CONVERT_TIMEZONE('UTC', 'US/Pacific', DATE_TRUNC('hour', GETDATE()))
        AND pm.pm_status = 'ACTIVE'
        AND u.user_status IN ('ACTIVE', 'EXPRESS_ACTIVE')
        AND pm.project_id != 85
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12

)

SELECT *
FROM active_vendors_hly


{% if is_incremental() %}

    WHERE fact_date >= (SELECT MAX(fact_date) FROM {{ this }})

{% endif %}

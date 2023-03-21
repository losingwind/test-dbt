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

, p AS (

    SELECT *
    FROM {{ source('recruiting', 'stg_exp_projects') }}

)

, fct_qualifying_vendors_hly AS (

    SELECT
        pm.project_id
        , pm.user_id
        , cmap.country_3 AS user_country
        , pm.locale_id
        , el.country_3 AS locale_country
        , el.language_3 AS locale_lang
        , pm.to_locale_id
        , el2.country_3 AS to_locale_country
        , el2.language_3 AS to_locale_lang
        , TRUNC(DATEADD(hour, -1, DATE_TRUNC('hour', CONVERT_TIMEZONE('UTC', 'US/Pacific', GETDATE())))) AS fact_date
        , CONVERT_TIMEZONE('UTC', 'US/Pacific', GETDATE()) AS fact_tmp
    FROM pm
    INNER JOIN u
        ON pm.user_id = u.user_id
    INNER JOIN p
        ON pm.project_id = p.project_id
    LEFT JOIN el
        ON pm.locale_id = el.locale_id
    LEFT JOIN el2
        ON pm.to_locale_id = el2.locale_id
    LEFT JOIN cmap
        ON u.user_country = cmap.country_2
    WHERE (u.job_offer_start_at < CONVERT_TIMEZONE('UTC', 'US/Pacific', DATE_TRUNC('hour', GETDATE()))
            OR u.first_hired_at < CONVERT_TIMEZONE('UTC', 'US/Pacific', DATE_TRUNC('hour', GETDATE()))
        )
        AND pm.pm_status IN ('PRESCREENING', 'EXAM_SCHEDULED', 'EXAM_STARTED', 'EXAM_FAILED')
        AND u.user_status IN ('ACTIVE', 'EXPRESS_ACTIVE')
        AND p.project_id = 1

    UNION

    SELECT
        pm.project_id
        , pm.user_id
        , cmap.country_3 AS user_country
        , pm.locale_id
        , el.country_3 AS locale_country
        , el.language_3 AS locale_lang
        , pm.to_locale_id
        , el2.country_3 AS to_locale_country
        , el2.language_3 AS to_locale_lang
        , TRUNC(DATEADD(hour, -1, DATE_TRUNC('hour', CONVERT_TIMEZONE('UTC', 'US/Pacific', GETDATE())))) AS fact_date
        , CONVERT_TIMEZONE('UTC', 'US/Pacific', GETDATE()) AS fact_tmp
    FROM pm
    INNER JOIN u
        ON pm.user_id = u.user_id
    INNER JOIN p
        ON pm.project_id = p.project_id
    LEFT JOIN el
        ON pm.locale_id = el.locale_id
    LEFT JOIN el2
        ON pm.to_locale_id = el2.locale_id
    LEFT JOIN cmap
        ON u.user_country = cmap.country_2
    WHERE (u.job_offer_start_at < CONVERT_TIMEZONE('UTC', 'US/Pacific', DATE_TRUNC('hour', GETDATE()))
            OR u.first_hired_at < CONVERT_TIMEZONE('UTC', 'US/Pacific', DATE_TRUNC('hour', GETDATE()))
        )
        AND pm.pm_status IN ('QUALIFYING', 'QUALIFYING_ON_HOLD', 'EXAM_STARTED', 'EXAM_FAILED')
        AND u.user_status IN ('ACTIVE', 'EXPRESS_ACTIVE')
        AND pm.project_id != 85
        AND p.project_id != 1

), qualifying_vendors_hly AS (

    SELECT
        project_id
        , user_country
        , locale_id
        , locale_country
        , locale_lang
        , to_locale_id
        , to_locale_country
        , to_locale_lang
        , fact_date
        , fact_tmp
        , {{ dbt_utils.surrogate_key([
            'project_id'
            , 'user_country'
            , 'locale_id'
            , 'locale_country'
            , 'locale_lang'
            , 'to_locale_id'
            , 'to_locale_country'
            , 'to_locale_lang'
            , 'fact_date'
        ]) }} AS unique_key
        , {{ dbt_utils.surrogate_key([
            'project_id'
            , 'user_country'
            , 'locale_id'
            , 'locale_country'
            , 'locale_lang'
            , 'to_locale_id'
            , 'to_locale_country'
            , 'to_locale_lang'
        ]) }} AS project_locale_key
        , COUNT(user_id)
    FROM fct_qualifying_vendors_hly
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12

)

SELECT *
FROM qualifying_vendors_hly


{% if is_incremental() %}

    WHERE fact_date >= (SELECT MAX(fact_date) FROM {{ this }})

{% endif %}

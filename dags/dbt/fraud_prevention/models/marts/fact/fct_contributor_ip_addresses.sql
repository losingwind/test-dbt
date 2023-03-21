{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

WITH countries_payrate AS (

    SELECT *
    FROM (
        SELECT
            country
            , pay_rate
            , ROW_NUMBER() OVER (PARTITION BY country ORDER BY date_updated DESC) AS row_number
        FROM {{ source('qrp','exp_country_payrates') }}
        WHERE state IS NULL
            AND rate_type = 'HOURLY'
    ) temp
    WHERE row_number = 1

)

, step1_data AS (

    SELECT
        contributor_id
        , ip_address
        , COALESCE(ipqs_country_code, '') AS ipqs_country_code
        , vpn
        , bot_status
        , COUNT(ip_address) AS count_of_ip_occurrence
        , MIN(date_created) AS first_appearance
        , MAX(date_created) AS last_appearance
    FROM {{ ref('fct_contributor_logins') }}
    GROUP BY 1, 2, 3, 4, 5

)

SELECT DISTINCT
    step1_data.contributor_id
    , COALESCE(step1_data.ip_address, '') AS ip_address
    , step1_data.count_of_ip_occurrence AS count_of_ip_occurrence
    , step1_data.first_appearance AS ac_ip_first_appearance
    , step1_data.last_appearance AS ac_ip_last_appearance
    , step1_data.ipqs_country_code
    , step1_data.vpn
    , step1_data.bot_status
    , countries_payrate.pay_rate AS ip_pay_rate
    , step1_data.ipqs_country_code || '/' || contributors.country AS ipqs_country_ac_country
    , CASE
        WHEN DATEDIFF(DAY, ac_ip_last_appearance, GETDATE()) < 7 THEN 'Recent'
        WHEN ac_ip_last_appearance IS NULL THEN ''
        ELSE 'Stale'
    END AS ip_less_than_8_days
    , CASE
        WHEN step1_data.ipqs_country_code = '' OR contributors.country = '' THEN ''
        WHEN step1_data.ipqs_country_code = contributors.country THEN 'OK'
        ELSE 'Country Mismatch'
    END AS country_mismatch
    , COALESCE(ip_less_than_8_days = 'Recent' AND step1_data.bot_status = TRUE, FALSE) AS recent_true_bot_status
    , CASE
        WHEN
            contributors.country IN (
                'AE'
                , 'AT'
                , 'AU'
                , 'BE'
                , 'CA'
                , 'CH'
                , 'DE'
                , 'DK'
                , 'ES'
                , 'FI'
                , 'FR'
                , 'GB'
                , 'UK'
                , 'IE'
                , 'JP'
                , 'LI'
                , 'NL'
                , 'NO'
                , 'SA'
                , 'SE'
                , 'US'
            )
            AND step1_data.ipqs_country_code IN (
                'UA', 'BR', 'BY', 'CY', 'EG', 'IN', 'MA', 'MD', 'NG', 'PL', 'RO', 'RU', 'TH', 'VN'
            ) THEN 'Take5-Fail'
        ELSE ''
    END AS take5_fail
    , CASE
        WHEN step1_data.ip_address IS NULL THEN 'Take5-NeedsReview'
        WHEN country_mismatch IN (''::VARCHAR, 'Country Mismatch'::VARCHAR) THEN 'Take5-NeedsReview'
        WHEN contributors.country = 'PH' OR step1_data.ipqs_country_code = 'PH' THEN 'Take5-NeedsReview'
        ELSE ''
    END AS take5_needs_review
    , CASE
        WHEN step1_data.ip_address IS NULL THEN 'no IP'
        WHEN country_mismatch IN (''::VARCHAR, 'Country Mismatch'::VARCHAR) THEN ipqs_country_ac_country
        WHEN contributors.country = 'PH' OR step1_data.ipqs_country_code = 'PH' THEN 'from PH'
        ELSE ''
    END AS take5_review_reason
FROM step1_data
LEFT JOIN {{ source('dim','dim_contributors') }} contributors
    ON step1_data.contributor_id = contributors.contributor_id
LEFT JOIN countries_payrate
    ON step1_data.ipqs_country_code = countries_payrate.country

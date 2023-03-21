{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

SELECT
    h.project_id
    , h.locale_id
    , l.name AS locale
    , c.country_2
    , u.first_name || ' ' || u.last_name AS owner_name
    , h.locale_country AS dialect
    , h.country AS hiring_country
    , h.locale_lang AS language
    , h.priority
    , SUM(h.target) AS target
FROM {{ source('qrp','exp_project_hiring_targets') }} h
LEFT JOIN {{ source('qrp','exp_locales') }} l
    ON l.id = h.locale_id
LEFT JOIN {{ source('dim','dim_country') }} c
    ON c.country_3 = h.locale_country
LEFT JOIN {{ source('qrp','exp_projects') }} p
    ON p.id = h.project_id
LEFT JOIN {{ source('qrp','users') }} u
    ON u.id = h.owner_id
WHERE target > 0
    AND p.status = 'ENABLED'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9

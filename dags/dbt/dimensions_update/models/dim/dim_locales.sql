{{
	config(
		materialized='table',
		tags=["dim"],
		sort='locale_id',
		dist='date_created'
	)
}}

WITH el AS (

    SELECT *
    FROM {{ source('qrp','exp_locales') }}

)

, dc AS (

    SELECT *
    FROM {{ source('dim','dim_country') }}

)

, dl AS (

    SELECT *
    FROM {{ source('dim','dim_language') }}

)

, dim_locales AS (

    SELECT
        el.id AS locale_id
        , el.date_created
        , el.name
        , el.language
        , el.language_3
        , dl.language_full AS language_name
        , el.country
        , el.country_3
        , dc.country_full AS country_name
        , el.code
        , el.code_3
    FROM el
    INNER JOIN dl
        ON el.language_3 = dl.language_3
    INNER JOIN dc
        ON el.country_3 = dc.country_3

)

SELECT *
FROM dim_locales

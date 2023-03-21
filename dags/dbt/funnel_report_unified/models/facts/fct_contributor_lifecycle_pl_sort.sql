{{
    config(
        materialized='table',dist='registration_date', sort=['project_id', 'locale_id']
        , tags=["fact"]
    )
}}

SELECT *
FROM {{ ref('fct_contributor_lifecycle') }}

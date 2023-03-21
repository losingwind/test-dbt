{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

WITH fct_similar_addresses AS (

    SELECT *
    FROM
        {{ source('crowdcrm','similar_addresses') }}

)

SELECT *
FROM fct_similar_addresses

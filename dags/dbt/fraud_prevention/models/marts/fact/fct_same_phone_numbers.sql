{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

WITH fct_same_phone_numbers AS (

    SELECT *
    FROM
        {{ source('crowdcrm','same_phone_numbers') }}

)

SELECT * FROM fct_same_phone_numbers

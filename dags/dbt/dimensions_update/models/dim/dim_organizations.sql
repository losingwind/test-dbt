{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

WITH dim_organizations AS (

    SELECT
        id
        , name
    FROM {{ source('r_akon','organizations') }}

)

SELECT * FROM dim_organizations

{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

WITH dim_teams AS (

    SELECT
        id
        , organization_id
        , name AS team_name
        , plan AS team_plan
        , fair_pay_enabled
        , local_nav_enabled
        , markup AS team_markup
    FROM {{ source('r_akon','teams') }}

)

SELECT * FROM dim_teams

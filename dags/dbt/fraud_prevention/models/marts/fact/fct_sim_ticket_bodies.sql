{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

WITH fct_sim_ticket_bodies AS (
    SELECT *
    FROM
        {{ source('crowdcrm','sim_ticket_bodies') }}
)

SELECT *
FROM fct_sim_ticket_bodies

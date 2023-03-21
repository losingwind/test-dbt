{{
	config(
		materialized='incremental',
		unique_key='employee_uid',
		tags=["dim"]
	)
}}

WITH u AS (

    SELECT
        id
        , first_name
        , last_name
    FROM {{ source('qrp','users') }}

)

, t AS (

    SELECT
        id
        , type
    FROM {{ source('qrp','teams') }}

)

, ut AS (

    SELECT
        user_id
        , team_id
    FROM {{ source('qrp','user_teams') }}

)

, dim_employees AS (

    SELECT DISTINCT
        u.id AS employee_uid
        , u.first_name || ' ' || u.last_name AS employee_name
        , t.type AS employee_role
        , CONVERT_TIMEZONE('UTC', 'US/Pacific', GETDATE()) AS edw_date_updated
    FROM u
    INNER JOIN ut
        ON u.id = ut.user_id
    INNER JOIN t
        ON ut.team_id = t.id
    WHERE t.type = 'PROJECT_MANAGER'

)

SELECT *
FROM dim_employees

{% if is_incremental() %}

    WHERE edw_date_updated >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}

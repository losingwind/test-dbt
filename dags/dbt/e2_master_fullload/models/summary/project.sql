{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_project AS (

	SELECT *
	FROM {{ source('e2_stage','project') }}

), project AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_project.id']) }} AS unique_key,
		stg_project.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_project

)

SELECT *
FROM project 

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}

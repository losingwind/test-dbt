{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_task AS(

	SELECT *
	FROM {{ source('e2_stage','task') }}

), task AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_task.id']) }} AS unique_key,
		stg_task.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_task

)

SELECT *
FROM task

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}
{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_taskprojectsummary AS(

	SELECT *
	FROM {{ source('e2_stage','taskprojectsummary') }}

), taskprojectsummary AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_taskprojectsummary.id','stg_taskprojectsummary.ix0']) }} AS unique_key,
		stg_taskprojectsummary.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_taskprojectsummary

)

SELECT *
FROM taskprojectsummary

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}
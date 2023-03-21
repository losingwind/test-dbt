{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_userraterscoresscore AS(

	SELECT *
	FROM {{ source('e2_stage','userraterscoresscore') }}

), userraterscoresscore AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_userraterscoresscore.id','stg_userraterscoresscore.ix0',
			'stg_userraterscoresscore.ix1']) }} AS unique_key,
		stg_userraterscoresscore.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_userraterscoresscore

)

SELECT *
FROM userraterscoresscore

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}
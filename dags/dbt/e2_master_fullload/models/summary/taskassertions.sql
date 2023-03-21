{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_taskassertions AS(

	SELECT *
	FROM {{ source('e2_stage','taskassertions') }}

), taskassertions AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_taskassertions.id','stg_taskassertions.ix0']) }} AS unique_key,
		stg_taskassertions.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_taskassertions

)

SELECT *
FROM taskassertions

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}
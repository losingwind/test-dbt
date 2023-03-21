{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_taskgoldenassertions AS(

	SELECT *
	FROM {{ source('e2_stage','taskgoldenassertions') }}

), taskgoldenassertions AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_taskgoldenassertions.id','stg_taskgoldenassertions.ix0']) }} AS unique_key,
		stg_taskgoldenassertions.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_taskgoldenassertions

)

SELECT *
FROM taskgoldenassertions

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}
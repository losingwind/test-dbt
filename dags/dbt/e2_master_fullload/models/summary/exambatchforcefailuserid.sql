{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_exambatchforcefailuserid AS (

	SELECT *
	FROM {{ source('e2_stage','exambatchforcefailuserid') }}

), exambatchforcefailuserid AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_exambatchforcefailuserid.id',
			'stg_exambatchforcefailuserid.ix0']) }} AS unique_key,
		stg_exambatchforcefailuserid.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_exambatchforcefailuserid

)

SELECT *
FROM exambatchforcefailuserid 

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}

{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_exambatchraterid AS (

	SELECT *
	FROM {{ source('e2_stage','exambatchraterid') }}

), exambatchraterid AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_exambatchraterid.id',
			'stg_exambatchraterid.ix0']) }} AS unique_key,
		stg_exambatchraterid.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_exambatchraterid

)

SELECT *
FROM exambatchraterid 

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}

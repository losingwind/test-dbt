{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_exambatch AS (

	SELECT *
	FROM {{ source('e2_stage','exambatch') }}

), exambatch AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_exambatch.id']) }} AS unique_key,
		stg_exambatch.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_exambatch

)

SELECT *
FROM exambatch 

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}

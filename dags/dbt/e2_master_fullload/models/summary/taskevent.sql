{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_taskevent AS (

	SELECT *
	FROM {{ source('e2_stage','taskevent') }}

), taskevent AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_taskevent.id','stg_taskevent.ix0']) }} AS unique_key,
		stg_taskevent.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_taskevent

)

SELECT *
FROM taskevent 

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}

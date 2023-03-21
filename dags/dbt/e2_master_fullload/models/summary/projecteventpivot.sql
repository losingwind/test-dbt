{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_projecteventpivot AS (

	SELECT *
	FROM {{ source('e2_stage','projecteventpivot') }}

), projecteventpivot AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_projecteventpivot.id']) }} AS unique_key,
		stg_projecteventpivot.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_projecteventpivot

)

SELECT *
FROM projecteventpivot 

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}

{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_property AS(

	SELECT *
	FROM {{ source('e2_stage','property') }}

), property AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_property.id']) }} AS unique_key,
		stg_property.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_property

)

SELECT *
FROM property

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}
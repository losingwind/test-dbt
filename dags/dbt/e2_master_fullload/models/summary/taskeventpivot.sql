{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_taskeventpivot AS(

	SELECT *
	FROM {{ source('e2_stage','taskeventpivot') }}

), taskeventpivot AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_taskeventpivot.id']) }} AS unique_key,
		stg_taskeventpivot.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_taskeventpivot

)

SELECT *
FROM taskeventpivot

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}
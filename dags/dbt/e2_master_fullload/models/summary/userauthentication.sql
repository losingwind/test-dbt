{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_userauthentication AS(

	SELECT *
	FROM {{ source('e2_stage','userauthentication') }}

), userauthentication AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_userauthentication.id']) }} AS unique_key,
		stg_userauthentication.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_userauthentication

)

SELECT *
FROM userauthentication

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}
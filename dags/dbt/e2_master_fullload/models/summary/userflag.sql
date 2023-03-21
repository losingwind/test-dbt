{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_userflag AS(

	SELECT *
	FROM {{ source('e2_stage','userflag') }}

), userflag AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_userflag.id','stg_userflag.ix0']) }} AS unique_key,
		stg_userflag.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_userflag

)

SELECT *
FROM userflag

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}
{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_userlocaleid AS(

	SELECT *
	FROM {{ source('e2_stage','userlocaleid') }}

), userlocaleid AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_userlocaleid.id','stg_userlocaleid.ix0']) }} AS unique_key,
		stg_userlocaleid.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_userlocaleid

)

SELECT *
FROM userlocaleid

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}
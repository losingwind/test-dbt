{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_useremployment AS(

	SELECT *
	FROM {{ source('e2_stage','useremployment') }}

), useremployment AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_useremployment.id','stg_useremployment.ix0']) }} AS unique_key,
		stg_useremployment.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_useremployment

)

SELECT *
FROM useremployment

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}
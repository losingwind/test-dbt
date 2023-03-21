{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_userraterpoolmembership AS(

	SELECT *
	FROM {{ source('e2_stage','userraterpoolmembership') }}

), userraterpoolmembership AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_userraterpoolmembership.id','stg_userraterpoolmembership.ix0']) }} AS unique_key,
		stg_userraterpoolmembership.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_userraterpoolmembership

)

SELECT *
FROM userraterpoolmembership

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}
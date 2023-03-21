{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_userrecentdevice AS(

	SELECT *
	FROM {{ source('e2_stage','userrecentdevice') }}

), userrecentdevice AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_userrecentdevice.id','stg_userrecentdevice.ix0']) }} AS unique_key,
		stg_userrecentdevice.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_userrecentdevice

)

SELECT *
FROM userrecentdevice

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}
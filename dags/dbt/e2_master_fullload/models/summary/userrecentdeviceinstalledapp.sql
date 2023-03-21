{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_userrecentdeviceinstalledapp AS(

	SELECT *
	FROM {{ source('e2_stage','userrecentdeviceinstalledapp') }}

), userrecentdeviceinstalledapp AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_userrecentdeviceinstalledapp.id','stg_userrecentdeviceinstalledapp.ix0',
			'stg_userrecentdeviceinstalledapp.ix1']) }} AS unique_key,
		stg_userrecentdeviceinstalledapp.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_userrecentdeviceinstalledapp

)

SELECT *
FROM userrecentdeviceinstalledapp

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}
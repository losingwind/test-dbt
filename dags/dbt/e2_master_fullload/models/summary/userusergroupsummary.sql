{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_userusergroupsummary AS(

	SELECT *
	FROM {{ source('e2_stage','userusergroupsummary') }}

), userusergroupsummary AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_userusergroupsummary.id','stg_userusergroupsummary.ix0']) }} AS unique_key,
		stg_userusergroupsummary.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_userusergroupsummary

)

SELECT *
FROM userusergroupsummary

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}
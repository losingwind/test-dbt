{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_userannotation AS(

	SELECT *
	FROM {{ source('e2_stage','userannotation') }}

), userannotation AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_userannotation.id','stg_userannotation.ix0']) }} AS unique_key,
		stg_userannotation.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_userannotation

)

SELECT *
FROM userannotation

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}
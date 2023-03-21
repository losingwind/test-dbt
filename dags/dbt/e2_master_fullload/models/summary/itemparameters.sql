{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_itemparameters AS(

	SELECT *
	FROM {{ source('e2_stage','itemparameters') }}

), itemparameters AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_itemparameters.id','stg_itemparameters.ix0']) }} AS unique_key,
		stg_itemparameters.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_itemparameters

)

SELECT *
FROM itemparameters

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}
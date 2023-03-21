{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_item AS (

	SELECT *
	FROM {{ source('e2_stage','item') }}

), item AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_item.id']) }} AS unique_key,
		stg_item.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_item

)

SELECT *
FROM item 

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}

{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_itemoriginalprojectsummary AS (

	SELECT *
	FROM {{ source('e2_stage','itemoriginalprojectsummary') }}

), itemoriginalprojectsummary AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_itemoriginalprojectsummary.id']) }} AS unique_key,
		stg_itemoriginalprojectsummary.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_itemoriginalprojectsummary

)

SELECT *
FROM itemoriginalprojectsummary 

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}

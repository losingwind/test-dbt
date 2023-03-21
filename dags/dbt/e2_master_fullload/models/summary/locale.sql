{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_locale AS (

	SELECT *
	FROM {{ source('e2_stage','locale') }}

), locale AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_locale.id']) }} AS unique_key,
		stg_locale.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_locale

)

SELECT *
FROM locale 

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}

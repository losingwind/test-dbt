{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_exambatchphaseinfo AS (

	SELECT *
	FROM {{ source('e2_stage','exambatchphaseinfo') }}

), exambatchphaseinfo AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_exambatchphaseinfo.id',
			'stg_exambatchphaseinfo.ix0']) }} AS unique_key,
		stg_exambatchphaseinfo.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_exambatchphaseinfo

)

SELECT *
FROM exambatchphaseinfo 

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}

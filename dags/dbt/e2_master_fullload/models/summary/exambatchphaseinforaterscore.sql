{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_exambatchphaseinforaterscore AS (

	SELECT *
	FROM {{ source('e2_stage','exambatchphaseinforaterscore') }}

), exambatchphaseinforaterscore AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_exambatchphaseinforaterscore.id',
			'stg_exambatchphaseinforaterscore.ix0','stg_exambatchphaseinforaterscore.ix1']) }} AS unique_key,
		stg_exambatchphaseinforaterscore.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_exambatchphaseinforaterscore

)

SELECT *
FROM exambatchphaseinforaterscore 

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}

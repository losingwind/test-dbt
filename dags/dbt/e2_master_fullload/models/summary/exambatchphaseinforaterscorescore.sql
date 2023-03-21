{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_exambatchphaseinforaterscorescore AS (

	SELECT *
	FROM {{ source('e2_stage','exambatchphaseinforaterscorescore') }}

), exambatchphaseinforaterscorescore AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_exambatchphaseinforaterscorescore.id',
			'stg_exambatchphaseinforaterscorescore.ix0','stg_exambatchphaseinforaterscorescore.ix1',
			'stg_exambatchphaseinforaterscorescore.ix2']) }} AS unique_key,
		stg_exambatchphaseinforaterscorescore.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_exambatchphaseinforaterscorescore

)

SELECT *
FROM exambatchphaseinforaterscorescore

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}

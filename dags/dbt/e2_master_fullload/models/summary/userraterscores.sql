{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_userraterscores AS(

	SELECT *
	FROM {{ source('e2_stage','userraterscores') }}

), userraterscores AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_userraterscores.id','stg_userraterscores.ix0']) }} AS unique_key,
		stg_userraterscores.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_userraterscores

)

SELECT *
FROM userraterscores

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}
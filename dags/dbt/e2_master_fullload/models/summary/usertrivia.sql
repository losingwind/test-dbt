{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_usertrivia AS(

	SELECT *
	FROM {{ source('e2_stage','usertrivia') }}

), usertrivia AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_usertrivia.id']) }} AS unique_key,
		stg_usertrivia.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_usertrivia

)

SELECT *
FROM usertrivia

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}
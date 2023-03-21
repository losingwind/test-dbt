{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_user AS(

	SELECT *
	FROM {{ source('e2_stage','user') }}

), user_tbl AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_user.id']) }} AS unique_key,
		stg_user.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_user

)

SELECT *
FROM user_tbl

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}
{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_usergroup AS(

	SELECT *
	FROM {{ source('e2_stage','usergroup') }}

), usergroup AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_usergroup.id']) }} AS unique_key,
		stg_usergroup.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_usergroup

)

SELECT *
FROM usergroup

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}
{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_projectitemdiscussion AS(

	SELECT *
	FROM {{ source('e2_stage','projectitemdiscussion') }}

), projectitemdiscussion AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_projectitemdiscussion.id','stg_projectitemdiscussion.ix0','stg_projectitemdiscussion.ix1']) }} AS unique_key,
		stg_projectitemdiscussion.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_projectitemdiscussion

)

SELECT *
FROM projectitemdiscussion

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}
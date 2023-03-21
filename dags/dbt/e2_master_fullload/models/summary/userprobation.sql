{{
	config(
		materialized='incremental',
		unique_key='unique_key'
		)
}}

WITH stg_userprobation AS(

	SELECT *
	FROM {{ source('e2_stage','userprobation') }}

), userprobation AS (

	SELECT
		{{ dbt_utils.surrogate_key(['stg_userprobation.id']) }} AS unique_key,
		stg_userprobation.*,
		DATEADD(DAY,-10,GETDATE()) AS edw_date_updated
	FROM stg_userprobation

)

SELECT *
FROM userprobation

{% if is_incremental() %}

  WHERE edw_date_updated  >= (SELECT MAX(edw_date_updated) FROM {{ this }})

{% endif %}
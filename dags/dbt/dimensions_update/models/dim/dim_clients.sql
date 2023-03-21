{{
	config(
		materialized='table',
		tags=["dim"],
		dist='client_id'
	)
}}


SELECT
    id AS client_id
    , name AS client
    , date_created
    , CONVERT_TIMEZONE('UTC', 'US/Pacific', GETDATE()) AS edw_date_updated
FROM {{ source('qrp','clients') }}

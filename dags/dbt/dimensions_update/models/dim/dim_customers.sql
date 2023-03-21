{{
	config(
		materialized='table',
		tags=["dim"],
		dist='customer_id'
	)
}}


SELECT
    id AS customer_id
    , name AS customer
    , date_created
    , is_disabled
    , client_id
    , CONVERT_TIMEZONE('UTC', 'US/Pacific', GETDATE()) AS edw_date_updated
FROM {{ source('qrp','customers') }}

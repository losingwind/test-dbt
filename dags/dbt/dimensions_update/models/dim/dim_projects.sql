{{
	config(
		materialized='table',
		tags=["dim"],
		sort='project_id',
		dist='date_created'
	)
}}

SELECT
    id AS project_id
    , date_created
    , DATE_TRUNC('month', date_created) AS month_date_created
    , date_updated
    , name
    , type
    , status
    , business_unit
    , customer_id
    , workday_id
    , is_express_project
    , is_auditor
    , is_disabled
    , is_external
    , is_cloned
FROM {{ source('qrp','exp_projects') }}

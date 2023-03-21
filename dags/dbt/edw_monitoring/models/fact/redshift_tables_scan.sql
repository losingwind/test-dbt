{{
	config(
		materialized='incremental',
		unique_key="unique_key",
	)
}}

WITH tbl_scan AS (

    SELECT
        userid
        , tbl
        , MAX(starttime) AS last_scan
    FROM stl_scan
    GROUP BY 1, 2

)

SELECT
    tbl_scan.tbl AS table_id
    , pg_user.usename::VARCHAR(120) AS username
    , svv_table_info."table"
    , svv_table_info.schema
    , svv_table_info.database
    , tbl_scan.last_scan
    , tbl_scan.tbl || '-' || pg_user.usename AS unique_key
FROM tbl_scan
INNER JOIN svv_table_info
    ON svv_table_info.table_id = tbl_scan.tbl
LEFT JOIN pg_user
    ON pg_user.usesysid = tbl_scan.userid
WHERE svv_table_info."table" IS NOT NULL

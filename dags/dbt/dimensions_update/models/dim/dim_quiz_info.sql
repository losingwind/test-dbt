{{
	config(
		materialized='table',
		tags=["dim"]
	)
}}

WITH q AS (

    SELECT
        id
        , title
    FROM {{ source('qrp','quizzes') }}

)

, qq AS (

    SELECT
        id
        , quiz_id
        , question_item_id
        , question_text
    FROM {{ source('qrp','quiz_questions') }}

)

, qqi AS (

    SELECT
        id
        , quiz_id
    FROM {{ source('qrp','quiz_question_items') }}

)

, dim_quiz_info AS (

    SELECT DISTINCT
        q.id AS quiz_id
        , q.title AS quiz_title
        , qq.id AS question_id
        , qq.question_text
    FROM q
    LEFT JOIN qqi
        ON q.id = qqi.quiz_id
    LEFT JOIN qq
        ON (q.id = qq.quiz_id OR qqi.id = qq.question_item_id)

)

SELECT *
FROM dim_quiz_info

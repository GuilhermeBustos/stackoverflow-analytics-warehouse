{{
    config(
        cluster_by=['tag_name', 'year']
    )
}}

WITH questions AS (
    SELECT * FROM {{ ref("fact_questions") }}
),

tags AS (
    SELECT * FROM {{ ref("int_bridge_question_tags") }}
),

dates AS (
    SELECT * FROM {{ ref("dim_date") }}
)

SELECT
    t.tag_name,
    d.year,
    COUNT(q.question_id) AS question_count
FROM questions q
INNER JOIN tags t
    ON q.question_id = t.question_id
INNER JOIN dates d
    ON DATE(q.creation_date) = d.date_day
GROUP BY
    t.tag_name,
    d.year

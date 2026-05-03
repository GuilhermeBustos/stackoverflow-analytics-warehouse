WITH answers AS (
    SELECT * FROM {{ ref("stg_stackoverflow__posts_answers") }}
),

questions AS (
    SELECT * FROM {{ ref("stg_stackoverflow__posts_questions") }}
)

SELECT
    a.id AS answer_id,
    a.parent_id AS question_id,
    COALESCE(q.accepted_answer_id = a.id, FALSE) AS is_accepted,
    TIMESTAMP_DIFF(a.creation_date, q.creation_date, HOUR) AS hours_to_answer
FROM answers a
LEFT JOIN questions q
    ON a.parent_id = q.id
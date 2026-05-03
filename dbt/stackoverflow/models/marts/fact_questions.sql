{{
    config(
        partition_by={
            'field': 'creation_date',
            'data_type': 'timestamp',
            'granularity': 'month'
        },
        cluster_by=['owner_user_id', 'question_id']
    )
}}

WITH source AS (
    SELECT * FROM {{ ref("stg_stackoverflow__posts_questions") }}
)

SELECT
    id AS question_id,
    owner_user_id,
    accepted_answer_id,
    title,
    body,
    score,
    view_count,
    answer_count,
    comment_count,
    favorite_count,
    creation_date,
    last_edit_date,
    last_activity_date,
    last_editor_user_id,
    TIMESTAMP_DIFF(last_activity_date, creation_date, DAY) AS days_active
FROM source

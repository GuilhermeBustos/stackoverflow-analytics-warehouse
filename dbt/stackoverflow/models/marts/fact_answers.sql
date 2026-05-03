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
    SELECT * FROM {{ ref("stg_stackoverflow__posts_answers") }}
)

SELECT
    id AS answer_id,
    parent_id AS question_id,
    owner_user_id,
    body AS answer_content,
    score,
    comment_count,
    creation_date,
    last_edit_date,
    last_activity_date,
    last_editor_user_id,
    community_owned_date
FROM source

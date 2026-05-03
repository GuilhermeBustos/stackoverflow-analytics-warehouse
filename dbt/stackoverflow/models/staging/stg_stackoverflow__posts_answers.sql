{{
    config(
        partition_by={
            'field': 'creation_date',
            'data_type': 'timestamp',
            'granularity': 'month'
        },
        cluster_by=['parent_id', 'owner_user_id', 'id']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('stackoverflow_raw', 'posts_answers') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY last_activity_date DESC) AS rn
    FROM source
),

renamed AS (
    SELECT
        id,
        title,
        body,
        accepted_answer_id,
        answer_count,
        comment_count,
        community_owned_date,
        creation_date,
        favorite_count,
        last_activity_date,
        last_edit_date,
        last_editor_display_name,
        last_editor_user_id,
        owner_display_name,
        owner_user_id,
        parent_id,
        post_type_id,
        score,
        tags,
        view_count
    FROM deduplicated
    WHERE rn = 1
)

SELECT *
FROM renamed

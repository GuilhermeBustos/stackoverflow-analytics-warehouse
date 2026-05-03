WITH unnested AS (
    SELECT
        id AS question_id,
        TRIM(tag_name_raw) AS tag_name_raw
    FROM {{ ref("stg_stackoverflow__posts_questions") }},
    UNNEST(SPLIT(tags, '|')) AS tag_name_raw
    WHERE tags IS NOT NULL
)

SELECT
    u.question_id,
    t.id AS tag_id,
    t.tag_name
FROM unnested u
INNER JOIN {{ ref("stg_stackoverflow__tags") }} t
    ON u.tag_name_raw = t.tag_name

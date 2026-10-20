
-- Checkpoint C: Assemble the phrase in the correct order

SELECT
    string_agg(word, ' ' ORDER BY order_no) AS phrase
FROM {{ ref('stg_fragments') }}

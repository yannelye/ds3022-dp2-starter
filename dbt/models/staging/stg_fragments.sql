
-- Checkpoint C: Clean and type the raw fragments

WITH cleaned AS (
    SELECT
        TRY_CAST(order_no AS INTEGER) AS order_no,
        TRIM(word) AS word
    FROM {{ source('raw', 'fragments') }}
),

deduplicated AS (
    SELECT DISTINCT
        order_no,
        word
    FROM cleaned
)

SELECT
    order_no,
    word
FROM deduplicated
WHERE order_no IS NOT NULL
  AND word IS NOT NULL
  AND word <> ''

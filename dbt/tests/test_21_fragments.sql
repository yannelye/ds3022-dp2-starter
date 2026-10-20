
-- Checkpoint C: Verify exactly 21 fragments with no gaps.
-- A dbt singular test passes when this query returns zero rows.

WITH expected AS (
    SELECT generate_series AS order_no
    FROM generate_series(1, 21)
),

actual AS (
    SELECT order_no
    FROM {{ ref('stg_fragments') }}
),

missing AS (
    SELECT order_no FROM expected
    EXCEPT
    SELECT order_no FROM actual
),

unexpected AS (
    SELECT order_no FROM actual
    EXCEPT
    SELECT order_no FROM expected
),

wrong_count AS (
    SELECT NULL AS order_no
    WHERE (SELECT COUNT(*) FROM actual) <> 21
)

SELECT order_no FROM missing
UNION ALL
SELECT order_no FROM unexpected
UNION ALL
SELECT order_no FROM wrong_count

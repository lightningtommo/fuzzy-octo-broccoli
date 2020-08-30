{{
  config(
    materialized='view'
  )
}}

WITH transactions AS (
  SELECT * FROM {{ ref('transactions') }} 
)

SELECT
  o.order_transaction_date,
  l.line_item_date,
  l.line_item_product_id,
  SUM(l.line_item_price_delta) AS line_item_price_delta
FROM 
  transactions o
CROSS JOIN
  o.line_items l
GROUP BY
  1,2,3
--HAVING
--  line_item_price_delta <> 0
ORDER BY
  1,2,3

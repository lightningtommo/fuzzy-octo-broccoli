{{
  config(
    materialized='view'
  )
}}

WITH transactions AS (
  SELECT * FROM {{ ref('transactions') }}
)


-- Show all rows, flattened
SELECT 
  order_transaction_date,
  order_id,
  order_price,
  order_price_delta,
  line_item_id,
  line_item_product_id,
  line_item_price,
  line_item_price_delta
FROM
  transactions
CROSS JOIN
  UNNEST(line_items) l
ORDER BY
  order_id,
  order_transaction_date,
  order_version,
  line_item_type,
  line_item_version DESC

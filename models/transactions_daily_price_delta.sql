{{
  config(
    materialized='view'
  )
}}

WITH transactions AS (
  SELECT * FROM {{ ref('transactions') }} 
)

-- Show money taken/given back on all orders on a given date
SELECT
  order_transaction_date,
  order_id,
  SUM(order_price_delta) AS order_price_delta
FROM
  transactions
GROUP BY
  1,2
ORDER BY
  1,2

{{
  config(
    materialized='view'
  )
}}

WITH transactions AS (
  SELECT * FROM {{ ref('transactions') }} 
)

SELECT
  * EXCEPT ( row_num )
FROM (  
  SELECT
    ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY order_version DESC) AS row_num,
    order_id,
    order_transaction_date,
    order_price
  FROM
    transactions
  WHERE
    order_transaction_date <= '9999-12-31'
    -- order_transaction_date <= '2020-07-13'
    -- order_transaction_date <= '2020-07-01'
)
WHERE
  row_num = 1

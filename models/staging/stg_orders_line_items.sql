WITH orders AS (
  SELECT
    *
  FROM
    {{ ref('stg_orders') }}
),

line_items AS (
  SELECT
    *
  FROM
    {{ ref('stg_line_items') }}
),

joined AS (
  SELECT
    o.*,
    l.* EXCEPT(order_id, order_version)
  FROM
    orders o
  JOIN
    line_items l
  ON
    o.order_id = l.order_id
  AND
    o.order_version = l.order_version
)

SELECT
  order_id,
  order_version,
  order_transaction_date,
  order_transaction_type,
  ARRAY_AGG(
    STRUCT(
      line_item_id,
      line_item_product_id,
      line_item_type,
      line_item_price,
      line_item_version,
      line_item_date
    )
  ) AS line_items 
FROM
  joined 
GROUP BY
  order_id,
  order_version,
  order_transaction_date,
  order_transaction_type

WITH orders AS (
  SELECT * FROM {{ ref('stg_orders') }}
),

line_items AS (
  SELECT * FROM {{ ref('stg_line_items') }}
),

joined AS (
  SELECT
    o.order_id,
    o.order_version,
    o.order_transaction_date,
    l.line_item_id,
    l.line_item_product_id,
    l.line_item_type,
    l.line_item_price,
    l.line_item_version,
    l.line_item_date
  FROM
    orders o
  INNER JOIN
    line_items l
  ON
    o.order_id = l.order_id
  AND
    o.order_version = l.order_version
  ORDER BY
    o.order_id,
    o.order_version,
    l.line_item_type
),

lag_line_level AS (
  SELECT
    *,
    line_item_price - IFNULL( LAG( line_item_price ) OVER ( PARTITION BY line_item_id ORDER BY order_version, line_item_version ), 0 ) AS line_item_price_delta,
    GENERATE_UUID() AS line_item_pkey
  FROM
    joined
),

rolled_up AS (
  SELECT
    order_id,
    order_version,
    order_transaction_date,
    SUM( line_item_price ) AS order_price,
    ARRAY_AGG(
      STRUCT(
	line_item_pkey,
        line_item_id,
        line_item_product_id,
        line_item_type,
        line_item_price,
        line_item_price_delta,
        line_item_version,
        line_item_date
      )
    ) AS line_items
  FROM
    lag_line_level
  GROUP BY
    1,2,3
),

lag_order_level AS (
  SELECT
    GENERATE_UUID() AS order_pkey,
    * EXCEPT ( line_items ),
    order_price - IFNULL( LAG( order_price ) OVER ( PARTITION BY order_id ORDER BY order_version ), 0 ) AS order_price_delta,
    line_items
  FROM
    rolled_up
)

SELECT
  *
FROM
  lag_order_level

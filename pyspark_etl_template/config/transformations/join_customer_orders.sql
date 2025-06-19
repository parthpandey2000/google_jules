-- Transformation SQL: join_customer_orders.sql
-- This SQL script joins customer data with their orders.
-- Input DataFrames (views) expected:
--   - customers_csv_df: Loaded from config/sources/customers_csv.yaml
--   - orders_parquet_df: Loaded from config/sources/orders_parquet.yaml
-- Output DataFrame (view) will be named based on 'output' field in pipeline config, e.g., 'customer_orders_joined_df'

SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.signup_date,
    o.order_id,
    o.order_date,
    o.order_amount,
    o.product_id
FROM
    customers_csv_df c -- This view name should match the DataFrame name after loading the source
JOIN
    orders_parquet_df o -- This view name should match the DataFrame name after loading the source
ON
    c.customer_id = o.customer_id
WHERE
    o.order_amount > 0 -- Example filter: only include orders with a positive amount

-- Optional: Add more complex logic, aggregations, or further joins if needed.
-- For example, if there was a products_df:
-- LEFT JOIN
--     products_df p
-- ON
--     o.product_id = p.product_id

-- The result of this query will be registered as a temporary view
-- with the name specified in the 'output' field of the transformation
-- definition in the pipeline YAML file.
-- Example: "output": "customer_orders_joined_df"
-- Spark job will then do:
-- df = spark.sql("SELECT ... FROM customers_csv_df ...")
-- df.createOrReplaceTempView("customer_orders_joined_df")

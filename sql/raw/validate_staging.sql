SELECT '{schema}.olist_customers_dataset' AS table_name, COUNT(*) AS total_rows FROM {schema}.olist_customers_dataset
UNION ALL
SELECT '{schema}.olist_orders_dataset', COUNT(*) FROM {schema}.olist_orders_dataset
UNION ALL
SELECT '{schema}.olist_order_items_dataset', COUNT(*) FROM {schema}.olist_order_items_dataset
UNION ALL
SELECT '{schema}.olist_order_payments_dataset', COUNT(*) FROM {schema}.olist_order_payments_dataset
UNION ALL
SELECT '{schema}.olist_order_reviews_dataset', COUNT(*) FROM {schema}.olist_order_reviews_dataset
UNION ALL
SELECT '{schema}.olist_products_dataset', COUNT(*) FROM {schema}.olist_products_dataset
UNION ALL
SELECT '{schema}.olist_sellers_dataset', COUNT(*) FROM {schema}.olist_sellers_dataset;

SELECT 'customers - null PK' AS check_type, COUNT(*) AS issue_count
FROM {schema}.olist_customers_dataset WHERE customer_id IS NULL
UNION ALL
SELECT 'orders - null PK', COUNT(*)
FROM {schema}.olist_orders_dataset WHERE order_id IS NULL
UNION ALL
SELECT 'products - null PK', COUNT(*)
FROM {schema}.olist_products_dataset WHERE product_id IS NULL
UNION ALL
SELECT 'sellers - null PK', COUNT(*)
FROM {schema}.olist_sellers_dataset WHERE seller_id IS NULL
UNION ALL
SELECT 'order_items - null PK', COUNT(*)
FROM {schema}.olist_order_items_dataset WHERE order_id IS NULL OR order_item_id IS NULL
UNION ALL
SELECT 'order_payments - null PK', COUNT(*)
FROM {schema}.olist_order_payments_dataset WHERE order_id IS NULL OR payment_sequential IS NULL
UNION ALL
SELECT 'order_reviews - null PK', COUNT(*)
FROM {schema}.olist_order_reviews_dataset WHERE review_id IS NULL OR order_id IS NULL
UNION ALL
SELECT 'customers - duplicate PK', COUNT(*)
FROM (
    SELECT customer_id FROM {schema}.olist_customers_dataset
    GROUP BY customer_id HAVING COUNT(*) > 1
) AS dup_customers
UNION ALL
SELECT 'orders - duplicate PK', COUNT(*)
FROM (
    SELECT order_id FROM {schema}.olist_orders_dataset
    GROUP BY order_id HAVING COUNT(*) > 1
) AS dup_orders
UNION ALL
SELECT 'products - duplicate PK', COUNT(*)
FROM (
    SELECT product_id FROM {schema}.olist_products_dataset
    GROUP BY product_id HAVING COUNT(*) > 1
) AS dup_products
UNION ALL
SELECT 'sellers - duplicate PK', COUNT(*)
FROM (
    SELECT seller_id FROM {schema}.olist_sellers_dataset
    GROUP BY seller_id HAVING COUNT(*) > 1
) AS dup_sellers
UNION ALL
SELECT 'order_items - duplicate composite PK', COUNT(*)
FROM (
    SELECT order_id, order_item_id FROM {schema}.olist_order_items_dataset
    GROUP BY order_id, order_item_id HAVING COUNT(*) > 1
) AS dup_items
UNION ALL
SELECT 'order_payments - duplicate composite PK', COUNT(*)
FROM (
    SELECT order_id, payment_sequential FROM {schema}.olist_order_payments_dataset
    GROUP BY order_id, payment_sequential HAVING COUNT(*) > 1
) AS dup_payments
UNION ALL
SELECT 'order_reviews - duplicate composite PK', COUNT(*)
FROM (
    SELECT review_id, order_id FROM {schema}.olist_order_reviews_dataset
    GROUP BY review_id, order_id HAVING COUNT(*) > 1
) AS dup_reviews;

SELECT
    'olist_orders_dataset' AS table_name,
    'order_purchase_timestamp' AS watermark_column,
    MIN(order_purchase_timestamp) AS min_date,
    MAX(order_purchase_timestamp) AS max_date
FROM {schema}.olist_orders_dataset
UNION ALL
SELECT
    'olist_order_reviews_dataset',
    'review_creation_date',
    MIN(review_creation_date),
    MAX(review_creation_date)
FROM {schema}.olist_order_reviews_dataset;

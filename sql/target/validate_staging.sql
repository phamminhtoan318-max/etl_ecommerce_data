-- Active: 1792219074670@@127.0.0.1@5432@olist_dwh
SELECT 'stg.olist_customers_dataset' AS table_name, COUNT(*) AS total_rows FROM stg.olist_customers_dataset
UNION ALL
SELECT 'stg.olist_orders_dataset', COUNT(*) FROM stg.olist_orders_dataset
UNION ALL
SELECT 'stg.olist_order_items_dataset', COUNT(*) FROM stg.olist_order_items_dataset
UNION ALL
SELECT 'stg.olist_order_payments_dataset', COUNT(*) FROM stg.olist_order_payments_dataset
UNION ALL
SELECT 'stg.olist_order_reviews_dataset', COUNT(*) FROM stg.olist_order_reviews_dataset
UNION ALL
SELECT 'stg.olist_products_dataset', COUNT(*) FROM stg.olist_products_dataset
UNION ALL
SELECT 'stg.olist_sellers_dataset', COUNT(*) FROM stg.olist_sellers_dataset;

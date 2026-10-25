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

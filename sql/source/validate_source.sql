-- Active: 1792219119102@@127.0.0.1@1433@E_commerce
SELECT 'olist_customers_dataset' AS [table_name], COUNT(*) AS [total_rows] FROM [dbo].[olist_customers_dataset] WITH (NOLOCK)
UNION ALL
SELECT 'olist_orders_dataset', COUNT(*) FROM [dbo].[olist_orders_dataset] WITH (NOLOCK)
UNION ALL
SELECT 'olist_order_items_dataset', COUNT(*) FROM [dbo].[olist_order_items_dataset] WITH (NOLOCK)
UNION ALL
SELECT 'olist_order_payments_dataset', COUNT(*) FROM [dbo].[olist_order_payments_dataset] WITH (NOLOCK)
UNION ALL
SELECT 'olist_order_reviews_dataset', COUNT(*) FROM [dbo].[olist_order_reviews_dataset] WITH (NOLOCK)
UNION ALL
SELECT 'olist_products_dataset', COUNT(*) FROM [dbo].[olist_products_dataset] WITH (NOLOCK)
UNION ALL
SELECT 'olist_sellers_dataset', COUNT(*) FROM [dbo].[olist_sellers_dataset] WITH (NOLOCK);
   
SELECT 'customers - null PK' AS [check_type], COUNT(*) AS [issue_count] 
FROM [dbo].[olist_customers_dataset] WITH (NOLOCK) WHERE [customer_id] IS NULL
UNION ALL
SELECT 'orders - null PK', COUNT(*) 
FROM [dbo].[olist_orders_dataset] WITH (NOLOCK) WHERE [order_id] IS NULL
UNION ALL
SELECT 'products - null PK', COUNT(*) 
FROM [dbo].[olist_products_dataset] WITH (NOLOCK) WHERE [product_id] IS NULL
UNION ALL
SELECT 'sellers - null PK', COUNT(*) 
FROM [dbo].[olist_sellers_dataset] WITH (NOLOCK) WHERE [seller_id] IS NULL
UNION ALL
SELECT 'order_items - null PK', COUNT(*) 
FROM [dbo].[olist_order_items_dataset] WITH (NOLOCK) WHERE [order_id] IS NULL OR [order_item_id] IS NULL
UNION ALL
SELECT 'order_payments - null PK', COUNT(*) 
FROM [dbo].[olist_order_payments_dataset] WITH (NOLOCK) WHERE [order_id] IS NULL OR [payment_sequential] IS NULL
UNION ALL
SELECT 'order_reviews - null PK', COUNT(*) 
FROM [dbo].[olist_order_reviews_dataset] WITH (NOLOCK) WHERE [review_id] IS NULL OR [order_id] IS NULL
UNION ALL
SELECT 'order_items - duplicate composite PK', COUNT(*)
FROM (
    SELECT [order_id], [order_item_id]
    FROM [dbo].[olist_order_items_dataset] WITH (NOLOCK)
    GROUP BY [order_id], [order_item_id]
    HAVING COUNT(*) > 1
) AS dup_items
UNION ALL
SELECT 'order_payments - duplicate composite PK', COUNT(*)
FROM (
    SELECT [order_id], [payment_sequential]
    FROM [dbo].[olist_order_payments_dataset] WITH (NOLOCK)
    GROUP BY [order_id], [payment_sequential]
    HAVING COUNT(*) > 1
) AS dup_payments
UNION ALL
SELECT 'order_reviews - duplicate composite PK', COUNT(*)
FROM (
    SELECT [review_id], [order_id]
    FROM [dbo].[olist_order_reviews_dataset] WITH (NOLOCK)
    GROUP BY [review_id], [order_id]
    HAVING COUNT(*) > 1
) AS dup_reviews;

SELECT 
    'olist_orders_dataset' AS [table_name],
    'order_purchase_timestamp' AS [watermark_column],
    MIN([order_purchase_timestamp]) AS [min_date],
    MAX([order_purchase_timestamp]) AS [max_date]
FROM [dbo].[olist_orders_dataset] WITH (NOLOCK)
UNION ALL
SELECT 
    'olist_order_reviews_dataset',
    'review_creation_date',
    MIN([review_creation_date]),
    MAX([review_creation_date])
FROM [dbo].[olist_order_reviews_dataset] WITH (NOLOCK);
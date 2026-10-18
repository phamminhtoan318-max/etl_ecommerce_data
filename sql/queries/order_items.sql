SELECT 
    [order_id],
    [order_item_id],
    [product_id],
    [seller_id],
    [shipping_limit_date],
    [price],
    [freight_value]
FROM [dbo].[olist_order_items_dataset] WITH (NOLOCK);

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_olist_orders_dataset_olist_customers_dataset')
BEGIN
    ALTER TABLE [dbo].[olist_orders_dataset]
    ADD CONSTRAINT [fk_olist_orders_dataset_olist_customers_dataset]
    FOREIGN KEY ([customer_id]) REFERENCES [dbo].[olist_customers_dataset] ([customer_id]);
END

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_olist_order_items_dataset_olist_orders_dataset')
BEGIN
    ALTER TABLE [dbo].[olist_order_items_dataset]
    ADD CONSTRAINT [fk_olist_order_items_dataset_olist_orders_dataset]
    FOREIGN KEY ([order_id]) REFERENCES [dbo].[olist_orders_dataset] ([order_id]);
END

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_olist_order_items_dataset_olist_products_dataset')
BEGIN
    ALTER TABLE [dbo].[olist_order_items_dataset]
    ADD CONSTRAINT [fk_olist_order_items_dataset_olist_products_dataset]
    FOREIGN KEY ([product_id]) REFERENCES [dbo].[olist_products_dataset] ([product_id]);
END

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_olist_order_items_dataset_olist_sellers_dataset')
BEGIN
    ALTER TABLE [dbo].[olist_order_items_dataset]
    ADD CONSTRAINT [fk_olist_order_items_dataset_olist_sellers_dataset]
    FOREIGN KEY ([seller_id]) REFERENCES [dbo].[olist_sellers_dataset] ([seller_id]);
END

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_olist_order_payments_dataset_olist_orders_dataset')
BEGIN
    ALTER TABLE [dbo].[olist_order_payments_dataset]
    ADD CONSTRAINT [fk_olist_order_payments_dataset_olist_orders_dataset]
    FOREIGN KEY ([order_id]) REFERENCES [dbo].[olist_orders_dataset] ([order_id]);
END

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_olist_order_reviews_dataset_olist_orders_dataset')
BEGIN
    ALTER TABLE [dbo].[olist_order_reviews_dataset]
    ADD CONSTRAINT [fk_olist_order_reviews_dataset_olist_orders_dataset]
    FOREIGN KEY ([order_id]) REFERENCES [dbo].[olist_orders_dataset] ([order_id]);
END

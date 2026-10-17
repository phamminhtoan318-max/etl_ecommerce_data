IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'pk_olist_customers_dataset')
BEGIN
    ALTER TABLE [dbo].[olist_customers_dataset]
    ADD CONSTRAINT [pk_olist_customers_dataset] PRIMARY KEY CLUSTERED ([customer_id] ASC);
END

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'pk_olist_products_dataset')
BEGIN
    ALTER TABLE [dbo].[olist_products_dataset]
    ADD CONSTRAINT [pk_olist_products_dataset] PRIMARY KEY CLUSTERED ([product_id] ASC);
END

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'pk_olist_sellers_dataset')
BEGIN
    ALTER TABLE [dbo].[olist_sellers_dataset]
    ADD CONSTRAINT [pk_olist_sellers_dataset] PRIMARY KEY CLUSTERED ([seller_id] ASC);
END

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'pk_olist_orders_dataset')
BEGIN
    ALTER TABLE [dbo].[olist_orders_dataset]
    ADD CONSTRAINT [pk_olist_orders_dataset] PRIMARY KEY CLUSTERED ([order_id] ASC);
END

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'pk_olist_order_items_dataset')
BEGIN
    ALTER TABLE [dbo].[olist_order_items_dataset]
    ADD CONSTRAINT [pk_olist_order_items_dataset] PRIMARY KEY CLUSTERED ([order_id] ASC, [order_item_id] ASC);
END

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'pk_olist_order_payments_dataset')
BEGIN
    ALTER TABLE [dbo].[olist_order_payments_dataset]
    ADD CONSTRAINT [pk_olist_order_payments_dataset] PRIMARY KEY CLUSTERED ([order_id] ASC, [payment_sequential] ASC);
END

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'pk_olist_order_reviews_dataset')
BEGIN
    ALTER TABLE [dbo].[olist_order_reviews_dataset]
    ADD CONSTRAINT [pk_olist_order_reviews_dataset] PRIMARY KEY CLUSTERED ([review_id] ASC, [order_id] ASC);
END

IF OBJECT_ID(N'[dbo].[olist_customers_dataset]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[olist_customers_dataset] (
        [customer_id]              VARCHAR(32)   NOT NULL,
        [customer_unique_id]       VARCHAR(32)   NOT NULL,
        [customer_zip_code_prefix] INT           NULL,
        [customer_city]            NVARCHAR(100) NULL,
        [customer_state]           VARCHAR(10)   NULL
    );
END

IF OBJECT_ID(N'[dbo].[olist_products_dataset]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[olist_products_dataset] (
        [product_id]                 VARCHAR(32)   NOT NULL,
        [product_category_name]      NVARCHAR(100) NULL,
        [product_name_lenght]        INT           NULL,
        [product_description_lenght] INT           NULL,
        [product_photos_qty]         INT           NULL,
        [product_weight_g]           INT           NULL,
        [product_length_cm]          INT           NULL,
        [product_height_cm]          INT           NULL,
        [product_width_cm]           INT           NULL
    );
END

IF OBJECT_ID(N'[dbo].[olist_sellers_dataset]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[olist_sellers_dataset] (
        [seller_id]              VARCHAR(32)   NOT NULL,
        [seller_zip_code_prefix] INT           NULL,
        [seller_city]            NVARCHAR(100) NULL,
        [seller_state]           VARCHAR(10)   NULL
    );
END

IF OBJECT_ID(N'[dbo].[olist_orders_dataset]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[olist_orders_dataset] (
        [order_id]                       VARCHAR(32)   NOT NULL,
        [customer_id]                    VARCHAR(32)   NOT NULL,
        [order_status]                   VARCHAR(20)   NOT NULL,
        [order_purchase_timestamp]       DATETIME2(0)  NOT NULL,
        [order_approved_at]              DATETIME2(0)  NULL,
        [order_delivered_carrier_date]   DATETIME2(0)  NULL,
        [order_delivered_customer_date]  DATETIME2(0)  NULL,
        [order_estimated_delivery_date]  DATETIME2(0)  NULL
    );
END

IF OBJECT_ID(N'[dbo].[olist_order_items_dataset]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[olist_order_items_dataset] (
        [order_id]            VARCHAR(32)    NOT NULL,
        [order_item_id]       INT            NOT NULL,
        [product_id]          VARCHAR(32)    NOT NULL,
        [seller_id]           VARCHAR(32)    NOT NULL,
        [shipping_limit_date] DATETIME2(0)   NULL,
        [price]               DECIMAL(10, 2) NOT NULL,
        [freight_value]       DECIMAL(10, 2) NOT NULL
    );
END

IF OBJECT_ID(N'[dbo].[olist_order_payments_dataset]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[olist_order_payments_dataset] (
        [order_id]             VARCHAR(32)    NOT NULL,
        [payment_sequential]   INT            NOT NULL,
        [payment_type]         VARCHAR(30)    NOT NULL,
        [payment_installments] INT            NOT NULL,
        [payment_value]        DECIMAL(10, 2) NOT NULL
    );
END

IF OBJECT_ID(N'[dbo].[olist_order_reviews_dataset]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[olist_order_reviews_dataset] (
        [review_id]               VARCHAR(32)    NOT NULL,
        [order_id]                VARCHAR(32)    NOT NULL,
        [review_score]            INT            NOT NULL,
        [review_comment_title]    NVARCHAR(255)  NULL,
        [review_comment_message]  NVARCHAR(MAX)  NULL,
        [review_creation_date]    DATETIME2(0)   NOT NULL,
        [review_answer_timestamp] DATETIME2(0)   NULL
    );
END

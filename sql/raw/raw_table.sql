CREATE SCHEMA IF NOT EXISTS {schema};

CREATE TABLE IF NOT EXISTS {schema}.olist_customers_dataset (
    customer_id              VARCHAR(32) NOT NULL,
    customer_unique_id       VARCHAR(32),
    customer_zip_code_prefix INT,
    customer_city            VARCHAR(100),
    customer_state           VARCHAR(10),
    _extracted_at            TIMESTAMP,
    _batch_id                VARCHAR(30),
    PRIMARY KEY (customer_id)
);

CREATE TABLE IF NOT EXISTS {schema}.olist_products_dataset (
    product_id                 VARCHAR(32) NOT NULL,
    product_category_name      VARCHAR(100),
    product_name_lenght        INT,
    product_description_lenght INT,
    product_photos_qty         INT,
    product_weight_g           INT,
    product_length_cm          INT,
    product_height_cm          INT,
    product_width_cm           INT,
    _extracted_at              TIMESTAMP,
    _batch_id                  VARCHAR(30),
    PRIMARY KEY (product_id)
);

CREATE TABLE IF NOT EXISTS {schema}.olist_sellers_dataset (
    seller_id              VARCHAR(32) NOT NULL,
    seller_zip_code_prefix INT,
    seller_city            VARCHAR(100),
    seller_state           VARCHAR(10),
    _extracted_at          TIMESTAMP,
    _batch_id              VARCHAR(30),
    PRIMARY KEY (seller_id)
);

CREATE TABLE IF NOT EXISTS {schema}.olist_orders_dataset (
    order_id                      VARCHAR(32) NOT NULL,
    customer_id                   VARCHAR(32),
    order_status                  VARCHAR(20),
    order_purchase_timestamp      TIMESTAMP,
    order_approved_at             TIMESTAMP,
    order_delivered_carrier_date  TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    _extracted_at                 TIMESTAMP,
    _batch_id                     VARCHAR(30),
    PRIMARY KEY (order_id)
);

CREATE INDEX IF NOT EXISTS idx_orders_purchase_timestamp ON {schema}.olist_orders_dataset (order_purchase_timestamp);

CREATE TABLE IF NOT EXISTS {schema}.olist_order_items_dataset (
    order_id            VARCHAR(32) NOT NULL,
    order_item_id       INT NOT NULL,
    product_id          VARCHAR(32),
    seller_id           VARCHAR(32),
    shipping_limit_date TIMESTAMP,
    price               NUMERIC(10, 2),
    freight_value       NUMERIC(10, 2),
    _extracted_at       TIMESTAMP,
    _batch_id           VARCHAR(30),
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE IF NOT EXISTS {schema}.olist_order_payments_dataset (
    order_id             VARCHAR(32) NOT NULL,
    payment_sequential   INT NOT NULL,
    payment_type         VARCHAR(30),
    payment_installments INT,
    payment_value        NUMERIC(10, 2),
    _extracted_at        TIMESTAMP,
    _batch_id            VARCHAR(30),
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE IF NOT EXISTS {schema}.olist_order_reviews_dataset (
    review_id               VARCHAR(32) NOT NULL,
    order_id                VARCHAR(32) NOT NULL,
    review_score            INT,
    review_comment_title    VARCHAR(255),
    review_comment_message  TEXT,
    review_creation_date    TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    _extracted_at           TIMESTAMP,
    _batch_id               VARCHAR(30),
    PRIMARY KEY (review_id, order_id)
);

CREATE INDEX IF NOT EXISTS idx_reviews_creation_date ON {schema}.olist_order_reviews_dataset (review_creation_date);

CREATE TABLE IF NOT EXISTS {schema}._etl_watermarks (
    logical_name     VARCHAR(50) PRIMARY KEY,
    target_table     VARCHAR(100) NOT NULL,
    watermark_column VARCHAR(100) NOT NULL,
    last_watermark   TIMESTAMP NOT NULL,
    last_batch_id    VARCHAR(30),
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
